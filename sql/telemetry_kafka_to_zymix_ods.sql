
-- 原则：只提取分区和主键需要的最小字段，payload 原文逐字节落库。
--   用 format='raw' 而不是 'json'：json 格式会把消息解析成列再序列化回去，原文就不再是原文了。
--
-- 目标表 DDL：ddl/zymix_ods/app_events.sql、ddl/zymix_ods/app_events_dlq.sql（先建表再启动作业）。
--
-- 变量（托管 Flink 的 Runtime properties，或本地 --props 文件）：
--   KAFKA_BROKERS             MSK 地址（MSK Serverless 只支持 IAM，配合 KAFKA_IAM=true）
--   TELEMETRY_EVENT_TOPIC     topic，生产上是 telemetry.events
--   ZYMIX_ODS_GROUP_ID        消费组。**不能和生产作业的 TELEMETRY_ODS_GROUP_ID 相同**，否则会抢分区
--   ZYMIX_ODS_STARTUP_MODE    首次启动的位置，默认 group-offsets（消费组没有位点时退回 earliest）
--   DORIS_FENODES / DORIS_BENODES(可选) / DORIS_USER / DORIS_PASSWORD（或 DORIS_SECRET_ID）
--   ZYMIX_ODS_LABEL_PREFIX    Doris stream load label 前缀，**每次不从快照恢复的重新提交都要换**
--                             （DLQ sink 用 <前缀>_dlq，跟着一起换）
--   TELEMETRY_DLQ_TOPIC       DLQ topic，默认 telemetry.events.dlq；DLQ 消费组固定为 <ZYMIX_ODS_GROUP_ID>-dlq
--
-- 一个作业两条链路（运行器把两条 INSERT 合进同一个 StatementSet）：
--   1. telemetry.events     → zymix_ods.app_events
--   2. telemetry.events.dlq → zymix_ods.app_events_dlq（telemetry-svc 校验不通过、带原因转发的事件）

SET 'pipeline.name' = 'telemetry_kafka_to_zymix_ods';
-- 数仓约定日期按 UTC。TO_TIMESTAMP_LTZ → DATE / TIMESTAMP 的换算依赖会话时区，
-- 不显式设置的话，本地（东八区）跑出来的 event_date 会和线上差一天。
SET 'table.local-time-zone' = 'UTC';
-- 以下在托管 Flink 上由服务接管（运行器会跳过），只对本地运行生效
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'restart-strategy' = 'exponential-delay';

CREATE TABLE kafka_telemetry_events (
    payload         STRING,
    kafka_partition INT              METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT           METADATA FROM 'offset'    VIRTUAL,
    kafka_ts        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    -- 处理时间：事件时间合法范围的参照基准（见下方过滤）。
    -- 不定义 WATERMARK：watermark 只能建在事件时间列上，且只对窗口 / interval join 等有状态算子起作用；
    -- 本作业是逐条 map + filter，没有窗口，watermark 不会丢弃任何行，范围过滤必须显式写在 WHERE 里。
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = '${TELEMETRY_EVENT_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.group.id' = '${ZYMIX_ODS_GROUP_ID}',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = '${ZYMIX_ODS_STARTUP_MODE:group-offsets}',
    -- group-offsets 模式下消费组还没有提交过位点时从哪里开始
    'properties.auto.offset.reset' = 'earliest',
    -- MSK Serverless 扩分区后自动发现新分区
    'scan.topic-partition-discovery.interval' = '5min',
    'format' = 'raw'
    -- MSK IAM 认证选项由运行器在 KAFKA_IAM=true 时追加
);

CREATE TABLE app_events_sink (
    event_date      DATE,
    event_id        STRING,
    event_name      STRING,
    event_time      TIMESTAMP(3),
    received_at     TIMESTAMP(3),
    schema_version  INT,
    payload         STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    kafka_ts        TIMESTAMP(3),
    ingest_seq      BIGINT
) WITH (
    'connector' = 'doris',
    'fenodes' = '${DORIS_FENODES}',
    'benodes' = '${DORIS_BENODES:}',
    'table.identifier' = 'zymix_ods.app_events',
    'username' = '${DORIS_USER}',
    'password' = '${DORIS_PASSWORD}',
    -- label = 前缀 + checkpoint 序号，Doris 用它做写入幂等。重新提交时 checkpoint 从 1 计数，
    -- 前缀不换就会撞上上一轮的 label，作业陷入 RESTARTING 循环。
    'sink.label-prefix' = '${ZYMIX_ODS_LABEL_PREFIX}',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3',
    -- Doris 默认 max_filter_ratio=0：一行被过滤（如落不进分区的迟到事件）整批失败，
    -- checkpoint 失败 → 重启 → 又读到同一行，死循环，整条链路卡住。
    -- 0.05 是「允许一批里有个别坏行」，正常应恒为 0；被过滤的行会静默丢弃，需要靠对账发现
    -- （消费位点数 vs 落库行数，kafka_partition / kafka_offset 两列就是为此准备的）。
    'sink.properties.max_filter_ratio' = '0.05',
    -- VeloDB（存算分离）不支持对 MoW 表做 2PC stream load（stream load 2pc is unsupported for mow table），
    -- 必须关掉。正确性由 UNIQUE KEY + sequence 列保证：重放的重复行按 event_id 收敛。
    'sink.enable-2pc' = 'false'
);

-- 先把要用的字段解析一次，后面的过滤和写入都基于它。
-- 数字字段一律 TRY_CAST：CAST 遇到非数字（如 "event_time_ms": "abc"）会抛 NumberFormatException，
-- 作业失败重启后又读到同一条消息，在无限重启里卡死；TRY_CAST 转不了就返回 NULL，交给下面的过滤处理。
CREATE TEMPORARY VIEW telemetry_events_parsed AS
SELECT
    JSON_VALUE(payload, '$.event_id')                                                  AS event_id,
    JSON_VALUE(payload, '$.event_name')                                                AS event_name,
    TO_TIMESTAMP_LTZ(TRY_CAST(JSON_VALUE(payload, '$.event_time_ms') AS BIGINT), 3)    AS event_time,
    TRY_CAST(JSON_VALUE(payload, '$.received_at_ms') AS BIGINT)                        AS received_at_ms,
    TRY_CAST(JSON_VALUE(payload, '$.schema_version') AS INT)                           AS schema_version,
    payload,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    proc_time
FROM kafka_telemetry_events;

INSERT INTO app_events_sink
SELECT
    CAST(event_time AS DATE)                                     AS event_date,
    event_id,
    event_name,
    -- 会话时区是 UTC，转成的 TIMESTAMP 即 UTC 时间
    CAST(event_time AS TIMESTAMP(3))                             AS event_time,
    CAST(TO_TIMESTAMP_LTZ(received_at_ms, 3) AS TIMESTAMP(3))    AS received_at,
    schema_version,
    payload,
    kafka_partition,
    kafka_offset,
    CAST(kafka_ts AS TIMESTAMP(3))                               AS kafka_ts,
    -- 取负：Doris 保留 sequence 较大者，即最早接收的那次。
    -- 没有接收时间的给最小值，同一事件有正常记录时总是让位给正常记录。
    COALESCE(0 - received_at_ms, CAST(-9223372036854775807 AS BIGINT)) AS ingest_seq
FROM telemetry_events_parsed
-- 结构过滤：没有主键 / 没有事件时间（或时间不是数字）的消息无法入库。
-- 业务校验在 telemetry-svc 已经做过，这里不做业务过滤。
WHERE event_id IS NOT NULL
  AND event_time IS NOT NULL
  -- 事件时间范围过滤，以处理时间为基准：
  --   早于 30 天前：超出 ODS 补建的历史分区范围，写进去也会被 Doris 过滤，提前丢掉；
  --   晚于 2 天后：客户端时钟错误，不是真实时间，也会落进提前建的未来分区污染数据。
  -- ⚠️ 被丢掉的事件不会进 ODS，也没有计数。作业停很久后从旧位点追数据时，
  --    「30 天」按追数时的处理时间算，停机期间的老事件可能因此被丢掉。
  AND event_time >= proc_time - INTERVAL '30' DAY
  AND event_time <  proc_time + INTERVAL '2' DAY;


-- ============================================================================
-- 链路 2：telemetry.events.dlq → zymix_ods.app_events_dlq
--   DLQ 消息没有 event_id，主键是 (received_date, kafka_partition, kafka_offset)：
--   同一条消息重放多少次都收敛成一行，所以同样可以关 2PC。
--   来自 Studio 笔记本 zymix_data/telemetry/flink/studio_dlq_to_ods.sql，迁过来后笔记本那个 INSERT 要停掉。
-- ============================================================================

CREATE TABLE kafka_telemetry_dlq (
    payload         STRING,
    kafka_partition INT              METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT           METADATA FROM 'offset'    VIRTUAL,
    kafka_ts        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = '${TELEMETRY_DLQ_TOPIC:telemetry.events.dlq}',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    -- 独立消费组：两张源表各自提交位点，互不影响
    'properties.group.id' = '${ZYMIX_ODS_GROUP_ID}-dlq',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = '${ZYMIX_ODS_STARTUP_MODE:group-offsets}',
    -- 新消费组第一次启动从最早开始，DLQ 全量历史重放一遍（按主键收敛，不会重复）
    'properties.auto.offset.reset' = 'earliest',
    'scan.topic-partition-discovery.interval' = '5min',
    'format' = 'raw'
);

CREATE TABLE app_events_dlq_sink (
    received_date    DATE,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    reason           STRING,
    detail           STRING,
    missing_property STRING,
    event_name       STRING,
    client_event_id  STRING,
    event_time       TIMESTAMP(3),
    received_at      TIMESTAMP(3),
    schema_version   INT,
    app_id           STRING,
    app_version      STRING,
    device_id        STRING,
    payload          STRING,
    kafka_ts         TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = '${DORIS_FENODES}',
    'benodes' = '${DORIS_BENODES:}',
    'table.identifier' = 'zymix_ods.app_events_dlq',
    'username' = '${DORIS_USER}',
    'password' = '${DORIS_PASSWORD}',
    -- 和 app_events_sink 的 label 必须不同，否则两个 sink 的 label 互相冲突
    'sink.label-prefix' = '${ZYMIX_ODS_LABEL_PREFIX}_dlq',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3',
    -- 个别坏行（如字段超长）不拖垮整批
    'sink.properties.max_filter_ratio' = '0.05',
    'sink.enable-2pc' = 'false'
);

INSERT INTO app_events_dlq_sink
SELECT
    -- 按接收日期分区：坏数据的 event_time 可能是 0、负数、未来时间，按它分区会大量落不进分区
    CAST(COALESCE(received_at, kafka_ts_utc) AS DATE)                    AS received_date,
    kafka_partition,
    kafka_offset,
    JSON_VALUE(payload, '$.reason')                                      AS reason,
    JSON_VALUE(payload, '$.detail')                                      AS detail,
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.detail'),
                   'missing required property \W([\w.]+)', 1)          AS missing_property,
    JSON_VALUE(payload, '$.event_name')                                  AS event_name,
    JSON_VALUE(payload, '$.client_event_id')                             AS client_event_id,
    event_time,
    received_at,
    TRY_CAST(JSON_VALUE(payload, '$.schema_version') AS INT)             AS schema_version,
    JSON_VALUE(payload, '$.app_id')                                      AS app_id,
    JSON_VALUE(payload, '$.app_version')                                 AS app_version,
    JSON_VALUE(payload, '$.device_id')                                   AS device_id,
    payload,
    kafka_ts_utc                                                         AS kafka_ts
FROM (
    SELECT
        payload,
        kafka_partition,
        kafka_offset,
        -- 会话时区是 UTC，转成的 TIMESTAMP 即 UTC 时间
        CAST(kafka_ts AS TIMESTAMP(3)) AS kafka_ts_utc,
        -- 只接受 2000-01-01 ~ 2100-01-01 之间的毫秒时间戳，其余置 NULL，避免脏时间戳让 Doris 过滤整行
        CASE WHEN ev_ms BETWEEN 946684800000 AND 4102444800000
             THEN CAST(TO_TIMESTAMP_LTZ(ev_ms, 3) AS TIMESTAMP(3)) END AS event_time,
        CASE WHEN rv_ms BETWEEN 946684800000 AND 4102444800000
             THEN CAST(TO_TIMESTAMP_LTZ(rv_ms, 3) AS TIMESTAMP(3)) END AS received_at
    FROM (
        SELECT
            payload,
            kafka_partition,
            kafka_offset,
            kafka_ts,
            TRY_CAST(JSON_VALUE(payload, '$.event_time_ms')  AS BIGINT) AS ev_ms,
            TRY_CAST(JSON_VALUE(payload, '$.received_at_ms') AS BIGINT) AS rv_ms
        FROM kafka_telemetry_dlq
    ) t0
) t1;
