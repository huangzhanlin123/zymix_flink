-- Kafka → ODS：常驻流式作业
--
-- 这是 ODS 唯一的写入者。它不解析业务字段，只提取分区与主键所需的最小集合，
-- payload 逐字节落库。
--
-- 用 format='raw' 而不是 format='json'：json 格式会把消息解析成列，再写回 ODS
-- 时就成了「重新序列化的结果」，一旦 Flink 的 JSON 处理有任何差异，原文就永久
-- 丢失了。raw 保证 ODS 里的 payload 和 Kafka 里的字节完全一致。
--
-- 消息字段定义见 data-plane/contracts/event-message.schema.json，
-- 该 schema 与 Go producer 结构体由 internal/data/message_test.go 双向断言。

SET 'pipeline.name' = 'telemetry_kafka_to_ods';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'restart-strategy' = 'exponential-delay';

CREATE TABLE kafka_telemetry_events (
    payload STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '${TELEMETRY_EVENT_TOPIC}',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.group.id' = '${TELEMETRY_ODS_GROUP_ID}',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = '${TELEMETRY_ODS_STARTUP_MODE}',
    'format' = 'raw'
);

CREATE TABLE ods_event_sink (
    event_date     DATE,
    event_id       STRING,
    event_name     STRING,
    received_at    TIMESTAMP(3),
    schema_version INT,
    payload        STRING,
    ingest_seq     BIGINT
) WITH (
    'connector' = 'doris',
    'fenodes' = '${DORIS_FENODES}',
    'benodes' = '${DORIS_BENODES:}',
    'table.identifier' = 'zymix_next_telemetry.ods_event',
    'username' = '${DORIS_USER}',
    'password' = '${DORIS_PASSWORD}',
    -- 每次提交必须唯一。Doris 用 label 做写入幂等，而 label 是
    -- 「前缀 + checkpoint 序号」；作业重新提交时 checkpoint 从 1 重新计数，
    -- 固定前缀就会撞上上一轮已完成的 label，作业陷入 RESTARTING 循环。
    -- 由 submit 脚本注入提交时间戳，见 scripts/submit-flink.sh。
    'sink.label-prefix' = '${TELEMETRY_ODS_LABEL_PREFIX}',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3',
    -- ── 一条坏行不能让整条 ingest 停下 ──
    --
    -- Doris 的 max_filter_ratio 默认是 0：**任何**一行被过滤都会让整批 stream
    -- load 失败。加上 2PC，失败的批次会让 checkpoint 失败，作业重启后又读到
    -- 同一行——死循环。
    --
    -- 2026-08-31 实测：一条 event_date=2026-08-25 的迟到事件（客户端离线缓存）
    -- 落不进分区，把整条 ingest 卡了 4 小时，Kafka 积压 2.9 万条。
    -- **一条数据错，所有数据都上不来**，这个形态不能留。
    --
    -- 0.05 不是「允许丢 5%」，是「允许一批里有个别坏行」：正常情况下这个数恒为
    -- 0，任何非零都是异常。系统性的坏数据仍然会超过它并让批次失败——那时该停
    -- 也确实该停。
    --
    -- ⚠️ **放宽比例本身是不够的**：被过滤的行会被静默丢掉。配套的可见性在
    -- check-pipeline-health.sh 的「Kafka → ODS 对账」那一项：消费掉的位点数与
    -- 落库行数对不上就报警。只放宽不看，等于把「卡死」换成了「偷偷丢」——
    -- 那更糟，因为卡死至少有人会发现。
    'sink.properties.max_filter_ratio' = '0.05',
    -- 两阶段提交：43 自建 Doris 上开着（submit-flink.sh 默认 true）。legacy 关掉它换取「Doris 事务过期后
    -- 不会无限重启」，代价是链路降级为 at-least-once 且当时没有唯一键兜底。这里两个保障都在：
    -- 2PC 减少重复，UNIQUE KEY(event_date, event_id) + sequence 列兜住剩下的。
    --
    -- **VeloDB / Doris 存算分离版不支持对 MoW 表做 2PC stream load**
    -- （NOT_IMPLEMENTED_ERROR: stream load 2pc is unsupported for mow table），开着就是「一有数据就失败重启」，
    -- 而 topic 没数据时作业照常 RUNNING、checkpoint 照常成功——看不出来。法兰克福生产（托管 Flink → VeloDB）设 false：
    -- 正确性仍由唯一键保证（重放的重复行按 event_id 收敛），代价是失败重启的窗口里可能先看到后被覆盖的同一行。
    'sink.enable-2pc' = '${TELEMETRY_ODS_ENABLE_2PC}'
);

INSERT INTO ods_event_sink
SELECT
    CAST(TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.event_time_ms') AS BIGINT), 3) AS DATE) AS event_date,
    JSON_VALUE(payload, '$.event_id')                                                        AS event_id,
    JSON_VALUE(payload, '$.event_name')                                                      AS event_name,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.received_at_ms') AS BIGINT), 3)             AS received_at,
    CAST(JSON_VALUE(payload, '$.schema_version') AS INT)                                     AS schema_version,
    payload                                                                                  AS payload,
    -- 取负：Doris 保留 sequence 较大者，即最早接收的那次（见 ODS DDL 注释）
    (0 - CAST(JSON_VALUE(payload, '$.received_at_ms') AS BIGINT))                            AS ingest_seq
FROM kafka_telemetry_events
-- 只丢弃结构上无法入库的消息（无主键 / 无时间）。这不是业务过滤：
-- 业务层面的拒绝已经在 telemetry-svc 完成，并且带原因、带计数、进了 DLQ。
-- legacy 在这个位置放了时间窗谓词，把迟到事件静默滤掉——那是它最隐蔽的丢数路径。
WHERE JSON_VALUE(payload, '$.event_id') IS NOT NULL
  AND JSON_VALUE(payload, '$.event_time_ms') IS NOT NULL;
