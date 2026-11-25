-- zymix_ods.app_events_dlq：埋点 DLQ 原文（Kafka telemetry.events.dlq → Flink → 本表）
--
-- 写入方：Flink 作业 sql/telemetry_kafka_to_zymix_ods.sql 的 DLQ 链路，本表唯一写入者。
-- 需要有建表权限的账号在 VeloDB 里执行（readonly_user 不行）。
-- 来源：zymix_data/telemetry/flink/studio_dlq_to_ods.sql 第 0 步，原样搬过来。
--
-- 和 app_events 的区别：
--   1. DLQ 消息没有 event_id → 主键用 (kafka_partition, kafka_offset)，重放收敛成一行，不需要 sequence 列；
--   2. 分区按 received_date（服务端接收日期，UTC，缺失时取 Kafka 时间戳），不按 event_time。
--
-- 日期一律 UTC。

CREATE TABLE IF NOT EXISTS zymix_ods.app_events_dlq (
    `received_date`    DATE          NOT NULL COMMENT "接收日期（UTC，分区键）：取自 received_at_ms，缺失时取 kafka_ts",
    `kafka_partition`  INT           NOT NULL COMMENT "来源 Kafka 分区",
    `kafka_offset`     BIGINT        NOT NULL COMMENT "来源 Kafka 位点；(partition, offset) 唯一确定一条 DLQ 消息",
    `reason`           VARCHAR(128)  NULL     COMMENT "进 DLQ 的原因，如 schema_mismatch",
    `detail`           STRING        NULL     COMMENT "原因详情（telemetry-svc 给出的校验错误原文）",
    `missing_property` VARCHAR(256)  NULL     COMMENT "从 detail 里解析出的缺失必填字段（只对 missing required property 类错误有值）",
    `event_name`       VARCHAR(256)  NULL     COMMENT "事件名",
    `client_event_id`  VARCHAR(512)  NULL     COMMENT "客户端事件 ID",
    `event_time`       DATETIME(3)   NULL     COMMENT "事件时间（UTC）；event_time_ms 不在 2000~2100 年范围内时为 NULL",
    `received_at`      DATETIME(3)   NULL     COMMENT "服务端接收时间（UTC）",
    `schema_version`   INT           NULL     COMMENT "Kafka 消息 schema 版本",
    `app_id`           VARCHAR(128)  NULL     COMMENT "应用包名",
    `app_version`      VARCHAR(64)   NULL     COMMENT "应用版本",
    `device_id`        VARCHAR(256)  NULL     COMMENT "设备 ID",
    `payload`          STRING        NULL     COMMENT "Kafka 消息原文，逐字节保留",
    `kafka_ts`         DATETIME(3)   NULL     COMMENT "Kafka 消息时间戳（UTC）"
) ENGINE=OLAP
UNIQUE KEY(`received_date`, `kafka_partition`, `kafka_offset`)
COMMENT 'app埋点 DLQ ODS：Kafka telemetry.events.dlq 原文（校验未通过的事件）'
PARTITION BY RANGE(`received_date`) ()
DISTRIBUTED BY HASH(`kafka_offset`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.time_zone" = "UTC",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "1",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "60"
);
