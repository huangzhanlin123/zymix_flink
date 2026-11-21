-- zymix_ods.app_events：埋点事件原文（Kafka telemetry.events → Flink → 本表）
--
-- 写入方：Flink 作业 sql/telemetry_kafka_to_zymix_ods.sql，本表唯一写入者。
-- 需要有建表权限的账号在 VeloDB 里执行（readonly_user 不行）。
--
-- 设计与 zymix_next_telemetry.ods_event 一致：
--   - payload 逐字节保留 Kafka 消息原文，不解析业务字段；
--   - UNIQUE KEY(event_date, event_id) + merge-on-write：重放、重复投递按主键收敛；
--   - sequence 列 ingest_seq = -received_at_ms：同一事件多次接收时保留最早接收的那次。
-- 多出来的 kafka_* 三列用于对账和排查（某条数据来自哪个分区哪个位点）。
--
-- 日期一律 UTC。

CREATE DATABASE IF NOT EXISTS zymix_ods;

CREATE TABLE IF NOT EXISTS zymix_ods.app_events (
  `event_date`      DATE          NOT NULL COMMENT "事件日期（UTC，分区键，取自 event_time_ms）",
  `event_id`        VARCHAR(64)   NOT NULL COMMENT "事件唯一 ID：SHA256(version, app_id, device_id, client_event_id)",
  `event_name`      VARCHAR(128)  NULL     COMMENT "事件名",
  `event_time`      DATETIME(3)   NULL     COMMENT "事件时间（UTC）",
  `received_at`    DATETIME(3)   NULL     COMMENT "服务端接收时间（UTC）",
  `schema_version`  INT           NULL     COMMENT "Kafka 消息 schema 版本",
  `payload`         TEXT          NULL     COMMENT "Kafka 消息原文，逐字节保留",
  `kafka_partition` INT           NULL     COMMENT "来源 Kafka 分区",
  `kafka_offset`    BIGINT        NULL     COMMENT "来源 Kafka 位点",
  `kafka_ts`        DATETIME(3)   NULL     COMMENT "Kafka 消息时间戳（UTC）",
  `ingest_seq`      BIGINT        NOT NULL COMMENT "冲突消解序号：负的接收时间戳，保留最早接收的一次"
) ENGINE=OLAP
UNIQUE KEY(`event_date`, `event_id`)
COMMENT "埋点事件 ODS：Kafka telemetry.events 原文"
PARTITION BY RANGE(`event_date`) ()
-- 自动分桶：新分区按最近几个分区的实际大小决定桶数，数据量变化不用手动调。
-- 已建好的分区桶数不会再变。
DISTRIBUTED BY HASH(`event_id`) BUCKETS AUTO
PROPERTIES (
  -- 建表时还没有历史分区可参考，按这个大小估算首批分区（含补建的 30 天）的桶数；
  -- 默认 10G 对这张表偏大，会分出过多的小 tablet
  "estimate_partition_size" = "1G",
  "enable_unique_key_merge_on_write" = "true",
  "function_column.sequence_col" = "ingest_seq",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.time_zone" = "UTC",
  -- 保留一年
  "dynamic_partition.start" = "-365",
  -- 提前建 3 天分区，客户端时钟略快的事件也能落进去
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  -- 建表时补建过去 30 天分区：客户端离线缓存的迟到事件最多晚约 15 天，30 天有余量。
  -- 比分区范围更早的事件写不进来，会被 sink 的 max_filter_ratio 过滤掉。
  "dynamic_partition.create_history_partition" = "true",
  "dynamic_partition.history_partition_num" = "30"
);
