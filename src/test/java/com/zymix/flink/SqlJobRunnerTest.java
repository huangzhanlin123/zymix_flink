package com.zymix.flink;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用仓库里真实的 sql/ 脚本跑一遍计划，保证脚本和运行器对得上。 */
class SqlJobRunnerTest {

    private static final String SCRIPT = "sql/telemetry_kafka_to_zymix_ods.sql";

    private static Map<String, String> vars() {
        Map<String, String> v = new HashMap<>();
        v.put("TELEMETRY_EVENT_TOPIC", "telemetry.events");
        v.put("KAFKA_BROKERS", "b-1:9098");
        v.put("ZYMIX_ODS_GROUP_ID", "zymix-ods");
        v.put("ZYMIX_ODS_LABEL_PREFIX", "zo_1");
        v.put("DORIS_FENODES", "fe:8080");
        v.put("DORIS_USER", "u");
        v.put("DORIS_PASSWORD", "secret");
        return v;
    }

    @Test
    void managedPlan() throws Exception {
        String sql = SqlJobRunner.loadSql(SCRIPT);
        AwsAccess aws = new AwsAccess("arn:aws:iam::1:role/r", "eu-central-1", "eu-central-1", "s");
        SqlJobRunner.Plan plan = SqlJobRunner.plan(sql, vars(), aws.kafkaIamOptions(), true);

        assertEquals(Map.of("pipeline.name", "telemetry_kafka_to_zymix_ods", "table.local-time-zone", "UTC"),
                plan.config);
        assertEquals(List.of("execution.checkpointing.interval", "execution.checkpointing.mode",
                "execution.checkpointing.min-pause", "restart-strategy"), plan.skippedSets);
        // Kafka 源表、Doris 结果表、解析视图
        assertEquals(3, plan.ddl.size());
        assertEquals(1, plan.inserts.size());
        assertTrue(plan.ddl.get(2).startsWith("CREATE TEMPORARY VIEW telemetry_events_parsed"));
        assertFalse(plan.ddl.get(2).contains("sasl"), "视图不是 Kafka 表，不应被追加认证选项");
        String insert = plan.inserts.get(0);
        assertTrue(insert.contains("event_time >= proc_time - INTERVAL '30' DAY"));
        assertTrue(insert.contains("event_time <  proc_time + INTERVAL '2' DAY"));

        String kafka = plan.ddl.get(0);
        assertTrue(kafka.contains("'properties.group.id' = 'zymix-ods'"));
        assertTrue(kafka.contains("'scan.startup.mode' = 'group-offsets'"));
        assertTrue(kafka.contains("'properties.sasl.mechanism' = 'AWS_MSK_IAM'"));
        assertTrue(kafka.contains("awsRoleArn=\"arn:aws:iam::1:role/r\""));

        String doris = plan.ddl.get(1);
        assertTrue(doris.contains("'table.identifier' = 'zymix_ods.app_events'"));
        assertTrue(doris.contains("'sink.enable-2pc' = 'false'"));
        assertFalse(doris.contains("benodes"), "未配置的 DORIS_BENODES 应被去掉");
        assertFalse(doris.contains("AWS_MSK_IAM"));
        assertFalse(SqlJobRunner.mask(doris).contains("secret"));
    }

    @Test
    void firstNonBlankSupportsProductionKeyNames() {
        Map<String, String> m = Map.of("DATA_ROLE_ARN", "arn:r", "AWS_DATA_ROLE_ARN", " ", "DORIS_USER", "etl");
        assertEquals("arn:r", SqlJobRunner.firstNonBlank(m, "AWS_DATA_ROLE_ARN", "DATA_ROLE_ARN"));
        assertEquals("etl", SqlJobRunner.firstNonBlank(m, "DORIS_USER", "user", "username"));
        assertEquals("etl", SqlJobRunner.firstNonBlank(Map.of("user", "etl"), "DORIS_USER", "user", "username"));
        assertEquals(null, SqlJobRunner.firstNonBlank(m, "SECRET_REGION"));
    }

    @Test
    void localPlanKeepsAllSets() throws Exception {
        String sql = SqlJobRunner.loadSql(SCRIPT);
        SqlJobRunner.Plan plan = SqlJobRunner.plan(sql, vars(), Map.of(), false);
        assertEquals(6, plan.config.size());
        assertTrue(plan.skippedSets.isEmpty());
        assertFalse(plan.ddl.get(0).contains("sasl"));
    }
}
