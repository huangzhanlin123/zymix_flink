package com.zymix.flink;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用仓库里真实的 sql/kafka_to_ods.sql 跑一遍计划，保证脚本和运行器对得上。 */
class SqlJobRunnerTest {

    private static Map<String, String> odsVars() {
        Map<String, String> v = new HashMap<>();
        v.put("TELEMETRY_EVENT_TOPIC", "events");
        v.put("KAFKA_BROKERS", "b-1:9098");
        v.put("TELEMETRY_ODS_GROUP_ID", "g");
        v.put("TELEMETRY_ODS_STARTUP_MODE", "latest-offset");
        v.put("DORIS_FENODES", "fe:8030");
        v.put("DORIS_USER", "u");
        v.put("DORIS_PASSWORD", "secret");
        v.put("TELEMETRY_ODS_LABEL_PREFIX", "ods_1");
        v.put("TELEMETRY_ODS_ENABLE_2PC", "false");
        return v;
    }

    @Test
    void managedPlanForOdsScript() throws Exception {
        String sql = SqlJobRunner.loadSql("sql/kafka_to_ods.sql");
        AwsAccess aws = new AwsAccess("arn:aws:iam::1:role/r", "eu-central-1", "s");
        SqlJobRunner.Plan plan = SqlJobRunner.plan(sql, odsVars(), aws.kafkaIamOptions(), true);

        assertEquals(Map.of("pipeline.name", "telemetry_kafka_to_ods"), plan.config);
        assertEquals(List.of("execution.checkpointing.interval", "execution.checkpointing.mode",
                "execution.checkpointing.min-pause", "restart-strategy"), plan.skippedSets);
        assertEquals(2, plan.ddl.size());
        assertEquals(1, plan.inserts.size());

        String kafka = plan.ddl.get(0);
        assertTrue(kafka.contains("'properties.sasl.mechanism' = 'AWS_MSK_IAM'"));
        assertTrue(kafka.contains("awsRoleArn=\"arn:aws:iam::1:role/r\""));
        String doris = plan.ddl.get(1);
        assertFalse(doris.contains("benodes"), "未配置的 DORIS_BENODES 应被去掉");
        assertFalse(doris.contains("AWS_MSK_IAM"));
        assertFalse(SqlJobRunner.mask(doris).contains("secret"));
    }

    @Test
    void localPlanKeepsAllSets() throws Exception {
        String sql = SqlJobRunner.loadSql("sql/kafka_to_ods.sql");
        SqlJobRunner.Plan plan = SqlJobRunner.plan(sql, odsVars(), Map.of(), false);
        assertEquals(5, plan.config.size());
        assertTrue(plan.skippedSets.isEmpty());
        assertFalse(plan.ddl.get(0).contains("sasl"));
    }
}
