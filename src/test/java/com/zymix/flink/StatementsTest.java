package com.zymix.flink;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatementsTest {

    @Test
    void substituteWithDefaultsAndMissing() {
        assertEquals("'a' = 'x', 'b' = 'dflt', 'c' = ''",
                Statements.substitute("'a' = '${A}', 'b' = '${B:dflt}', 'c' = '${C:}'", Map.of("A", "x")));
        assertEquals("'p' = 'it''s'", Statements.substitute("'p' = '${P}'", Map.of("P", "it's")));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Statements.substitute("${Y} ${X}", Map.of()));
        assertTrue(e.getMessage().contains("[X, Y]"));
    }

    @Test
    void parseSet() {
        assertArrayEquals(new String[] {"pipeline.name", "job"}, Statements.parseSet("SET 'pipeline.name' = 'job'"));
        assertNull(Statements.parseSet("CREATE TABLE t (a INT)"));
        assertTrue(Statements.managedByService("execution.checkpointing.interval"));
        assertTrue(Statements.managedByService("restart-strategy"));
        assertFalse(Statements.managedByService("pipeline.name"));
    }

    @Test
    void dropsEmptyOptions() {
        String in = "CREATE TABLE t (a INT) WITH ('connector' = 'doris', 'benodes' = '', 'x' = 'it''s')";
        assertEquals("CREATE TABLE t (a INT) WITH ('connector' = 'doris', 'x' = 'it''s')",
                Statements.dropEmptyOptions(in));
        assertEquals("CREATE TABLE t (a INT)", Statements.dropEmptyOptions("CREATE TABLE t (a INT)"));
    }

    @Test
    void appendsKafkaOptionsWithoutOverriding() {
        String in = "CREATE TABLE k (s STRING) WITH (\n  'connector' = 'kafka',\n"
                + "  'properties.security.protocol' = 'PLAINTEXT'\n)";
        assertTrue(Statements.isKafkaTable(in));
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("properties.security.protocol", "SASL_SSL");
        opts.put("properties.sasl.jaas.config", "M required;");
        String out = Statements.appendWithOptions(in, opts);
        assertTrue(out.contains("'properties.security.protocol' = 'PLAINTEXT'"));
        assertFalse(out.contains("SASL_SSL"));
        assertTrue(out.trim().endsWith("'properties.sasl.jaas.config' = 'M required;'\n)"));
    }
}
