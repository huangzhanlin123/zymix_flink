package com.zymix.flink;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlScriptTest {

    @Test
    void splitsOnSemicolonsOutsideStringsAndComments() {
        String script = "-- comment; with semicolon\n"
                + "SET 'a' = 'x;y';\n"
                + "/* block; comment */ CREATE TABLE t (s STRING) WITH ('k' = 'it''s; fine');\n"
                + "INSERT INTO t SELECT 'a;b' -- trailing; comment\n;\n";
        List<String> stmts = SqlScript.split(script);
        assertEquals(3, stmts.size());
        assertEquals("SET 'a' = 'x;y'", stmts.get(0));
        assertEquals("CREATE TABLE t (s STRING) WITH ('k' = 'it''s; fine')", stmts.get(1));
        assertEquals("INSERT INTO t SELECT 'a;b'", stmts.get(2));
    }

    @Test
    void unwrapsStatementSet() {
        String script = "EXECUTE STATEMENT SET BEGIN\n"
                + "INSERT INTO a SELECT 1;\n"
                + "INSERT INTO b SELECT 2;\n"
                + "END;";
        assertEquals(List.of("INSERT INTO a SELECT 1", "INSERT INTO b SELECT 2"), SqlScript.split(script));
    }
}
