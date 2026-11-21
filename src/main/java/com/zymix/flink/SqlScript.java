package com.zymix.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 把一个 SQL 脚本切成单条语句：去掉注释，按字符串 / 反引号之外的分号切分。 */
final class SqlScript {

    /** EXECUTE STATEMENT SET BEGIN ... END; 的开头。里面的 INSERT 本来就会合进同一个 StatementSet，这层包装直接去掉。 */
    private static final Pattern STATEMENT_SET_OPEN =
            Pattern.compile("^(?:EXECUTE\\s+)?STATEMENT\\s+SET\\s+BEGIN\\s*|^BEGIN\\s+STATEMENT\\s+SET\\s*",
                    Pattern.CASE_INSENSITIVE);

    private SqlScript() {}

    static List<String> split(String script) {
        List<String> raw = splitRaw(script);
        List<String> out = new ArrayList<>();
        for (String stmt : raw) {
            Matcher m = STATEMENT_SET_OPEN.matcher(stmt);
            if (m.find()) {
                stmt = stmt.substring(m.end()).trim();
            }
            if (stmt.isEmpty() || stmt.toUpperCase(Locale.ROOT).equals("END")) {
                continue;
            }
            out.add(stmt);
        }
        return out;
    }

    private static List<String> splitRaw(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        int n = s.length();
        int i = 0;
        while (i < n) {
            char c = s.charAt(i);
            char next = i + 1 < n ? s.charAt(i + 1) : '\0';
            if (c == '-' && next == '-') {
                // 行注释：跳到行尾，保留换行
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && next == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                cur.append(' ');
            } else if (c == '\'' || c == '`' || c == '"') {
                // 字符串 / 标识符：原样复制；'' 是转义的单引号
                cur.append(c);
                i++;
                while (i < n) {
                    char d = s.charAt(i);
                    cur.append(d);
                    i++;
                    if (d == c) {
                        if (i < n && s.charAt(i) == c) {
                            cur.append(c);
                            i++;
                        } else {
                            break;
                        }
                    }
                }
            } else if (c == ';') {
                out.add(cur.toString().trim());
                cur.setLength(0);
                i++;
            } else {
                cur.append(c);
                i++;
            }
        }
        out.add(cur.toString().trim());
        return out;
    }
}
