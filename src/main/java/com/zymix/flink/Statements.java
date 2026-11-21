package com.zymix.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 单条 SQL 语句的识别与改写。输入都是 {@link SqlScript#split} 切出来、已去掉注释的语句。 */
final class Statements {

    /** ${NAME} 或 ${NAME:default}。 */
    private static final Pattern VAR = Pattern.compile("\\$\\{([A-Za-z0-9_.]+)(?::([^}]*))?}");
    private static final Pattern SET =
            Pattern.compile("^SET\\s+'([^']+)'\\s*=\\s*'((?:[^']|'')*)'$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern INSERT = Pattern.compile("^INSERT\\s+(INTO|OVERWRITE)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_TABLE =
            Pattern.compile("^CREATE\\s+(TEMPORARY\\s+)?TABLE\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern KAFKA_CONNECTOR =
            Pattern.compile("'connector'\\s*=\\s*'(upsert-)?kafka'", Pattern.CASE_INSENSITIVE);
    private static final Pattern DORIS_CONNECTOR =
            Pattern.compile("'connector'\\s*=\\s*'doris'", Pattern.CASE_INSENSITIVE);
    private static final Pattern EMPTY_OPTION =
            Pattern.compile("'[^']+'\\s*=\\s*''(?!')");
    private static final Pattern WITH_OPEN = Pattern.compile("\\bWITH\\s*\\(", Pattern.CASE_INSENSITIVE);

    /**
     * 托管 Flink 自己管 checkpoint / 重启 / 状态 / 并行度，作业里再设会被忽略或和控制台配置冲突，跳过。
     * 规则与生产 jar telemetry-managed-flink-0.1.3 的 Statements.managedByService 一致。
     */
    private static final String[] MANAGED_PREFIXES = {"execution.checkpointing.", "restart-strategy", "state."};

    /** 并行度由控制台的 Parallelism / ParallelismPerKPU（及自动扩缩容）决定。精确匹配，不影响 parallelism.* 的其他键。 */
    private static final String MANAGED_PARALLELISM = "parallelism.default";

    private Statements() {}

    static boolean managedByService(String key) {
        for (String p : MANAGED_PREFIXES) {
            if (key.startsWith(p)) {
                return true;
            }
        }
        return key.equals(MANAGED_PARALLELISM);
    }

    /** SET 'k' = 'v' → {k, v}；不是 SET 返回 null。 */
    static String[] parseSet(String stmt) {
        Matcher m = SET.matcher(stmt.trim());
        if (!m.matches()) {
            return null;
        }
        return new String[] {m.group(1), m.group(2).replace("''", "'")};
    }

    static boolean isInsert(String stmt) {
        return INSERT.matcher(stmt).find();
    }

    static boolean isKafkaTable(String stmt) {
        return CREATE_TABLE.matcher(stmt).find() && KAFKA_CONNECTOR.matcher(stmt).find();
    }

    /** 替换 ${NAME} / ${NAME:default}。缺变量时一次性列出所有缺的名字再失败，省得一个个试。 */
    static String substitute(String stmt, Map<String, String> vars) {
        Matcher m = VAR.matcher(stmt);
        StringBuilder out = new StringBuilder();
        TreeSet<String> missing = new TreeSet<>();
        while (m.find()) {
            String name = m.group(1);
            String value = vars.get(name);
            if (value == null) {
                value = m.group(2);
            }
            if (value == null) {
                missing.add(name);
                value = "";
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value.replace("'", "''")));
        }
        m.appendTail(out);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("缺少变量 " + missing + "，语句：" + head(stmt));
        }
        return out.toString();
    }

    /**
     * 去掉 WITH 里值为空串的选项（如 'benodes' = ''）。配合 ${VAR:} 实现「没配就不传」，
     * 否则连接器会把空串当成一个真的配置值。
     */
    static String dropEmptyOptions(String stmt) {
        int[] range = withClauseRange(stmt);
        if (range == null) {
            return stmt;
        }
        String body = stmt.substring(range[0], range[1]);
        List<String> kept = new ArrayList<>();
        for (String opt : splitOptions(body)) {
            if (!EMPTY_OPTION.matcher(opt.trim()).matches()) {
                kept.add(opt);
            }
        }
        if (kept.size() == splitOptions(body).size()) {
            return stmt;
        }
        return stmt.substring(0, range[0]) + String.join(",", kept) + stmt.substring(range[1]);
    }

    /** 往 WITH ( ... ) 末尾追加选项；表里已有同名选项的不覆盖。 */
    static String appendWithOptions(String stmt, Map<String, String> options) {
        int[] range = withClauseRange(stmt);
        if (range == null) {
            throw new IllegalArgumentException("没有 WITH 子句：" + head(stmt));
        }
        String body = stmt.substring(range[0], range[1]);
        StringBuilder extra = new StringBuilder();
        for (Map.Entry<String, String> e : options.entrySet()) {
            if (body.contains("'" + e.getKey() + "'")) {
                continue;
            }
            extra.append(",\n    '").append(e.getKey()).append("' = '")
                    .append(e.getValue().replace("'", "''")).append('\'');
        }
        String trimmedBody = body.replaceAll("\\s+$", "");
        return stmt.substring(0, range[0]) + trimmedBody + extra + "\n" + stmt.substring(range[1]);
    }

    static boolean isDorisTable(String stmt) {
        return CREATE_TABLE.matcher(stmt).find() && DORIS_CONNECTOR.matcher(stmt).find();
    }

    /** 把 WITH ( ... ) 整个换成 'connector' = 'blackhole'，保留表结构。用于离线校验 SQL。 */
    static String toBlackhole(String stmt) {
        int[] range = withClauseRange(stmt);
        if (range == null) {
            throw new IllegalArgumentException("没有 WITH 子句：" + head(stmt));
        }
        return stmt.substring(0, range[0]) + "'connector' = 'blackhole'" + stmt.substring(range[1]);
    }

    /** 语句第一行，日志用。 */
    static String head(String stmt) {
        String s = stmt.trim();
        int nl = s.indexOf('\n');
        String first = nl < 0 ? s : s.substring(0, nl);
        return first.length() > 120 ? first.substring(0, 120) + "..." : first;
    }

    /** WITH ( 与对应 ) 之间的下标区间 [start, end)，跳过字符串里的括号。 */
    private static int[] withClauseRange(String stmt) {
        Matcher m = WITH_OPEN.matcher(stmt);
        int start = -1;
        // 取最后一个 WITH (：CREATE TABLE 的列定义里不会出现 WITH，SELECT 里的 CTE 不走这里
        while (m.find()) {
            start = m.end();
        }
        if (start < 0) {
            return null;
        }
        int depth = 1;
        boolean inStr = false;
        for (int i = start; i < stmt.length(); i++) {
            char c = stmt.charAt(i);
            if (inStr) {
                if (c == '\'') {
                    if (i + 1 < stmt.length() && stmt.charAt(i + 1) == '\'') {
                        i++;
                    } else {
                        inStr = false;
                    }
                }
            } else if (c == '\'') {
                inStr = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                return new int[] {start, i};
            }
        }
        return null;
    }

    /** 按字符串之外的逗号切 WITH 选项。 */
    private static List<String> splitOptions(String body) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inStr = false;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\'') {
                if (inStr && i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                    cur.append("''");
                    i++;
                    continue;
                }
                inStr = !inStr;
            }
            if (c == ',' && !inStr) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.toString().trim().length() > 0 || out.isEmpty()) {
            out.add(cur.toString());
        }
        return out;
    }
}
