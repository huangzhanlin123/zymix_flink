# zymix_flink

Flink SQL 作业工程，部署到 AWS Managed Service for Apache Flink。

- Flink **1.20**（编译用 1.20.5，`provided`，运行时由托管服务提供）
- JDK **11**
- 连接器版本与生产 jar `telemetry-managed-flink-0.1.3` 一致：Kafka `3.4.0-1.20`、Doris `flink-doris-connector-1.20:25.0.0`、MSK IAM `2.3.2`

## 结构

```
sql/                    Flink SQL 脚本，构建时打进 jar 的 sql/ 目录
src/main/java/com/zymix/flink/
  SqlJobRunner.java     入口：读配置 → 解析脚本 → 提交作业
  SqlScript.java        脚本切分（注释 / 字符串 / STATEMENT SET）
  Statements.java       语句识别与改写（${VAR}、SET、WITH 选项）
  AwsAccess.java        MSK IAM 选项、Secrets Manager、跨账号角色
local/example.properties  本地运行的变量模板
config/log4j2-local.properties  本地运行的日志配置
```

## 运行器做什么

1. 读变量：托管 Flink 上来自应用的 Runtime properties（所有分组合并）；本地来自 `--props` 文件，缺的再找环境变量。
2. 读 `SQL_FILE` 指定的脚本（本地文件或 jar 内 `sql/`）。
3. 替换 `${VAR}` / `${VAR:默认值}`；缺变量时一次列出全部缺的名字。值为空串的 WITH 选项会被去掉。
4. `SET` → 作业配置。托管 Flink 上跳过 `execution.checkpointing.*`、`restart-strategy`、`state.*`，这些在控制台里配。
5. `KAFKA_IAM=true` 时给 Kafka 表补 `SASL_SSL` + `AWS_MSK_IAM`；设了 `AWS_DATA_ROLE_ARN` 就扮演该角色。
6. 设了 `DORIS_SECRET_ID` 时从 Secrets Manager 读 `username` / `password`，写入 `DORIS_USER` / `DORIS_PASSWORD`。
7. 所有 INSERT 合进一个 StatementSet 提交。

## 常用命令

```bash
# 编译 + 单测 + 打 fat jar（target/zymix-flink-<version>.jar）
mvn package

# 只看解析结果，不连 Kafka / Doris
cp local/example.properties local/ods.properties
mvn -Plocal -q compile exec:java -Dexec.args="--props local/ods.properties --dry-run"

# 本地真跑（MiniCluster，需要能连到 Kafka 和 Doris）
mvn -Plocal -q compile exec:java -Dexec.args="--props local/ods.properties"
```

## 部署到托管 Flink

1. `mvn package`，把 `target/zymix-flink-<version>.jar` 传到 S3。
2. 应用选 Apache Flink **1.20** 运行时，代码位置指向这个 jar。
3. Runtime properties 填 `local/example.properties` 里的键（分组名随意），至少要有 `SQL_FILE`。
4. Checkpoint / 并行度 / 重启策略在应用配置里设，脚本里的同类 SET 会被跳过。

## 注意

- `TELEMETRY_ODS_LABEL_PREFIX` 每次重新提交（不从快照恢复）都要换，否则 Doris label 冲突，作业反复重启。
- VeloDB（存算分离）对 MoW 表不支持 2PC stream load，`TELEMETRY_ODS_ENABLE_2PC` 必须是 `false`。
- 新增脚本直接放进 `sql/`，用 `SQL_FILE` 选择；一个托管应用跑一个脚本。
