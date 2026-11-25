# zymix_flink

Flink SQL 作业工程，部署到 AWS Managed Service for Apache Flink。

- Flink **1.20**（编译用 1.20.5，`provided`，运行时由托管服务提供）
- JDK **11**
- 连接器版本与生产 jar `telemetry-managed-flink-0.1.3` 一致：Kafka `3.4.0-1.20`、Doris `flink-doris-connector-1.20:25.0.0`、MSK IAM `2.3.2`

## 结构

```
sql/                    Flink SQL 脚本，构建时打进 jar 的 sql/ 目录
  telemetry_kafka_to_zymix_ods.sql  埋点 telemetry.events → zymix_ods.app_events
                                    DLQ  telemetry.events.dlq → zymix_ods.app_events_dlq（同一个作业）
ddl/zymix_ods/          VeloDB 建表语句（需有写权限的账号执行）
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
4. `SET` → 作业配置。托管 Flink 上跳过 `execution.checkpointing.*`、`restart-strategy*`、`state.*`、`parallelism.default`（与生产 jar 规则一致），这些在控制台里配。
5. `KAFKA_IAM=true` 时给 Kafka 表补 `SASL_SSL` + `AWS_MSK_IAM`；设了 `AWS_DATA_ROLE_ARN` 就扮演该角色。
6. 设了 `DORIS_SECRET_ID` 时从 Secrets Manager 读 `username` / `password`，写入 `DORIS_USER` / `DORIS_PASSWORD`。
7. 所有 INSERT 合进一个 StatementSet 提交。

## 常用命令

```bash
# 编译 + 单测 + 打 fat jar（target/zymix-flink-<version>.jar）
mvn package

cp local/example.properties local/ods.properties

# 只看变量替换后的语句，不连 Kafka / Doris
mvn -Plocal -q compile exec:exec -Drun.args="--props local/ods.properties --dry-run"

# 让 Flink 校验 SQL 并打印执行计划（Doris 表临时换成 blackhole），不连外部系统
mvn -Plocal -q compile exec:exec -Drun.args="--props local/ods.properties --explain"

# 本地真跑（MiniCluster，需要能连到 Kafka 和 Doris）
mvn -Plocal -q compile exec:exec -Drun.args="--props local/ods.properties"
```

## 部署到托管 Flink

一键脚本 [scripts/deploy_app_events.sh](scripts/deploy_app_events.sh)，在托管 Flink 所在账号的 AWS CloudShell 里运行：

1. `mvn clean package`，把 `target/zymix-flink-0.1.0-SNAPSHOT.jar` 和脚本上传到 CloudShell。
2. `bash deploy_app_events.sh`：打印部署计划，输入 `yes` 后执行。加 `START=1` 会顺便启动。

脚本复用线上应用 `telemetry_kafka_to_ods` 的服务角色、VPC、代码桶、日志组、Kafka / Doris 地址和 `aws` 属性组（跨账号数据角色），
jar 传到同一个桶，应用不存在就创建（Flink 1.20），存在就更新代码和属性；每次部署自动换 `ZYMIX_ODS_LABEL_PREFIX`。
Doris 账号从 Secrets Manager `next/prod/flink/velodb_etl` 读（字段 `DORIS_USER` / `DORIS_PASSWORD`），密码不进应用配置。
Checkpoint 用服务默认配置，并行度 1；脚本里的同类 SET 会被跳过。

## 监控告警

线上应用 `flink_kafka_velodb_zymix_ods` 有 3 个 CloudWatch 告警，通知发到 SNS 主题 `zymix-data-alerts`（邮件）：

| 告警 | 条件 |
|---|---|
| `zymix-flink-flink_kafka_velodb_zymix_ods-down` | 连续 3 分钟不在运行（无数据也算） |
| `zymix-flink-flink_kafka_velodb_zymix_ods-restarts` | 5 分钟内重启 ≥ 2 次 |
| `zymix-flink-flink_kafka_velodb_zymix_ods-checkpoint-failed` | 5 分钟内 checkpoint 失败 ≥ 2 次 |

完整策略、新应用建告警的命令、加收件人、下线删除，见 `zymix_data/monitoring/README.md` 的「zymix 作业的告警」一节。
新部署一个应用后记得照那里建 3 个告警。

## 注意

- `ZYMIX_ODS_LABEL_PREFIX` 每次重新提交（不从快照恢复）都要换，否则 Doris label 冲突，作业反复重启。
- VeloDB（存算分离）对 MoW 表不支持 2PC stream load，写 VeloDB 的 Doris sink 必须 `'sink.enable-2pc' = 'false'`。
- 新增脚本直接放进 `sql/`，用 `SQL_FILE` 选择；一个托管应用跑一个脚本。
- 一个脚本里可以有多个源表和多条 INSERT，合成一个作业。每个 Kafka 源表用自己的消费组，每个 Doris sink 用自己的 `sink.label-prefix`。
- 改了作业拓扑（增删源表 / INSERT）后，旧快照里的算子 ID 对不上，要停应用、换 `LABEL_PREFIX`、以 `SKIP_RESTORE_FROM_SNAPSHOT` 启动；Kafka 源用 `group-offsets` 从消费组位点续读。
