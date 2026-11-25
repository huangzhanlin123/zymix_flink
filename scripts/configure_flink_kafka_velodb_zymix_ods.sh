#!/usr/bin/env bash
# =============================================================================
# 一键配置托管 Flink 应用 flink_kafka_velodb_zymix_ods
#   Kafka telemetry.events     → zymix_ods.app_events
#   Kafka telemetry.events.dlq → zymix_ods.app_events_dlq（jar：zymix_flink 仓库）
#
# 在 268502217808 账号的 AWS CloudShell（eu-central-1）里运行：
#   bash configure_flink_kafka_velodb_zymix_ods.sh            # 只配置，不启动
#   START=1 bash configure_flink_kafka_velodb_zymix_ods.sh    # 配置完启动（首次不从快照恢复）
#
# 参照线上应用 telemetry_kafka_to_ods 的配置（同一个服务角色、数据角色、VPC、日志组）。
# 会做的事（每一步改动前都会打印出来，确认后才执行）：
#   1. 检查 jar 在 S3
#   2. 检查数据角色能否读 velodb_etl secret、能否用新的消费组读两个 topic；缺的权限确认后补到数据角色上
#   3. 应用不存在就创建，存在就更新：代码位置、运行时属性、快照开、并行度 1、关自动扩缩、VPC、日志
#   4. START=1 时启动
# Doris 密码只在运行时从 Secrets Manager 读，本脚本不读、不打印。
# =============================================================================
set -euo pipefail

REGION="${REGION:-eu-central-1}"
APP="${APP:-flink_kafka_velodb_zymix_ods}"
REF_APP="${REF_APP:-telemetry_kafka_to_ods}"

ACCOUNT_EXPECTED="268502217808"
EXEC_ROLE_NAME="next-prod-flink-exec-telemetry-ods"
DATA_ROLE_ARN="arn:aws:iam::268502217808:role/next-prod-flink-telemetry-ods"
DATA_ROLE_NAME="${DATA_ROLE_ARN##*/}"

BUCKET="${BUCKET:-zymix-flink-268502217808-eu-central-1-an}"
JAR_KEY="${JAR_KEY:-flink/zymix-flink-0.1.0-SNAPSHOT.jar}"

SECRET_NAME="next/prod/flink/velodb_etl"
SECRET_ARN="arn:aws:secretsmanager:eu-central-1:268502217808:secret:next/prod/flink/velodb_etl-sEvJR6"

KAFKA_BROKERS="boot-syluwxnk.c3.kafka-serverless.eu-central-1.amazonaws.com:9098"
TOPIC="telemetry.events"
DLQ_TOPIC="telemetry.events.dlq"
GROUP_ID="${GROUP_ID:-zymix-ods-app-events}"          # 不能和线上的 telemetry-ods 相同；DLQ 源表用 ${GROUP_ID}-dlq
LABEL_PREFIX="${LABEL_PREFIX:-zymix_ods_app_events_v1}" # 不从快照恢复的重新启动时要换（v2、v3…）；DLQ sink 用 ${LABEL_PREFIX}_dlq
DORIS_FENODES="lb-95540229-09f9844ddebbbe4e.elb.eu-central-1.amazonaws.com:8080"

SUBNETS=(subnet-09e9c3453d32c1fd6 subnet-0d2f2bfce06aff583 subnet-0adc332e4192b39d2)
SECURITY_GROUPS=(sg-04b9c75a61ce65cdc)

START="${START:-0}"
YES="${YES:-0}"

AWS=(aws --region "$REGION")
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[OK]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[警告]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[错误]\033[0m %s\n' "$*" >&2; exit 1; }
confirm() {
  [[ "$YES" == 1 ]] && return 0
  local ans; read -r -p "$1 输入 yes 继续：" ans; [[ "$ans" == yes ]]
}

command -v jq >/dev/null || die "找不到 jq"
[[ "$GROUP_ID" != "telemetry-ods" ]] || die "消费组不能和线上作业相同（telemetry-ods）"

# -----------------------------------------------------------------------------
# 0. 账号和 jar
# -----------------------------------------------------------------------------
ACCOUNT="$("${AWS[@]}" sts get-caller-identity --query Account --output text)"
[[ "$ACCOUNT" == "$ACCOUNT_EXPECTED" ]] || die "当前账号是 $ACCOUNT，应在 $ACCOUNT_EXPECTED 里运行"
log "账号 $ACCOUNT，区域 $REGION，应用 $APP"

"${AWS[@]}" s3api head-object --bucket "$BUCKET" --key "$JAR_KEY" >/dev/null 2>&1 \
  || die "S3 上没有 s3://$BUCKET/$JAR_KEY"
ok "jar 在 s3://$BUCKET/$JAR_KEY"

# -----------------------------------------------------------------------------
# 1. 数据角色的权限：读 velodb_etl secret、用新消费组读 MSK
# -----------------------------------------------------------------------------
log "定位 MSK 集群（bootstrap = $KAFKA_BROKERS）"
CLUSTER_ARN=""
for arn in $("${AWS[@]}" kafka list-clusters-v2 --query 'ClusterInfoList[].ClusterArn' --output text 2>/dev/null || true); do
  bs="$("${AWS[@]}" kafka get-bootstrap-brokers --cluster-arn "$arn" --query 'BootstrapBrokerStringSaslIam' --output text 2>/dev/null || true)"
  if [[ "$bs" == *"${KAFKA_BROKERS%%:*}"* ]]; then CLUSTER_ARN="$arn"; break; fi
done
if [[ -n "$CLUSTER_ARN" ]]; then
  ok "MSK 集群 $CLUSTER_ARN"
  CLUSTER_PATH="${CLUSTER_ARN#*:cluster/}"                 # <name>/<uuid>
  KAFKA_ARN_PREFIX="${CLUSTER_ARN%%:cluster/*}"            # arn:aws:kafka:<region>:<account>
  TOPIC_ARN="${KAFKA_ARN_PREFIX}:topic/${CLUSTER_PATH}/${TOPIC}"
  DLQ_TOPIC_ARN="${KAFKA_ARN_PREFIX}:topic/${CLUSTER_PATH}/${DLQ_TOPIC}"
  GROUP_ARN="${KAFKA_ARN_PREFIX}:group/${CLUSTER_PATH}/${GROUP_ID}"
  DLQ_GROUP_ARN="${KAFKA_ARN_PREFIX}:group/${CLUSTER_PATH}/${GROUP_ID}-dlq"
else
  warn "没找到 bootstrap 对应的 MSK 集群（可能没有 kafka:ListClustersV2 权限），跳过 MSK 权限检查"
fi

# simulate-principal-policy：返回被拒绝的 action（空 = 都允许）；没权限模拟时返回 "?"
denied_actions() {  # $1 = 资源 ARN，其余 = action
  local res="$1"; shift
  local out
  out="$("${AWS[@]}" iam simulate-principal-policy --policy-source-arn "$DATA_ROLE_ARN" \
          --action-names "$@" --resource-arns "$res" \
          --query 'EvaluationResults[?EvalDecision!=`allowed`].EvalActionName' --output text 2>/dev/null)" \
    || { echo "?"; return; }
  [[ "$out" == None ]] && out=""
  echo "$out"
}

MISSING_STATEMENTS="[]"
add_statement() {  # $1 = sid, $2 = 资源 ARN（JSON 数组）, 其余 = action
  local sid="$1" res="$2"; shift 2
  MISSING_STATEMENTS="$(jq -c --arg sid "$sid" --argjson res "$res" --args \
    '. + [{Sid: $sid, Effect: "Allow", Action: $ARGS.positional, Resource: $res}]' \
    <<<"$MISSING_STATEMENTS" "$@")"
}

log "检查数据角色 $DATA_ROLE_NAME 的权限"
SIM_UNAVAILABLE=0
d="$(denied_actions "$SECRET_ARN" secretsmanager:GetSecretValue)"
if [[ "$d" == "?" ]]; then
  SIM_UNAVAILABLE=1
elif [[ -n "$d" ]]; then
  warn "数据角色读不了 $SECRET_NAME"
  add_statement ReadVelodbEtlSecret "[\"$SECRET_ARN\"]" secretsmanager:GetSecretValue
else
  ok "数据角色能读 $SECRET_NAME"
fi

if [[ -n "$CLUSTER_ARN" && "$SIM_UNAVAILABLE" == 0 ]]; then
  d1="$(denied_actions "$CLUSTER_ARN" kafka-cluster:Connect kafka-cluster:DescribeCluster)"
  d2="$(denied_actions "$TOPIC_ARN" kafka-cluster:DescribeTopic kafka-cluster:ReadData)"
  d3="$(denied_actions "$GROUP_ARN" kafka-cluster:DescribeGroup kafka-cluster:AlterGroup)"
  d4="$(denied_actions "$DLQ_TOPIC_ARN" kafka-cluster:DescribeTopic kafka-cluster:ReadData)"
  d5="$(denied_actions "$DLQ_GROUP_ARN" kafka-cluster:DescribeGroup kafka-cluster:AlterGroup)"
  [[ -z "$d1" ]] && ok "数据角色能连接 MSK 集群" || { warn "缺集群权限：$d1"
    add_statement MskConnect "[\"$CLUSTER_ARN\"]" kafka-cluster:Connect kafka-cluster:DescribeCluster; }
  [[ -z "$d2" ]] && ok "数据角色能读 topic $TOPIC" || { warn "缺 topic 权限：$d2"
    add_statement MskReadTopic "[\"$TOPIC_ARN\"]" kafka-cluster:DescribeTopic kafka-cluster:ReadData; }
  [[ -z "$d3" ]] && ok "数据角色能用消费组 $GROUP_ID" || { warn "缺消费组权限：$d3"
    add_statement MskZymixOdsGroup "[\"$GROUP_ARN\"]" kafka-cluster:DescribeGroup kafka-cluster:AlterGroup; }
  [[ -z "$d4" ]] && ok "数据角色能读 topic $DLQ_TOPIC" || { warn "缺 topic 权限：$d4"
    add_statement MskReadDlqTopic "[\"$DLQ_TOPIC_ARN\"]" kafka-cluster:DescribeTopic kafka-cluster:ReadData; }
  [[ -z "$d5" ]] && ok "数据角色能用消费组 ${GROUP_ID}-dlq" || { warn "缺消费组权限：$d5"
    add_statement MskZymixOdsDlqGroup "[\"$DLQ_GROUP_ARN\"]" kafka-cluster:DescribeGroup kafka-cluster:AlterGroup; }
fi
[[ "$SIM_UNAVAILABLE" == 1 ]] && warn "没有 iam:SimulatePrincipalPolicy 权限，无法预检数据角色；启动后看日志是否有 AccessDenied"

if [[ "$(jq length <<<"$MISSING_STATEMENTS")" -gt 0 ]]; then
  # put-role-policy 会整体覆盖同名内联策略：先取出已有语句，按 Sid 合并，否则重跑脚本会丢掉之前加的权限
  EXISTING_STATEMENTS="$("${AWS[@]}" iam get-role-policy --role-name "$DATA_ROLE_NAME" \
      --policy-name zymix-ods-app-events --query 'PolicyDocument.Statement' --output json 2>/dev/null || echo '[]')"
  POLICY="$(jq -c --argjson old "$EXISTING_STATEMENTS" '
    ([.[].Sid] as $new | [$old[] | select(.Sid as $s | $new | index($s) | not)]) + .
    | {Version: "2012-10-17", Statement: .}' <<<"$MISSING_STATEMENTS")"
  echo
  echo "数据角色 $DATA_ROLE_NAME（线上作业也在用）的内联策略 zymix-ods-app-events 需要补上以下权限："
  jq . <<<"$MISSING_STATEMENTS"
  echo "（合并已有语句后的完整策略共 $(jq '.Statement | length' <<<"$POLICY") 条）"
  if confirm "给数据角色加上这条策略？"; then
    "${AWS[@]}" iam put-role-policy --role-name "$DATA_ROLE_NAME" \
      --policy-name zymix-ods-app-events --policy-document "$POLICY"
    ok "已添加，等 10 秒让 IAM 生效"; sleep 10
  else
    die "已取消。没有这些权限，作业启动后会报 AccessDenied"
  fi
fi

# -----------------------------------------------------------------------------
# 2. 日志流：放在线上应用的日志组里（服务角色已有写权限）
# -----------------------------------------------------------------------------
REF_JSON="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$REF_APP")"
REF_LOG_STREAM_ARN="$(jq -r '.ApplicationDetail.CloudWatchLoggingOptionDescriptions[0].LogStreamARN // empty' <<<"$REF_JSON")"
EXEC_ROLE_ARN="$(jq -r '.ApplicationDetail.ServiceExecutionRole' <<<"$REF_JSON")"
[[ "$EXEC_ROLE_ARN" == *"/$EXEC_ROLE_NAME" ]] || warn "线上应用的服务角色是 $EXEC_ROLE_ARN，和预期的 $EXEC_ROLE_NAME 不同，按线上的用"
LOG_STREAM_ARN=""
if [[ -n "$REF_LOG_STREAM_ARN" ]]; then
  LOG_GROUP_ARN="${REF_LOG_STREAM_ARN%%:log-stream:*}"
  LOG_GROUP="${LOG_GROUP_ARN#*:log-group:}"
  LOG_STREAM_ARN="${LOG_GROUP_ARN}:log-stream:${APP}"
fi

# -----------------------------------------------------------------------------
# 3. 运行时属性（键名与线上应用一致，运行器会合并所有属性组）
# -----------------------------------------------------------------------------
PROPERTY_GROUPS="$(jq -nc \
  --arg brokers "$KAFKA_BROKERS" --arg topic "$TOPIC" --arg gid "$GROUP_ID" --arg label "$LABEL_PREFIX" \
  --arg fe "$DORIS_FENODES" --arg role "$DATA_ROLE_ARN" --arg secret "$SECRET_NAME" \
  --arg session "$APP" --arg region "$REGION" '[
  {PropertyGroupId: "job", PropertyMap: {
     SQL_FILE: "telemetry_kafka_to_zymix_ods.sql",
     KAFKA_IAM: "true",
     KAFKA_BROKERS: $brokers,
     TELEMETRY_EVENT_TOPIC: $topic,
     ZYMIX_ODS_GROUP_ID: $gid,
     ZYMIX_ODS_STARTUP_MODE: "group-offsets",
     ZYMIX_ODS_LABEL_PREFIX: $label,
     DORIS_FENODES: $fe}},
  {PropertyGroupId: "aws", PropertyMap: {
     DATA_ROLE_ARN: $role,
     DORIS_SECRET_ID: $secret,
     ROLE_SESSION_NAME: $session,
     SECRET_REGION: $region,
     STS_REGION: $region}}]')"

BUCKET_ARN="arn:aws:s3:::${BUCKET}"
SUBNETS_JSON="$(printf '%s\n' "${SUBNETS[@]}" | jq -R . | jq -sc .)"
SGS_JSON="$(printf '%s\n' "${SECURITY_GROUPS[@]}" | jq -R . | jq -sc .)"

EXISTING="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$APP" 2>/dev/null || true)"

echo
echo "================ 配置计划 ================"
echo "应用          : $APP（$([[ -n "$EXISTING" ]] && jq -r '"已存在，状态 " + .ApplicationDetail.ApplicationStatus' <<<"$EXISTING" || echo 不存在，将创建)）"
echo "服务角色      : $EXEC_ROLE_ARN"
echo "代码          : s3://$BUCKET/$JAR_KEY"
echo "运行时        : FLINK-1_20，快照开，checkpoint 默认，并行度 1 / 每 KPU 1，自动扩缩关"
echo "VPC           : ${SUBNETS[*]} / ${SECURITY_GROUPS[*]}"
echo "日志流        : ${LOG_STREAM_ARN:-（无）}"
echo "运行时属性    :"; jq . <<<"$PROPERTY_GROUPS"
echo "启动          : $([[ "$START" == 1 ]] && echo 是 || echo '否（START=1 才启动）')"
echo "=========================================="
confirm "按以上配置应用？" || die "已取消，应用没有改动"

if [[ -n "$LOG_STREAM_ARN" ]]; then
  if ! out="$("${AWS[@]}" logs create-log-stream --log-group-name "$LOG_GROUP" --log-stream-name "$APP" 2>&1)"; then
    grep -q ResourceAlreadyExistsException <<<"$out" || die "建日志流失败：$out"
  fi
  ok "日志流 $LOG_GROUP / $APP"
fi

# -----------------------------------------------------------------------------
# 4. 创建或更新
# -----------------------------------------------------------------------------
version() { "${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$APP" \
              --query 'ApplicationDetail.ApplicationVersionId' --output text; }

if [[ -z "$EXISTING" ]]; then
  log "创建应用 $APP"
  CREATE_JSON="$(jq -nc --arg name "$APP" --arg role "$EXEC_ROLE_ARN" --arg bucket "$BUCKET_ARN" --arg key "$JAR_KEY" \
    --argjson groups "$PROPERTY_GROUPS" --argjson subnets "$SUBNETS_JSON" --argjson sgs "$SGS_JSON" \
    --arg logarn "$LOG_STREAM_ARN" '{
      ApplicationName: $name,
      ApplicationDescription: "Kafka telemetry.events(.dlq) -> zymix_ods.app_events(_dlq)",
      RuntimeEnvironment: "FLINK-1_20",
      ServiceExecutionRole: $role,
      ApplicationConfiguration: {
        ApplicationCodeConfiguration: {CodeContent: {S3ContentLocation: {BucketARN: $bucket, FileKey: $key}},
                                       CodeContentType: "ZIPFILE"},
        EnvironmentProperties: {PropertyGroups: $groups},
        FlinkApplicationConfiguration: {
          CheckpointConfiguration: {ConfigurationType: "DEFAULT"},
          MonitoringConfiguration: {ConfigurationType: "CUSTOM", MetricsLevel: "APPLICATION", LogLevel: "INFO"},
          ParallelismConfiguration: {ConfigurationType: "CUSTOM", Parallelism: 1, ParallelismPerKPU: 1,
                                     AutoScalingEnabled: false}},
        ApplicationSnapshotConfiguration: {SnapshotsEnabled: true},
        VpcConfigurations: [{SubnetIds: $subnets, SecurityGroupIds: $sgs}]}
    } + (if $logarn == "" then {} else {CloudWatchLoggingOptions: [{LogStreamARN: $logarn}]} end)')"
  "${AWS[@]}" kinesisanalyticsv2 create-application --cli-input-json "$CREATE_JSON" \
    --query 'ApplicationDetail.ApplicationStatus' --output text
else
  STATUS="$(jq -r '.ApplicationDetail.ApplicationStatus' <<<"$EXISTING")"
  [[ "$STATUS" == READY || "$STATUS" == RUNNING ]] || die "应用状态是 $STATUS，等它变成 READY 或 RUNNING 再运行"

  log "更新应用 $APP：代码、属性、快照、并行度"
  UPDATE_JSON="$(jq -nc --arg bucket "$BUCKET_ARN" --arg key "$JAR_KEY" --argjson groups "$PROPERTY_GROUPS" '{
    ApplicationCodeConfigurationUpdate: {CodeContentTypeUpdate: "ZIPFILE",
      CodeContentUpdate: {S3ContentLocationUpdate: {BucketARNUpdate: $bucket, FileKeyUpdate: $key}}},
    EnvironmentPropertyUpdates: {PropertyGroups: $groups},
    FlinkApplicationConfigurationUpdate: {
      CheckpointConfigurationUpdate: {ConfigurationTypeUpdate: "DEFAULT"},
      MonitoringConfigurationUpdate: {ConfigurationTypeUpdate: "CUSTOM", MetricsLevelUpdate: "APPLICATION",
                                      LogLevelUpdate: "INFO"},
      ParallelismConfigurationUpdate: {ConfigurationTypeUpdate: "CUSTOM", ParallelismUpdate: 1,
                                       ParallelismPerKPUUpdate: 1, AutoScalingEnabledUpdate: false}},
    ApplicationSnapshotConfigurationUpdate: {SnapshotsEnabledUpdate: true}}')"
  "${AWS[@]}" kinesisanalyticsv2 update-application --application-name "$APP" \
    --current-application-version-id "$(version)" --application-configuration-update "$UPDATE_JSON" \
    --query 'ApplicationDetail.ApplicationStatus' --output text

  if [[ "$(jq '[.ApplicationDetail.ApplicationConfigurationDescription.VpcConfigurationDescriptions[]?] | length' <<<"$EXISTING")" == 0 ]]; then
    log "补 VPC 配置"
    "${AWS[@]}" kinesisanalyticsv2 add-application-vpc-configuration --application-name "$APP" \
      --current-application-version-id "$(version)" \
      --vpc-configuration "$(jq -nc --argjson s "$SUBNETS_JSON" --argjson g "$SGS_JSON" '{SubnetIds: $s, SecurityGroupIds: $g}')" \
      --query 'ApplicationVersionId' --output text
  else
    ok "已有 VPC 配置：$(jq -c '.ApplicationDetail.ApplicationConfigurationDescription.VpcConfigurationDescriptions[0] | {SubnetIds, SecurityGroupIds}' <<<"$EXISTING")"
  fi

  if [[ -n "$LOG_STREAM_ARN" && "$(jq '[.ApplicationDetail.CloudWatchLoggingOptionDescriptions[]?] | length' <<<"$EXISTING")" == 0 ]]; then
    log "补 CloudWatch 日志"
    "${AWS[@]}" kinesisanalyticsv2 add-application-cloud-watch-logging-option --application-name "$APP" \
      --current-application-version-id "$(version)" --cloud-watch-logging-option "LogStreamARN=$LOG_STREAM_ARN" \
      --query 'ApplicationVersionId' --output text
  fi
fi

# -----------------------------------------------------------------------------
# 5. 启动（可选）
# -----------------------------------------------------------------------------
wait_status() {
  local s
  for _ in $(seq 1 90); do
    s="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$APP" \
          --query 'ApplicationDetail.ApplicationStatus' --output text)"
    [[ "$s" == *ING ]] || { echo "$s"; return; }
    sleep 10
  done
  echo "$s"
}
STATUS="$(wait_status)"
ok "应用状态：$STATUS"

if [[ "$START" == 1 ]]; then
  if [[ "$STATUS" == READY ]]; then
    log "启动（首次不从快照恢复）"
    "${AWS[@]}" kinesisanalyticsv2 start-application --application-name "$APP" \
      --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
    ok "应用状态：$(wait_status)"
  else
    log "应用是 $STATUS，不重复启动（运行中的应用更新后会按新配置从快照重启）"
  fi
fi

cat <<EOF

完成。
- 控制台：https://${REGION}.console.aws.amazon.com/flink/home?region=${REGION}#/applications/${APP}
- 启动：aws --region ${REGION} kinesisanalyticsv2 start-application --application-name ${APP} --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}}'
- 日志：aws --region ${REGION} logs tail '${LOG_GROUP:-<日志组>}' --log-stream-names ${APP} --follow --since 10m
- 正常的标志：日志里有「Doris 账号已从 Secrets Manager 读取」「作业已提交」，Flink 控制面板作业 RUNNING、checkpoint 成功，
  zymix_ods.app_events 和 zymix_ods.app_events_dlq 都有新数据
- 以后不从快照恢复的启动，先改 LABEL_PREFIX（如 v2）再运行本脚本
EOF
