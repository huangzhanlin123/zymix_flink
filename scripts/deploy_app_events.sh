#!/usr/bin/env bash
# =============================================================================
# 一键部署：托管 Flink 应用 zymix_ods_app_events（Kafka telemetry.events → zymix_ods.app_events）
#
# 在哪里跑：托管 Flink 应用所在账号的 AWS CloudShell（eu-central-1）。
#   1. CloudShell 右上角「操作 → 上传文件」，上传 target/zymix-flink-0.1.0-SNAPSHOT.jar 和本脚本
#   2. bash deploy_app_events.sh
#
# 它做什么（先打印计划，确认后才改动）：
#   - 读线上应用 telemetry_kafka_to_ods（REF_APP）的配置，复用：服务角色、VPC、代码桶、日志组、
#     Kafka 地址、Doris 地址、跨账号数据角色（aws 属性组）
#   - jar 传到同一个代码桶
#   - 应用不存在就创建（Flink 1.20），已存在就更新代码和属性
#   - Doris 账号走 Secrets Manager：DORIS_SECRET_ID = 下面的 SECRET_ARN（字段 DORIS_USER / DORIS_PASSWORD），
#     运行器启动时读取，密码不进应用配置、本脚本也不读不打印
#   - 默认不启动；START=1 时创建 / 更新后启动
#
# 可用环境变量覆盖下面任意配置，例如：
#   START=1 bash deploy_app_events.sh
#   S3_BUCKET=my-bucket S3_PREFIX=flink/jars bash deploy_app_events.sh   # jar 传到自己的桶
#   YES=1 JAR_PATH=~/zymix-flink.jar bash deploy_app_events.sh
# =============================================================================
set -euo pipefail

REGION="${REGION:-eu-central-1}"
REF_APP="${REF_APP:-telemetry_kafka_to_ods}"
APP_NAME="${APP_NAME:-zymix_ods_app_events}"
JAR_PATH="${JAR_PATH:-./zymix-flink-0.1.0-SNAPSHOT.jar}"
# jar 放哪个桶：不填就放参考应用的代码桶；填了就放自己的桶（必须在同一区域，且服务角色能读）
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-zymix-flink}"
SECRET_ARN="${SECRET_ARN:-arn:aws:secretsmanager:eu-central-1:268502217808:secret:next/prod/flink/velodb_etl-sEvJR6}"

SQL_FILE="${SQL_FILE:-telemetry_kafka_to_zymix_ods.sql}"
TELEMETRY_EVENT_TOPIC_DEFAULT="telemetry.events"
# 不能和线上作业的消费组相同
ZYMIX_ODS_GROUP_ID="${ZYMIX_ODS_GROUP_ID:-zymix-ods-app-events}"
ZYMIX_ODS_STARTUP_MODE="${ZYMIX_ODS_STARTUP_MODE:-group-offsets}"

# 资源：数据量小，1 并行度 / 1 KPU 起步，关自动扩缩容
PARALLELISM="${PARALLELISM:-1}"
PARALLELISM_PER_KPU="${PARALLELISM_PER_KPU:-1}"
AUTO_SCALING="${AUTO_SCALING:-false}"

START="${START:-0}"
YES="${YES:-0}"

TS="$(date -u +%Y%m%d%H%M%S)"
# 每次部署换一个 label 前缀：Doris 用「前缀 + checkpoint 序号」做幂等，不换会和上一轮撞车
ZYMIX_ODS_LABEL_PREFIX="${ZYMIX_ODS_LABEL_PREFIX:-zymix_ods_app_events_${TS}}"

AWS=(aws --region "$REGION")
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[警告]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[错误]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 0. 前置检查
# ---------------------------------------------------------------------------
command -v aws >/dev/null || die "找不到 aws 命令（请在 AWS CloudShell 里运行）"
command -v jq  >/dev/null || die "找不到 jq"
[[ -f "$JAR_PATH" ]] || die "找不到 jar：$JAR_PATH（先上传到 CloudShell，或用 JAR_PATH=... 指定）"
if command -v unzip >/dev/null; then
  unzip -Z1 "$JAR_PATH" "sql/${SQL_FILE}" >/dev/null 2>&1 \
    || die "jar 里没有 sql/${SQL_FILE}，确认上传的是最新打的包"
fi

ACCOUNT="$("${AWS[@]}" sts get-caller-identity --query Account --output text)" \
  || die "AWS 凭证不可用"
log "当前账号 $ACCOUNT，区域 $REGION"

# ---------------------------------------------------------------------------
# 1. 读参考应用的配置
# ---------------------------------------------------------------------------
log "读取参考应用 $REF_APP 的配置"
REF_JSON="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$REF_APP" \
  --include-additional-details 2>&1)" \
  || die "读不到 $REF_APP：$REF_JSON
      （确认在托管 Flink 应用所在的账号里运行，且有 kinesisanalytics:DescribeApplication 权限；
       应用名不同时用 REF_APP=... 指定）"

jqr() { jq -r "$1" <<<"$REF_JSON"; }
CFG='.ApplicationDetail.ApplicationConfigurationDescription'

ROLE_ARN="$(jqr '.ApplicationDetail.ServiceExecutionRole')"
BUCKET_ARN="$(jqr "$CFG.ApplicationCodeConfigurationDescription.CodeContentDescription.S3ApplicationCodeLocationDescription.BucketARN")"
REF_KEY="$(jqr "$CFG.ApplicationCodeConfigurationDescription.CodeContentDescription.S3ApplicationCodeLocationDescription.FileKey")"
SUBNETS="$(jq -c "[$CFG.VpcConfigurationDescriptions[]?.SubnetIds[]]" <<<"$REF_JSON")"
SGS="$(jq -c "[$CFG.VpcConfigurationDescriptions[]?.SecurityGroupIds[]]" <<<"$REF_JSON")"
REF_LOG_STREAM_ARN="$(jqr '.ApplicationDetail.CloudWatchLoggingOptionDescriptions[0].LogStreamARN // empty')"

# 所有属性组拍平成一张表（参考应用把属性分在 job / aws 两个组）
REF_PROPS="$(jq -c "[$CFG.EnvironmentPropertyDescriptions.PropertyGroupDescriptions[]?.PropertyMap] | add // {}" <<<"$REF_JSON")"
ref_prop() { jq -r --arg k "$1" '.[$k] // empty' <<<"$REF_PROPS"; }
# aws 组原样复用：跨账号数据角色、STS / Secrets Manager 区域
REF_AWS_GROUP="$(jq -c "[$CFG.EnvironmentPropertyDescriptions.PropertyGroupDescriptions[]? | select(.PropertyGroupId == \"aws\") | .PropertyMap] | add // {}" <<<"$REF_JSON")"

KAFKA_BROKERS="${KAFKA_BROKERS:-$(ref_prop KAFKA_BROKERS)}"
TELEMETRY_EVENT_TOPIC="${TELEMETRY_EVENT_TOPIC:-$(ref_prop TELEMETRY_EVENT_TOPIC)}"
TELEMETRY_EVENT_TOPIC="${TELEMETRY_EVENT_TOPIC:-$TELEMETRY_EVENT_TOPIC_DEFAULT}"
DORIS_FENODES="${DORIS_FENODES:-$(ref_prop DORIS_FENODES)}"
DORIS_BENODES="${DORIS_BENODES:-$(ref_prop DORIS_BENODES)}"
REF_SECRET="$(ref_prop DORIS_SECRET_ID)"
REF_GROUP_ID="$(ref_prop TELEMETRY_ODS_GROUP_ID)"

[[ -n "$ROLE_ARN" && "$ROLE_ARN" != null ]] || die "参考应用没有服务角色"
[[ "$BUCKET_ARN" == arn:aws:s3:::* ]]       || die "参考应用的代码不在 S3：$BUCKET_ARN"
[[ -n "$KAFKA_BROKERS" ]]  || die "参考应用里没找到 KAFKA_BROKERS，用 KAFKA_BROKERS=... 指定"
[[ -n "$DORIS_FENODES" ]]  || die "参考应用里没找到 DORIS_FENODES，用 DORIS_FENODES=... 指定"
[[ "$ZYMIX_ODS_GROUP_ID" != "$REF_GROUP_ID" ]] \
  || die "消费组 $ZYMIX_ODS_GROUP_ID 和线上作业相同，会抢分区，换一个"
[[ "$SUBNETS" != "[]" ]] || warn "参考应用没有 VPC 配置，新应用也不配 VPC（连不到内网的 MSK / VeloDB 时要补）"

if [[ -n "$S3_BUCKET" ]]; then
  # 用自己的桶
  BUCKET="${S3_BUCKET#s3://}"; BUCKET="${BUCKET%%/*}"
  BUCKET_ARN="arn:aws:s3:::${BUCKET}"
  S3_PREFIX="${S3_PREFIX%/}"
  NEW_KEY="${S3_PREFIX:+$S3_PREFIX/}zymix-flink-${TS}.jar"
  BUCKET_REGION="$("${AWS[@]}" s3api get-bucket-location --bucket "$BUCKET" --query LocationConstraint --output text 2>&1)" \
    || die "访问不了桶 $BUCKET：$BUCKET_REGION"
  [[ "$BUCKET_REGION" == None || "$BUCKET_REGION" == null ]] && BUCKET_REGION=us-east-1
  [[ "$BUCKET_REGION" == "$REGION" ]] \
    || die "桶 $BUCKET 在 $BUCKET_REGION，托管 Flink 在 $REGION，代码桶必须在同一区域"
  warn "jar 放在你自己的桶 $BUCKET。服务角色 $ROLE_ARN 必须能读它，否则应用创建 / 启动时报 S3 权限错误。
        需要的权限（加到服务角色上，或桶策略里允许该角色）：
          {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:GetObjectVersion\"],\"Resource\":\"arn:aws:s3:::${BUCKET}/${NEW_KEY%/*}/*\"}"
else
  # 默认：和参考应用放同一个桶（服务角色已能读）
  BUCKET="${BUCKET_ARN#arn:aws:s3:::}"
  KEY_DIR="$(dirname "$REF_KEY")"; [[ "$KEY_DIR" == "." ]] && KEY_DIR=""
  NEW_KEY="${KEY_DIR:+$KEY_DIR/}zymix-flink/zymix-flink-${TS}.jar"
fi

# 日志：在参考应用的日志组里建一个以新应用命名的日志流（服务角色通常已有该组的写权限）
LOG_STREAM_ARN=""
if [[ -n "$REF_LOG_STREAM_ARN" ]]; then
  LOG_GROUP_ARN="${REF_LOG_STREAM_ARN%%:log-stream:*}"
  LOG_GROUP="${LOG_GROUP_ARN#*:log-group:}"
  LOG_STREAM_ARN="${LOG_GROUP_ARN}:log-stream:${APP_NAME}"
else
  warn "参考应用没有配置 CloudWatch 日志，新应用也不配"
fi

# ---------------------------------------------------------------------------
# 2. 新应用的属性
# ---------------------------------------------------------------------------
JOB_GROUP="$(jq -nc \
  --arg sql "$SQL_FILE" --arg brokers "$KAFKA_BROKERS" --arg topic "$TELEMETRY_EVENT_TOPIC" \
  --arg gid "$ZYMIX_ODS_GROUP_ID" --arg mode "$ZYMIX_ODS_STARTUP_MODE" --arg label "$ZYMIX_ODS_LABEL_PREFIX" \
  --arg fe "$DORIS_FENODES" --arg be "$DORIS_BENODES" --arg secret "$SECRET_ARN" '
  {SQL_FILE: $sql, KAFKA_IAM: "true", KAFKA_BROKERS: $brokers, TELEMETRY_EVENT_TOPIC: $topic,
   ZYMIX_ODS_GROUP_ID: $gid, ZYMIX_ODS_STARTUP_MODE: $mode, ZYMIX_ODS_LABEL_PREFIX: $label,
   DORIS_FENODES: $fe, DORIS_SECRET_ID: $secret}
  + (if $be == "" then {} else {DORIS_BENODES: $be} end)')"
AWS_GROUP="$(jq -c --arg s "$APP_NAME" '. + {ROLE_SESSION_NAME: $s}' <<<"$REF_AWS_GROUP")"
PROPERTY_GROUPS="$(jq -nc --argjson job "$JOB_GROUP" --argjson aws "$AWS_GROUP" '
  [{PropertyGroupId: "job", PropertyMap: $job}]
  + (if ($aws | length) > 0 then [{PropertyGroupId: "aws", PropertyMap: $aws}] else [] end)')"

EXISTING_JSON="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$APP_NAME" 2>/dev/null || true)"
if [[ -n "$EXISTING_JSON" ]]; then
  ACTION="更新"
  APP_STATUS="$(jq -r '.ApplicationDetail.ApplicationStatus' <<<"$EXISTING_JSON")"
else
  ACTION="创建"
  APP_STATUS="（新建）"
fi

# ---------------------------------------------------------------------------
# 3. 打印计划并确认
# ---------------------------------------------------------------------------
cat <<EOF

================ 部署计划 ================
动作            : ${ACTION}应用 ${APP_NAME}（当前状态：${APP_STATUS}）
运行时          : FLINK-1_20
服务角色        : ${ROLE_ARN}
jar             : ${JAR_PATH} → s3://${BUCKET}/${NEW_KEY}
VPC 子网 / 安全组: ${SUBNETS} / ${SGS}
日志流          : ${LOG_STREAM_ARN:-（无）}
并行度          : ${PARALLELISM}（每 KPU ${PARALLELISM_PER_KPU}，自动扩缩容 ${AUTO_SCALING}）
启动            : $([[ "$START" == 1 ]] && echo 是 || echo "否（START=1 才启动）")
属性组（Doris 密码不在这里，运行时从 Secrets Manager 读）:
$(jq . <<<"$PROPERTY_GROUPS")
==========================================
EOF

# secret 可以写名称也可以写 ARN（ARN 末尾带 6 位随机后缀），统一成名称再比较
secret_name() { local s="${1#*:secret:}"; [[ "$1" == arn:* ]] && s="${s%-??????}"; echo "$s"; }
if [[ -n "$REF_SECRET" && "$(secret_name "$REF_SECRET")" == "$(secret_name "$SECRET_ARN")" ]]; then
  log "线上应用 $REF_APP 用的也是这个 secret，数据角色已有读取权限"
elif [[ -n "$REF_SECRET" ]]; then
  warn "线上应用用的 secret 是 $REF_SECRET，和这里的 $SECRET_ARN 不同。
        读 secret 用的是数据角色 $(jq -r '.DATA_ROLE_ARN // "（无，用服务角色）"' <<<"$AWS_GROUP")，确认它有该 secret 的 secretsmanager:GetSecretValue 权限。"
fi

if [[ "$YES" != 1 ]]; then
  read -r -p "确认执行？输入 yes 继续：" ans
  [[ "$ans" == yes ]] || die "已取消，没有做任何改动"
fi

# ---------------------------------------------------------------------------
# 4. 上传 jar
# ---------------------------------------------------------------------------
log "上传 jar 到 s3://${BUCKET}/${NEW_KEY}"
"${AWS[@]}" s3 cp "$JAR_PATH" "s3://${BUCKET}/${NEW_KEY}" --only-show-errors

# ---------------------------------------------------------------------------
# 5. 日志流
# ---------------------------------------------------------------------------
if [[ -n "$LOG_STREAM_ARN" ]]; then
  log "日志流 ${LOG_GROUP} / ${APP_NAME}"
  if ! out="$("${AWS[@]}" logs create-log-stream --log-group-name "$LOG_GROUP" --log-stream-name "$APP_NAME" 2>&1)"; then
    grep -q ResourceAlreadyExistsException <<<"$out" || die "建日志流失败：$out"
  fi
fi

# ---------------------------------------------------------------------------
# 6. 创建或更新应用
# ---------------------------------------------------------------------------
if [[ "$ACTION" == "创建" ]]; then
  log "创建应用 $APP_NAME"
  CREATE_JSON="$(jq -nc \
    --arg name "$APP_NAME" --arg role "$ROLE_ARN" --arg bucket "$BUCKET_ARN" --arg key "$NEW_KEY" \
    --argjson groups "$PROPERTY_GROUPS" --argjson subnets "$SUBNETS" --argjson sgs "$SGS" \
    --argjson par "$PARALLELISM" --argjson ppk "$PARALLELISM_PER_KPU" --argjson auto "$AUTO_SCALING" \
    --arg logarn "$LOG_STREAM_ARN" '
    {
      ApplicationName: $name,
      ApplicationDescription: "Kafka telemetry.events -> zymix_ods.app_events (zymix_flink)",
      RuntimeEnvironment: "FLINK-1_20",
      ServiceExecutionRole: $role,
      ApplicationConfiguration: ({
        ApplicationCodeConfiguration: {
          CodeContent: {S3ContentLocation: {BucketARN: $bucket, FileKey: $key}},
          CodeContentType: "ZIPFILE"
        },
        EnvironmentProperties: {PropertyGroups: $groups},
        FlinkApplicationConfiguration: {
          CheckpointConfiguration: {ConfigurationType: "DEFAULT"},
          MonitoringConfiguration: {ConfigurationType: "CUSTOM", MetricsLevel: "APPLICATION", LogLevel: "INFO"},
          ParallelismConfiguration: {ConfigurationType: "CUSTOM", Parallelism: $par,
                                     ParallelismPerKPU: $ppk, AutoScalingEnabled: $auto}
        },
        ApplicationSnapshotConfiguration: {SnapshotsEnabled: true}
      } + (if ($subnets | length) > 0
           then {VpcConfigurations: [{SubnetIds: $subnets, SecurityGroupIds: $sgs}]} else {} end))
    } + (if $logarn == "" then {} else {CloudWatchLoggingOptions: [{LogStreamARN: $logarn}]} end)')"
  "${AWS[@]}" kinesisanalyticsv2 create-application --cli-input-json "$CREATE_JSON" \
    --query 'ApplicationDetail.[ApplicationARN, ApplicationStatus]' --output text
else
  log "更新应用 $APP_NAME（代码 + 属性）"
  VERSION="$(jq -r '.ApplicationDetail.ApplicationVersionId' <<<"$EXISTING_JSON")"
  UPDATE_JSON="$(jq -nc --arg bucket "$BUCKET_ARN" --arg key "$NEW_KEY" --argjson groups "$PROPERTY_GROUPS" '
    {
      ApplicationCodeConfigurationUpdate: {CodeContentUpdate: {S3ContentLocationUpdate:
        {BucketARNUpdate: $bucket, FileKeyUpdate: $key}}},
      EnvironmentPropertyUpdates: {PropertyGroups: $groups}
    }')"
  "${AWS[@]}" kinesisanalyticsv2 update-application --application-name "$APP_NAME" \
    --current-application-version-id "$VERSION" --application-configuration-update "$UPDATE_JSON" \
    --query 'ApplicationDetail.[ApplicationARN, ApplicationStatus, ApplicationVersionId]' --output text
fi

# ---------------------------------------------------------------------------
# 7. 启动（可选）
# ---------------------------------------------------------------------------
wait_status() {  # 等到不是 *ING 的中间状态
  local s
  for _ in $(seq 1 60); do
    s="$("${AWS[@]}" kinesisanalyticsv2 describe-application --application-name "$APP_NAME" \
          --query 'ApplicationDetail.ApplicationStatus' --output text)"
    [[ "$s" == *ING ]] || { echo "$s"; return; }
    sleep 10
  done
  echo "$s"
}

STATUS="$(wait_status)"
log "应用状态：$STATUS"
if [[ "$START" == 1 ]]; then
  if [[ "$STATUS" == READY ]]; then
    log "启动应用"
    "${AWS[@]}" kinesisanalyticsv2 start-application --application-name "$APP_NAME"
    STATUS="$(wait_status)"
    log "应用状态：$STATUS"
  else
    log "应用不是 READY（$STATUS），不执行启动；运行中的应用更新后会自动按新配置重启"
  fi
fi

cat <<EOF

完成。
- 控制台：https://${REGION}.console.aws.amazon.com/flink/home?region=${REGION}#/applications/${APP_NAME}
- 手动启动：aws --region ${REGION} kinesisanalyticsv2 start-application --application-name ${APP_NAME}
- 看日志：aws --region ${REGION} logs tail '${LOG_GROUP:-<日志组>}' --log-stream-names ${APP_NAME} --follow
- 启动后检查：日志里有「Doris 账号已从 Secrets Manager 读取」「作业已提交」，
  且 zymix_ods.app_events 有数据：SELECT event_date, COUNT(*) FROM zymix_ods.app_events GROUP BY 1 ORDER BY 1 DESC LIMIT 5;
- 告警：用 zymix_data/monitoring/create_flink_alarms.sh 给 ${APP_NAME} 配 down / restarts / checkpoint 告警
EOF
