package com.zymix.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 访问数据所在 AWS 账号的资源：MSK（IAM 认证）和 Secrets Manager。
 *
 * <p>roleArn 为空时用运行环境自己的凭证（托管 Flink 的服务角色 / 本地的 AWS profile）；
 * 不为空时先扮演这个角色，用于跨账号访问。
 */
final class AwsAccess {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String roleArn;
    private final String stsRegion;
    private final String secretRegion;
    private final String sessionName;

    AwsAccess(String roleArn, String stsRegion, String secretRegion, String sessionName) {
        this.roleArn = blankToNull(roleArn);
        this.stsRegion = blankToNull(stsRegion);
        this.secretRegion = blankToNull(secretRegion);
        this.sessionName = sessionName;
    }

    /** 给 Kafka 表补的 MSK IAM 认证选项（非 shaded 连接器，所以是 properties.* 直通 kafka-clients）。 */
    Map<String, String> kafkaIamOptions() {
        StringBuilder jaas = new StringBuilder("software.amazon.msk.auth.iam.IAMLoginModule required");
        if (roleArn != null) {
            jaas.append(" awsRoleArn=\"").append(roleArn).append('"')
                    .append(" awsRoleSessionName=\"").append(sessionName).append('"');
            if (stsRegion != null) {
                jaas.append(" awsStsRegion=\"").append(stsRegion).append('"');
            }
        }
        jaas.append(';');

        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("properties.security.protocol", "SASL_SSL");
        opts.put("properties.sasl.mechanism", "AWS_MSK_IAM");
        opts.put("properties.sasl.jaas.config", jaas.toString());
        opts.put("properties.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return opts;
    }

    /** 读一个 JSON 格式的 secret（如 {"DORIS_USER": "...", "DORIS_PASSWORD": "..."}），返回扁平的 key → value。 */
    Map<String, String> readSecret(String secretId) {
        var builder = SecretsManagerClient.builder().credentialsProvider(credentials());
        if (secretRegion != null) {
            builder.region(Region.of(secretRegion));
        }
        try (SecretsManagerClient sm = builder.build()) {
            String raw = sm.getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build()).secretString();
            Map<String, Object> parsed = JSON.readValue(raw, new TypeReference<Map<String, Object>>() {});
            Map<String, String> out = new LinkedHashMap<>();
            parsed.forEach((k, v) -> out.put(k, v == null ? null : String.valueOf(v)));
            return out;
        } catch (java.io.IOException e) {
            throw new IllegalStateException("secret " + secretId + " 不是 JSON 对象", e);
        }
    }

    private AwsCredentialsProvider credentials() {
        if (roleArn == null) {
            return DefaultCredentialsProvider.create();
        }
        var sts = StsClient.builder();
        if (stsRegion != null) {
            sts.region(Region.of(stsRegion));
        }
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(sts.build())
                .refreshRequest(r -> r.roleArn(roleArn).roleSessionName(sessionName))
                .build();
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }
}
