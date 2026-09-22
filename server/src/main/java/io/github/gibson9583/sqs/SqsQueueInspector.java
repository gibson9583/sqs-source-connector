/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ConnectionTestResponse;
import com.mirth.connect.util.ConnectionTestResponse.Type;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sts.model.StsException;

/** Inspects only queue metadata; never receives, sends, deletes, or changes messages. */
public class SqsQueueInspector {
    private final TemplateValueReplacer replacer;
    private static final Map<String, String> ATTRIBUTES = new LinkedHashMap<>();
    static {
        ATTRIBUTES.put("QueueArn", "Queue ARN");
        ATTRIBUTES.put("FifoQueue", "FIFO queue");
        ATTRIBUTES.put("ContentBasedDeduplication", "Content-based deduplication");
        ATTRIBUTES.put("DelaySeconds", "Default delivery delay (seconds)");
        ATTRIBUTES.put("VisibilityTimeout", "Default visibility timeout (seconds)");
        ATTRIBUTES.put("ReceiveMessageWaitTimeSeconds", "Default receive wait (seconds)");
        ATTRIBUTES.put("MaximumMessageSize", "Maximum message size (bytes)");
        ATTRIBUTES.put("MessageRetentionPeriod", "Message retention (seconds)");
        ATTRIBUTES.put("ApproximateNumberOfMessages", "Approximate available messages");
        ATTRIBUTES.put("ApproximateNumberOfMessagesNotVisible", "Approximate in-flight messages");
        ATTRIBUTES.put("ApproximateNumberOfMessagesDelayed", "Approximate delayed messages");
        ATTRIBUTES.put("RedrivePolicy", "Dead-letter policy");
        ATTRIBUTES.put("KmsMasterKeyId", "KMS key");
    }

    public SqsQueueInspector() { this(new TemplateValueReplacer()); }

    SqsQueueInspector(TemplateValueReplacer replacer) { this.replacer = replacer; }

    public ConnectionTestResponse inspect(String channelId, String channelName, ConnectorProperties properties) {
        List<String> secrets = new ArrayList<>();
        AwsConnectorCredentials.AuthType authType = null;
        try {
            Settings settings = settings(properties);
            authType = settings.authType;
            secrets.add(settings.accessKeyId);
            secrets.add(settings.secretAccessKey);
            secrets.add(settings.externalId);
            String queueUrl = resolve(settings.queueUrl, channelId, channelName).trim();
            String regionName = resolve(settings.region, channelId, channelName).trim();
            String accessKey = resolveSecret(settings.accessKeyId, channelId, channelName, secrets);
            String secretKey = resolveSecret(settings.secretAccessKey, channelId, channelName, secrets);
            String roleArn = resolve(settings.roleArn, channelId, channelName).trim();
            String externalId = resolveSecret(settings.externalId, channelId, channelName, secrets);
            validateQueueUrl(queueUrl);
            if (settings.authType == AwsConnectorCredentials.AuthType.STATIC
                    && (accessKey.isBlank() || secretKey.isBlank())) {
                throw new IllegalArgumentException("Static authentication requires an access key and secret key.");
            }
            if (settings.authType == AwsConnectorCredentials.AuthType.ROLE && roleArn.isBlank()) {
                throw new IllegalArgumentException("Assume Role authentication requires a role ARN.");
            }
            Region region = regionName.isEmpty() ? defaultRegion() : Region.of(regionName);
            try (AwsConnectorCredentials credentials = createCredentials(settings.authType,
                    accessKey, secretKey, roleArn, externalId, region.id());
                    SqsClient client = buildClient(region, credentials)) {
                Map<String, String> attributes = client.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl).attributeNames(QueueAttributeName.ALL).build()).attributesAsStrings();
                StringBuilder message = new StringBuilder("Queue inspection succeeded (sqs:GetQueueAttributes).\n")
                        .append("Queue URL: ").append(queueUrl).append("\nRegion: ").append(region.id());
                for (Map.Entry<String, String> attribute : ATTRIBUTES.entrySet()) {
                    String value = attributes.get(attribute.getKey());
                    if (value != null) {
                        message.append('\n').append(attribute.getValue()).append(": ").append(value);
                    }
                }
                message.append("\n\nMessage counts are approximate. Receive, delete, send, and KMS access were not tested.");
                return new ConnectionTestResponse(Type.SUCCESS, redact(message.toString(), secrets));
            }
        } catch (Exception e) {
            String message = "Queue inspection failed: " + errorDescription(e)
                    + failureGuidance(e, authType)
                    + "\nNo messages were received, sent, or deleted.";
            Type type = e instanceof ApiCallTimeoutException || e instanceof ApiCallAttemptTimeoutException
                    ? Type.TIME_OUT : Type.FAILURE;
            return new ConnectionTestResponse(type, redact(message, secrets));
        }
    }

    protected Region defaultRegion() { return DefaultAwsRegionProviderChain.builder().build().getRegion(); }

    protected AwsConnectorCredentials createCredentials(AwsConnectorCredentials.AuthType authType,
            String key, String secret, String role, String externalId, String region) {
        return AwsConnectorCredentials.create(authType, key, secret, role, externalId, region);
    }

    protected SqsClient buildClient(Region region, AwsConnectorCredentials credentials) {
        return SqsClient.builder().region(region).credentialsProvider(credentials.getProvider())
                .overrideConfiguration(AwsClientConfiguration.inspection()).build();
    }

    private String resolve(String value, String channelId, String channelName) {
        if (value == null || value.isEmpty()) return "";
        String resolved = replacer.replaceValues(value, channelId, channelName == null ? "" : channelName);
        return resolved == null ? "" : resolved;
    }

    private String resolveSecret(String value, String channelId, String channelName, List<String> secrets) {
        String resolved = resolve(value, channelId, channelName);
        secrets.add(resolved);
        return resolved;
    }

    static void validateQueueUrl(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Queue URL is required.");
        try {
            URI uri = URI.create(value);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                    || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                    || uri.getPath() == null || uri.getPath().equals("/") || uri.getPath().isEmpty()) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Queue URL must resolve to a complete HTTPS SQS queue URL. "
                    + "Use configuration-map keys such as ${queueUrl}; per-message variables need a concrete URL for inspection.");
        }
    }

    private static String errorDescription(Exception e) {
        if (e instanceof AwsServiceException) {
            AwsServiceException aws = (AwsServiceException) e;
            String code = aws.awsErrorDetails() == null ? null : aws.awsErrorDetails().errorCode();
            String detail = aws.awsErrorDetails() == null ? null : aws.awsErrorDetails().errorMessage();
            return (code == null ? "AWS service error" : code) + " (HTTP " + aws.statusCode() + ")"
                    + (detail == null ? "" : ": " + detail)
                    + (aws.requestId() == null ? "" : " [request " + aws.requestId() + "]");
        }
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    private static String failureGuidance(Exception e, AwsConnectorCredentials.AuthType authType) {
        String mode = authType == null ? "" : "\nAuthentication mode: " + authType + ".";
        if (!(e instanceof AwsServiceException)) return mode;
        AwsServiceException aws = (AwsServiceException) e;
        String code = aws.awsErrorDetails() == null ? null : aws.awsErrorDetails().errorCode();
        if (code == null) return mode;
        switch (code) {
            case "InvalidClientTokenId":
            case "UnrecognizedClientException":
            case "InvalidAccessKeyId":
            case "ExpiredToken":
            case "ExpiredTokenException":
            case "InvalidToken":
            case "InvalidIdentityToken":
            case "SignatureDoesNotMatch":
            case "InvalidSignatureException":
            case "MissingAuthenticationToken":
                return mode + "\nAWS rejected the signing credentials. Verify the credential values and their validity."
                        + credentialGuidance(authType);
            case "AccessDenied":
            case "AccessDeniedException":
            case "UnauthorizedOperation":
            case "NotAuthorized":
                if (e instanceof StsException) {
                    return mode + "\nSTS denied the role request. Check sts:AssumeRole permission for the OIE server's "
                            + "base identity, the target role's trust policy, and any required external ID."
                            + "\nThe base credentials come from the OIE server's default provider chain.";
                }
                return mode + "\nAWS denied queue access. Check sqs:GetQueueAttributes permission on the configured queue "
                        + "and any applicable queue, endpoint, or organization policy.";
            default:
                return mode;
        }
    }

    private static String credentialGuidance(AwsConnectorCredentials.AuthType authType) {
        if (authType == AwsConnectorCredentials.AuthType.STATIC) {
            return "\nAccess Key / Secret Key uses the values in this connector. Verify that the key and secret belong together."
                    + " Temporary AWS credentials also require a session token; use Default Provider Chain with all three values.";
        }
        if (authType == AwsConnectorCredentials.AuthType.ROLE) {
            return "\nAssume IAM Role uses the OIE server's default provider chain as the base identity for STS."
                    + " Verify those base credentials and their session token when temporary, then refresh expired role credentials.";
        }
        return "\nDefault Provider Chain loads credentials in the OIE server process. Verify that process's credential source,"
                + " include the session token for temporary credentials, and refresh expired credentials.";
    }

    static String redact(String message, List<String> secrets) {
        for (String secret : secrets) {
            if (secret != null && !secret.isEmpty()) message = message.replace(secret, "[redacted]");
        }
        return message;
    }

    private static Settings settings(ConnectorProperties properties) {
        if (properties instanceof SqsReceiverProperties) {
            SqsReceiverProperties p = (SqsReceiverProperties) properties;
            return new Settings(p.getQueueUrl(), p.getRegion(), p.getAuthType().name(),
                    p.getAccessKeyId(), p.getSecretAccessKey(), p.getRoleArn(), p.getExternalId());
        }
        if (properties instanceof SqsDispatcherProperties) {
            SqsDispatcherProperties p = (SqsDispatcherProperties) properties;
            return new Settings(p.getQueueUrl(), p.getRegion(), p.getAuthType().name(),
                    p.getAccessKeyId(), p.getSecretAccessKey(), p.getRoleArn(), p.getExternalId());
        }
        throw new IllegalArgumentException("SQS Reader or SQS Sender settings are required.");
    }

    private static class Settings {
        final String queueUrl, region, accessKeyId, secretAccessKey, roleArn, externalId;
        final AwsConnectorCredentials.AuthType authType;
        Settings(String queueUrl, String region, String authType, String accessKeyId,
                String secretAccessKey, String roleArn, String externalId) {
            this.queueUrl = queueUrl;
            this.region = region;
            this.authType = AwsConnectorCredentials.AuthType.valueOf(authType);
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
            this.roleArn = roleArn;
            this.externalId = externalId;
        }
    }
}
