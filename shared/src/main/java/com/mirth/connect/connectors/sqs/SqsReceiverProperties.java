/*
 * SPDX-License-Identifier: MIT
 */
package com.mirth.connect.connectors.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.PollConnectorProperties;
import com.mirth.connect.donkey.model.channel.PollConnectorPropertiesInterface;
import com.mirth.connect.donkey.model.channel.SourceConnectorProperties;
import com.mirth.connect.donkey.model.channel.SourceConnectorPropertiesInterface;
import com.mirth.connect.donkey.util.DonkeyElement;

/**
 * Configuration properties for the SQS source connector.
 * <p>
 * All fields that a user might want to templatize with OIE replacement
 * variables are stored as {@code String} so that Velocity expressions like
 * {@code ${sqs.queueUrl}} or {@code ${sqs.waitTime}} survive XStream
 * serialization and are resolved at deploy/start time by the OIE engine.
 * <p>
 * The server-side {@code SqsReceiver} is responsible for parsing numeric
 * strings to {@code int} after Velocity substitution has occurred.
 */
public class SqsReceiverProperties extends ConnectorProperties
        implements PollConnectorPropertiesInterface, SourceConnectorPropertiesInterface {

    // --- Auth type enum ---
    public enum AuthType {
        /** Use AWS default credential provider chain (env vars, instance profile, etc.) */
        DEFAULT,
        /** Use explicit access key and secret key */
        STATIC,
        /** Assume an IAM role via STS */
        ROLE
    }

    // --- S3 event handling enum ---
    public enum S3EventMode {
        /** Treat SQS body as-is (default) */
        DISABLED,
        /** Parse S3 event JSON, add bucket/key/event details to source map, keep original SQS body */
        EXTRACT_DETAILS,
        /** Parse S3 event JSON, add details to source map, AND replace message body with fetched S3 object content */
        FETCH_OBJECT
    }

    // --- OIE built-in properties ---
    private PollConnectorProperties pollConnectorProperties;
    private SourceConnectorProperties sourceConnectorProperties;

    // --- AWS connection settings (all String for Velocity substitution) ---
    private String queueUrl;
    private String region;
    private AuthType authType;
    private String accessKeyId;
    private String secretAccessKey;
    private String roleArn;
    private String externalId;

    // --- SQS polling settings (String for Velocity substitution) ---
    private String waitTimeSeconds;
    private String maxMessages;
    private String visibilityTimeout;

    // --- Message handling ---
    private boolean includeAttributes;
    private boolean messageGroupHandling;

    // --- S3 event notification settings ---
    private S3EventMode s3EventMode;
    private String s3MaxObjectSizeKB;
    private String s3FileType;
    private String s3Encoding;

    public SqsReceiverProperties() {
        pollConnectorProperties = new PollConnectorProperties();
        sourceConnectorProperties = new SourceConnectorProperties();

        // Defaults
        queueUrl = "";
        region = "";
        authType = AuthType.DEFAULT;
        accessKeyId = "";
        secretAccessKey = "";
        roleArn = "";
        externalId = "";
        waitTimeSeconds = "20";
        maxMessages = "10";
        visibilityTimeout = "30";
        includeAttributes = true;
        messageGroupHandling = false;
        s3EventMode = S3EventMode.DISABLED;
        s3MaxObjectSizeKB = "10240";
        s3FileType = "Text";
        s3Encoding = "DEFAULT_ENCODING";
    }

    // =========================================================================
    // ConnectorProperties overrides
    // =========================================================================

    @Override
    public String getProtocol() {
        return "SQS";
    }

    @Override
    public String getName() {
        return "SQS Reader";
    }

    @Override
    public String toFormattedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QUEUE URL: ").append(queueUrl).append('\n');
        sb.append("REGION: ").append(region).append('\n');
        sb.append("AUTH TYPE: ").append(authType).append('\n');
        sb.append("LONG POLL WAIT: ").append(waitTimeSeconds).append("s\n");
        sb.append("MAX MESSAGES: ").append(maxMessages).append('\n');
        sb.append("VISIBILITY TIMEOUT: ").append(visibilityTimeout).append("s\n");
        if (messageGroupHandling) {
            sb.append("FIFO MESSAGE GROUP HANDLING: enabled\n");
        }
        if (s3EventMode != null && s3EventMode != S3EventMode.DISABLED) {
            sb.append("S3 EVENT MODE: ").append(s3EventMode).append('\n');
            if (s3EventMode == S3EventMode.FETCH_OBJECT) {
                sb.append("S3 MAX OBJECT SIZE (KB): ").append(s3MaxObjectSizeKB).append('\n');
                sb.append("S3 FILE TYPE: ").append(getS3FileType()).append('\n');
                if ("Text".equals(getS3FileType())) {
                    sb.append("S3 ENCODING: ").append(getS3Encoding()).append('\n');
                }
            }
        }
        return sb.toString();
    }

    @Override
    public Map<String, Object> getPurgedProperties() {
        Map<String, Object> purged = new HashMap<>();
        purged.put("pollConnectorProperties", pollConnectorProperties.getPurgedProperties());
        purged.put("sourceConnectorProperties", sourceConnectorProperties.getPurgedProperties());
        purged.put("region", region);
        purged.put("authType", authType.name());
        purged.put("waitTimeSeconds", waitTimeSeconds);
        purged.put("maxMessages", maxMessages);
        purged.put("visibilityTimeout", visibilityTimeout);
        purged.put("includeAttributes", includeAttributes);
        purged.put("messageGroupHandling", messageGroupHandling);
        purged.put("s3EventMode", s3EventMode != null ? s3EventMode.name() : S3EventMode.DISABLED.name());
        purged.put("s3MaxObjectSizeKB", s3MaxObjectSizeKB);
        purged.put("s3FileType", getS3FileType());
        purged.put("s3Encoding", getS3Encoding());
        return purged;
    }

    // =========================================================================
    // PollConnectorPropertiesInterface
    // =========================================================================

    @Override
    public PollConnectorProperties getPollConnectorProperties() {
        return pollConnectorProperties;
    }

    public void setPollConnectorProperties(PollConnectorProperties pollConnectorProperties) {
        this.pollConnectorProperties = pollConnectorProperties;
    }

    // =========================================================================
    // SourceConnectorPropertiesInterface
    // =========================================================================

    @Override
    public SourceConnectorProperties getSourceConnectorProperties() {
        return sourceConnectorProperties;
    }

    public void setSourceConnectorProperties(SourceConnectorProperties sourceConnectorProperties) {
        this.sourceConnectorProperties = sourceConnectorProperties;
    }

    @Override
    public boolean canBatch() {
        return true;
    }

    // =========================================================================
    // Migratable (required abstract methods not provided by ConnectorProperties)
    // =========================================================================

    @Override
    public void migrate3_0_1(DonkeyElement element) {}

    @Override
    public void migrate3_0_2(DonkeyElement element) {}

    // =========================================================================
    // Getters and Setters
    // =========================================================================

    public String getQueueUrl() {
        return queueUrl;
    }

    public void setQueueUrl(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public AuthType getAuthType() {
        return authType != null ? authType : AuthType.DEFAULT;
    }

    public void setAuthType(AuthType authType) {
        this.authType = authType;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public void setRoleArn(String roleArn) {
        this.roleArn = roleArn;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getWaitTimeSeconds() {
        return waitTimeSeconds;
    }

    public void setWaitTimeSeconds(String waitTimeSeconds) {
        this.waitTimeSeconds = waitTimeSeconds;
    }

    public String getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(String maxMessages) {
        this.maxMessages = maxMessages;
    }

    public String getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(String visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public boolean isIncludeAttributes() {
        return includeAttributes;
    }

    public void setIncludeAttributes(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    public boolean isMessageGroupHandling() {
        return messageGroupHandling;
    }

    public void setMessageGroupHandling(boolean messageGroupHandling) {
        this.messageGroupHandling = messageGroupHandling;
    }

    public S3EventMode getS3EventMode() {
        return s3EventMode != null ? s3EventMode : S3EventMode.DISABLED;
    }

    public void setS3EventMode(S3EventMode s3EventMode) {
        this.s3EventMode = s3EventMode;
    }

    public String getS3MaxObjectSizeKB() {
        return s3MaxObjectSizeKB != null ? s3MaxObjectSizeKB : "10240";
    }

    public void setS3MaxObjectSizeKB(String s3MaxObjectSizeKB) {
        this.s3MaxObjectSizeKB = s3MaxObjectSizeKB;
    }

    public String getS3FileType() {
        return s3FileType != null ? s3FileType : "Text";
    }

    public void setS3FileType(String s3FileType) {
        this.s3FileType = s3FileType;
    }

    public String getS3Encoding() {
        return s3Encoding != null ? s3Encoding : "DEFAULT_ENCODING";
    }

    public void setS3Encoding(String s3Encoding) {
        this.s3Encoding = s3Encoding;
    }

    // =========================================================================
    // equals / hashCode / clone
    // =========================================================================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqsReceiverProperties that = (SqsReceiverProperties) o;
        return includeAttributes == that.includeAttributes
                && messageGroupHandling == that.messageGroupHandling
                && Objects.equals(queueUrl, that.queueUrl)
                && Objects.equals(region, that.region)
                && authType == that.authType
                && Objects.equals(accessKeyId, that.accessKeyId)
                && Objects.equals(secretAccessKey, that.secretAccessKey)
                && Objects.equals(roleArn, that.roleArn)
                && Objects.equals(externalId, that.externalId)
                && Objects.equals(waitTimeSeconds, that.waitTimeSeconds)
                && Objects.equals(maxMessages, that.maxMessages)
                && Objects.equals(visibilityTimeout, that.visibilityTimeout)
                && Objects.equals(s3EventMode, that.s3EventMode)
                && Objects.equals(s3MaxObjectSizeKB, that.s3MaxObjectSizeKB)
                && Objects.equals(s3FileType, that.s3FileType)
                && Objects.equals(s3Encoding, that.s3Encoding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueUrl, region, authType, accessKeyId, secretAccessKey,
                roleArn, externalId, waitTimeSeconds, maxMessages, visibilityTimeout,
                includeAttributes, messageGroupHandling, s3EventMode, s3MaxObjectSizeKB,
                s3FileType, s3Encoding);
    }

    public SqsReceiverProperties cloneProperties() {
        SqsReceiverProperties props = new SqsReceiverProperties();
        props.pollConnectorProperties = new PollConnectorProperties(pollConnectorProperties);
        props.sourceConnectorProperties = new SourceConnectorProperties();
        props.queueUrl = queueUrl;
        props.region = region;
        props.authType = authType;
        props.accessKeyId = accessKeyId;
        props.secretAccessKey = secretAccessKey;
        props.roleArn = roleArn;
        props.externalId = externalId;
        props.waitTimeSeconds = waitTimeSeconds;
        props.maxMessages = maxMessages;
        props.visibilityTimeout = visibilityTimeout;
        props.includeAttributes = includeAttributes;
        props.messageGroupHandling = messageGroupHandling;
        props.s3EventMode = s3EventMode;
        props.s3MaxObjectSizeKB = s3MaxObjectSizeKB;
        props.s3FileType = s3FileType;
        props.s3Encoding = s3Encoding;
        return props;
    }
}
