/*
 * SPDX-License-Identifier: MPL-2.0
 */
package com.mirth.connect.connectors.sqs;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.donkey.model.channel.DestinationConnectorPropertiesInterface;
import com.mirth.connect.donkey.util.DonkeyElement;

/**
 * Configuration properties for the SQS destination connector.
 * <p>
 * All fields that a user might want to templatize with OIE replacement
 * variables are stored as {@code String} so that Velocity expressions like
 * {@code ${sqs.queueUrl}} or {@code ${message.encodedData}} survive XStream
 * serialization and are resolved per message by the OIE engine.
 * <p>
 * The server-side {@code SqsDispatcher} is responsible for parsing numeric
 * strings to {@code int} after Velocity substitution has occurred.
 */
public class SqsDispatcherProperties extends ConnectorProperties
        implements DestinationConnectorPropertiesInterface {

    // --- Auth type enum ---
    public enum AuthType {
        /** Use AWS default credential provider chain (env vars, instance profile, etc.) */
        DEFAULT,
        /** Use explicit access key and secret key */
        STATIC,
        /** Assume an IAM role via STS */
        ROLE
    }

    // --- OIE built-in properties ---
    private DestinationConnectorProperties destinationConnectorProperties;

    // --- AWS connection settings (all String for Velocity substitution) ---
    private String queueUrl;
    private String region;
    private AuthType authType;
    private String accessKeyId;
    private String secretAccessKey;
    private String roleArn;
    private String externalId;

    // --- Message settings (String for Velocity substitution) ---
    private String template;
    private String delaySeconds;
    private String messageGroupId;
    private String messageDeduplicationId;
    private List<SqsMessageAttribute> messageAttributes;

    public SqsDispatcherProperties() {
        destinationConnectorProperties = new DestinationConnectorProperties(false);

        // Defaults
        queueUrl = "";
        region = "";
        authType = AuthType.DEFAULT;
        accessKeyId = "";
        secretAccessKey = "";
        roleArn = "";
        externalId = "";
        template = "${message.encodedData}";
        delaySeconds = "";
        messageGroupId = "";
        messageDeduplicationId = "";
        messageAttributes = new ArrayList<>();
    }

    public SqsDispatcherProperties(SqsDispatcherProperties props) {
        super(props);
        destinationConnectorProperties = new DestinationConnectorProperties(props.getDestinationConnectorProperties());

        queueUrl = props.getQueueUrl();
        region = props.getRegion();
        authType = props.getAuthType();
        accessKeyId = props.getAccessKeyId();
        secretAccessKey = props.getSecretAccessKey();
        roleArn = props.getRoleArn();
        externalId = props.getExternalId();
        template = props.getTemplate();
        delaySeconds = props.getDelaySeconds();
        messageGroupId = props.getMessageGroupId();
        messageDeduplicationId = props.getMessageDeduplicationId();
        messageAttributes = new ArrayList<>();
        for (SqsMessageAttribute attribute : props.getMessageAttributes()) {
            messageAttributes.add(attribute == null ? null : new SqsMessageAttribute(attribute));
        }
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
        return "SQS Sender";
    }

    @Override
    public String toFormattedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QUEUE URL: ").append(queueUrl).append('\n');
        sb.append("REGION: ").append(region).append('\n');
        sb.append("AUTH TYPE: ").append(getAuthType()).append('\n');
        if (delaySeconds != null && !delaySeconds.isBlank()) {
            sb.append("DELAY: ").append(delaySeconds).append("s\n");
        }
        if (messageGroupId != null && !messageGroupId.isBlank()) {
            sb.append("MESSAGE GROUP ID: ").append(messageGroupId).append('\n');
        }
        if (messageDeduplicationId != null && !messageDeduplicationId.isBlank()) {
            sb.append("MESSAGE DEDUPLICATION ID: ").append(messageDeduplicationId).append('\n');
        }
        if (!getMessageAttributes().isEmpty()) {
            sb.append("MESSAGE ATTRIBUTES:\n");
            for (SqsMessageAttribute attribute : getMessageAttributes()) {
                if (attribute != null) {
                    sb.append(attribute.getName()).append(" (").append(attribute.getDataType())
                            .append("): ").append(attribute.getValue()).append('\n');
                }
            }
        }
        sb.append('\n').append("[CONTENT]").append('\n');
        sb.append(template).append('\n');
        return sb.toString();
    }

    @Override
    public Map<String, Object> getPurgedProperties() {
        Map<String, Object> purged = new HashMap<>();
        purged.put("destinationConnectorProperties", destinationConnectorProperties.getPurgedProperties());
        purged.put("region", region);
        purged.put("authType", getAuthType().name());
        purged.put("delaySecondsSet", delaySeconds != null && !delaySeconds.isBlank());
        purged.put("messageGroupIdSet", messageGroupId != null && !messageGroupId.isBlank());
        purged.put("messageDeduplicationIdSet", messageDeduplicationId != null && !messageDeduplicationId.isBlank());
        purged.put("templateLines", countLines(template));
        purged.put("messageAttributeCount", getMessageAttributes().size());
        return purged;
    }

    private static int countLines(String str) {
        return str == null ? 0 : str.split("\r\n|\r|\n").length;
    }

    // =========================================================================
    // DestinationConnectorPropertiesInterface
    // =========================================================================

    @Override
    public DestinationConnectorProperties getDestinationConnectorProperties() {
        return destinationConnectorProperties;
    }

    public void setDestinationConnectorProperties(DestinationConnectorProperties destinationConnectorProperties) {
        this.destinationConnectorProperties = destinationConnectorProperties;
    }

    @Override
    public boolean canValidateResponse() {
        return false;
    }

    @Override
    public SqsDispatcherProperties clone() {
        return new SqsDispatcherProperties(this);
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

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public String getDelaySeconds() {
        return delaySeconds;
    }

    public void setDelaySeconds(String delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public String getMessageGroupId() {
        return messageGroupId;
    }

    public void setMessageGroupId(String messageGroupId) {
        this.messageGroupId = messageGroupId;
    }

    public String getMessageDeduplicationId() {
        return messageDeduplicationId;
    }

    public void setMessageDeduplicationId(String messageDeduplicationId) {
        this.messageDeduplicationId = messageDeduplicationId;
    }

    /** Older serialized channels have no attribute list. */
    public List<SqsMessageAttribute> getMessageAttributes() {
        if (messageAttributes == null) {
            messageAttributes = new ArrayList<>();
        }
        return messageAttributes;
    }

    public void setMessageAttributes(List<SqsMessageAttribute> messageAttributes) {
        this.messageAttributes = new ArrayList<>();
        if (messageAttributes != null) {
            for (SqsMessageAttribute attribute : messageAttributes) {
                this.messageAttributes.add(attribute == null ? null : new SqsMessageAttribute(attribute));
            }
        }
    }

    // =========================================================================
    // equals / hashCode
    // =========================================================================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqsDispatcherProperties that = (SqsDispatcherProperties) o;
        return Objects.equals(queueUrl, that.queueUrl)
                && Objects.equals(region, that.region)
                && getAuthType() == that.getAuthType()
                && Objects.equals(accessKeyId, that.accessKeyId)
                && Objects.equals(secretAccessKey, that.secretAccessKey)
                && Objects.equals(roleArn, that.roleArn)
                && Objects.equals(externalId, that.externalId)
                && Objects.equals(template, that.template)
                && Objects.equals(delaySeconds, that.delaySeconds)
                && Objects.equals(messageGroupId, that.messageGroupId)
                && Objects.equals(messageDeduplicationId, that.messageDeduplicationId)
                && Objects.equals(getMessageAttributes(), that.getMessageAttributes())
                && Objects.equals(destinationConnectorProperties, that.destinationConnectorProperties)
                && Objects.equals(getPluginProperties(), that.getPluginProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueUrl, region, getAuthType(), accessKeyId, secretAccessKey,
                roleArn, externalId, template, delaySeconds, messageGroupId,
                messageDeduplicationId, getMessageAttributes(),
                SqsPropertyHash.destination(destinationConnectorProperties),
                SqsPropertyHash.plugins(getPluginProperties()));
    }
}
