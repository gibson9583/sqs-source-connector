/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.gibson9583.sqs;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.donkey.model.event.ConnectionStatusEventType;
import com.mirth.connect.donkey.model.event.ErrorEventType;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Response;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DestinationConnector;
import com.mirth.connect.donkey.server.event.ConnectionStatusEvent;
import com.mirth.connect.donkey.server.event.ErrorEvent;
import com.mirth.connect.server.controllers.EventController;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ErrorMessageBuilder;

import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsMessageAttribute;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * OIE destination connector that sends messages to an AWS SQS queue.
 * <p>
 * Connection-level settings (region, credentials) are resolved through OIE's
 * {@link TemplateValueReplacer} at start time. Message-level settings (queue
 * URL, body template, delay, FIFO group/deduplication IDs) are resolved per
 * message in {@link #replaceConnectorProperties}, so they support expressions
 * like {@code ${message.encodedData}} or channel map variables.
 * <p>
 * On send failure the response status is QUEUED so that OIE's destination
 * queue/retry settings apply; with queueing disabled OIE converts this to
 * ERROR automatically.
 */
public class SqsDispatcher extends DestinationConnector {

    private static final Logger logger = LogManager.getLogger(SqsDispatcher.class);
    private static final int MAX_MESSAGE_BYTES = 1024 * 1024;
    private static final Pattern ATTRIBUTE_NAME = Pattern.compile("[A-Za-z0-9_.-]{1,256}");
    private static final Pattern FIFO_ID = Pattern.compile("[\\x21-\\x7E]{1,128}");
    private static final Pattern NUMBER = Pattern.compile("[+-]?([0-9]*)(?:\\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?");
    private static final BigDecimal MIN_NUMBER = new BigDecimal("1E-128");
    private static final BigDecimal MAX_NUMBER = new BigDecimal("1E126");

    private final TemplateValueReplacer replacer = new TemplateValueReplacer();
    private EventController eventController;

    private SqsClient sqsClient;
    private AwsConnectorCredentials awsCredentials;
    private SqsDispatcherProperties connectorProperties;

    // Resolved connection-level values (after Velocity substitution at start)
    private String resolvedRegion;
    private String resolvedAccessKeyId;
    private String resolvedSecretAccessKey;
    private String resolvedRoleArn;
    private String resolvedExternalId;

    // =========================================================================
    // Lifecycle
    // =========================================================================

    @Override
    public void onDeploy() throws ConnectorTaskException {
        eventController = EventController.getInstance();
        connectorProperties = (SqsDispatcherProperties) getConnectorProperties();

        if (connectorProperties.getQueueUrl() == null || connectorProperties.getQueueUrl().isBlank()) {
            throw new ConnectorTaskException("SQS Queue URL is required");
        }
        eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                getMetaDataId(), getDestinationName(), ConnectionStatusEventType.IDLE));
    }

    @Override
    public void onUndeploy() throws ConnectorTaskException {
        // no-op
    }

    @Override
    public void onStart() throws ConnectorTaskException {
        connectorProperties = (SqsDispatcherProperties) getConnectorProperties();

        try {
            resolveConnectionProperties();

            awsCredentials = AwsConnectorCredentials.create(
                    AwsConnectorCredentials.AuthType.valueOf(connectorProperties.getAuthType().name()),
                    resolvedAccessKeyId, resolvedSecretAccessKey,
                    resolvedRoleArn, resolvedExternalId, resolvedRegion);

            sqsClient = buildSqsClient();

            logger.info("SQS Sender started. Region: {}, Auth: {}",
                    resolvedRegion, connectorProperties.getAuthType());

        } catch (Exception e) {
            closeClients();
            throw new ConnectorTaskException("Failed to initialize SQS client: " + e.getMessage(), e);
        }
    }

    @Override
    public void onStop() throws ConnectorTaskException {
        closeClients();
    }

    private void closeClients() {
        if (sqsClient != null) {
            try {
                sqsClient.close();
            } catch (Exception e) {
                logger.warn("Error closing SQS client", e);
            } finally {
                sqsClient = null;
            }
        }
        if (awsCredentials != null) {
            try {
                awsCredentials.close();
            } catch (Exception e) {
                logger.warn("Error closing AWS credentials provider", e);
            } finally {
                awsCredentials = null;
            }
        }
    }

    @Override
    public void onHalt() throws ConnectorTaskException {
        onStop();
    }

    // =========================================================================
    // Velocity / Replacement Variable Resolution
    // =========================================================================

    /**
     * Resolves connection-level properties (region, credentials) through
     * OIE's TemplateValueReplacer using channel-scoped context. These are
     * resolved once at start because the SQS client is built once.
     */
    private void resolveConnectionProperties() {
        String channelId = getChannelId();
        String channelName = getChannel() != null ? getChannel().getName() : "";

        resolvedRegion = replacer.replaceValues(connectorProperties.getRegion(), channelId, channelName);
        resolvedAccessKeyId = replacer.replaceValues(connectorProperties.getAccessKeyId(), channelId, channelName);
        resolvedSecretAccessKey = replacer.replaceValues(connectorProperties.getSecretAccessKey(), channelId, channelName);
        resolvedRoleArn = replacer.replaceValues(connectorProperties.getRoleArn(), channelId, channelName);
        resolvedExternalId = replacer.replaceValues(connectorProperties.getExternalId(), channelId, channelName);
    }

    /**
     * Resolves message-level properties against the connector message context.
     * Called by the donkey engine on a cloned copy of the properties before
     * each {@link #send}.
     */
    @Override
    public void replaceConnectorProperties(ConnectorProperties connectorProperties, ConnectorMessage connectorMessage) {
        SqsDispatcherProperties props = (SqsDispatcherProperties) connectorProperties;

        props.setQueueUrl(replacer.replaceValues(props.getQueueUrl(), connectorMessage));
        props.setTemplate(replacer.replaceValues(props.getTemplate(), connectorMessage));
        props.setDelaySeconds(replacer.replaceValues(props.getDelaySeconds(), connectorMessage));
        props.setMessageGroupId(replacer.replaceValues(props.getMessageGroupId(), connectorMessage));
        props.setMessageDeduplicationId(replacer.replaceValues(props.getMessageDeduplicationId(), connectorMessage));
        for (SqsMessageAttribute attribute : props.getMessageAttributes()) {
            if (attribute != null) {
                attribute.setName(replacer.replaceValues(attribute.getName(), connectorMessage));
                attribute.setValue(replacer.replaceValues(attribute.getValue(), connectorMessage));
            }
        }
    }

    // =========================================================================
    // Send
    // =========================================================================

    @Override
    public Response send(ConnectorProperties connectorProperties, ConnectorMessage connectorMessage) {
        SqsDispatcherProperties props = (SqsDispatcherProperties) connectorProperties;

        eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                getMetaDataId(), getDestinationName(), ConnectionStatusEventType.SENDING));

        String responseData = null;
        String responseError = null;
        String responseStatusMessage = null;
        Status responseStatus = Status.QUEUED;

        try {
            String queueUrl = props.getQueueUrl();
            SendMessageRequest request = buildSendRequest(props);

            SendMessageResponse sendResponse = sqsClient.sendMessage(request);

            responseData = sendResponse.messageId();
            responseStatus = Status.SENT;
            responseStatusMessage = "Message sent to SQS. MessageId: " + sendResponse.messageId();
            if (sendResponse.sequenceNumber() != null) {
                responseStatusMessage += ", SequenceNumber: " + sendResponse.sequenceNumber();
            }

            logger.debug("Sent message {} to SQS queue {}", sendResponse.messageId(), queueUrl);

        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (e instanceof SqsException && ((SqsException) e).awsErrorDetails() != null
                    && ((SqsException) e).awsErrorDetails().errorMessage() != null) {
                errorMessage = ((SqsException) e).awsErrorDetails().errorMessage();
            }

            if (isQueueEnabled()) {
                logger.warn("Error sending message to SQS queue: {}", errorMessage, e);
            } else {
                logger.error("Error sending message to SQS queue: {}", errorMessage, e);
            }

            responseStatusMessage = ErrorMessageBuilder.buildErrorResponse(
                    "Error sending message to SQS queue", e);
            responseError = ErrorMessageBuilder.buildErrorMessage(
                    props.getName(), "Error sending message to SQS queue: " + errorMessage, e);

            eventController.dispatchEvent(new ErrorEvent(getChannelId(), getMetaDataId(),
                    connectorMessage.getMessageId(), ErrorEventType.DESTINATION_CONNECTOR,
                    getDestinationName(), props.getName(),
                    "Error sending message to SQS queue", e));

        } finally {
            eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                    getMetaDataId(), getDestinationName(), ConnectionStatusEventType.IDLE));
        }

        return new Response(responseStatus, responseData, responseStatusMessage, responseError);
    }

    /** Validates the resolved envelope on every attempt, including persisted queue retries. */
    static SendMessageRequest buildSendRequest(SqsDispatcherProperties props) {
        String queueUrl = props.getQueueUrl();
        if (queueUrl == null || queueUrl.isBlank()) {
            throw new IllegalArgumentException("SQS Queue URL is empty after variable substitution");
        }
        String path = URI.create(queueUrl).getPath();
        boolean fifo = path != null && path.replaceFirst("/+$", "").endsWith(".fifo");
        long size = textSize(props.getTemplate(), "Message body");
        SendMessageRequest.Builder requestBuilder = SendMessageRequest.builder()
                .queueUrl(queueUrl).messageBody(props.getTemplate());

        String delaySeconds = props.getDelaySeconds();
        if (delaySeconds != null && !delaySeconds.isBlank()) {
            if (fifo) {
                throw new IllegalArgumentException("FIFO queues do not support per-message Delay Seconds; leave it blank and configure the queue delay instead");
            }
            requestBuilder.delaySeconds(parseIntProperty(delaySeconds, "Delay Seconds", 0, 900));
        }

        String group = props.getMessageGroupId();
        if (group != null && !group.isBlank()) {
            requestBuilder.messageGroupId(validateFifoId(group.trim(), "Message Group ID"));
        } else if (fifo) {
            throw new IllegalArgumentException("Message Group ID is required for FIFO queues");
        }

        String deduplication = props.getMessageDeduplicationId();
        if (deduplication != null && !deduplication.isBlank()) {
            if (!fifo) {
                throw new IllegalArgumentException("Message Deduplication ID is supported only on FIFO queues");
            }
            requestBuilder.messageDeduplicationId(validateFifoId(deduplication.trim(), "Message Deduplication ID"));
        }

        if (props.getMessageAttributes().size() > 10) {
            throw new IllegalArgumentException("SQS supports at most 10 message attributes");
        }
        Map<String, MessageAttributeValue> attributes = new LinkedHashMap<>();
        for (SqsMessageAttribute attribute : props.getMessageAttributes()) {
            if (attribute == null) {
                throw new IllegalArgumentException("Message attribute must not be null");
            }
            String name = attribute.getName();
            validateAttributeName(name);
            if (attributes.containsKey(name)) {
                throw new IllegalArgumentException("Message attribute names must be unique after variable substitution");
            }
            String type = attribute.getDataType();
            String value = attribute.getValue();
            MessageAttributeValue.Builder valueBuilder = MessageAttributeValue.builder().dataType(type);
            if ("Binary".equals(type)) {
                if (value == null || value.isEmpty()) {
                    throw new IllegalArgumentException("Binary message attribute value must not be empty");
                }
                if (value.length() > ((MAX_MESSAGE_BYTES + 2) / 3) * 4) {
                    throw new IllegalArgumentException("Binary message attribute exceeds the SQS message size limit");
                }
                byte[] decoded;
                try {
                    decoded = Base64.getDecoder().decode(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Binary message attribute must contain valid Base64", e);
                }
                size += decoded.length;
                valueBuilder.binaryValue(SdkBytes.fromByteArray(decoded));
            } else if ("String".equals(type) || "Number".equals(type)) {
                size += textSize(value, "Message attribute value");
                if ("Number".equals(type)) {
                    validateNumber(value);
                }
                valueBuilder.stringValue(value);
            } else {
                throw new IllegalArgumentException("Message attribute data type must be String, Number, or Binary");
            }
            size += name.getBytes(StandardCharsets.UTF_8).length + type.getBytes(StandardCharsets.UTF_8).length;
            attributes.put(name, valueBuilder.build());
            if (size > MAX_MESSAGE_BYTES) {
                throw new IllegalArgumentException("Message body and attributes exceed the SQS limit of 1 MiB");
            }
        }
        return requestBuilder.messageAttributes(attributes).build();
    }

    private static void validateAttributeName(String name) {
        if (name == null || !ATTRIBUTE_NAME.matcher(name).matches() || name.startsWith(".")
                || name.endsWith(".") || name.contains("..")
                || name.toLowerCase(Locale.ROOT).startsWith("aws.")
                || name.toLowerCase(Locale.ROOT).startsWith("amazon.")) {
            throw new IllegalArgumentException("Message attribute name must be 1-256 letters, digits, underscores, hyphens, or periods; reserved prefixes and leading, trailing, or consecutive periods are not allowed");
        }
    }

    private static String validateFifoId(String value, String name) {
        if (!FIFO_ID.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must contain 1-128 ASCII letters, digits, or punctuation, without spaces");
        }
        return value;
    }

    private static long textSize(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        // Valid UTF-8 requires at least as many bytes as UTF-16 code units. Reject
        // oversized inputs before scanning or allocating their encoded byte array.
        if (value.length() > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException(name + " exceeds the SQS limit of 1 MiB");
        }
        for (int offset = 0; offset < value.length();) {
            int codePoint = value.codePointAt(offset);
            if (!(codePoint == 9 || codePoint == 10 || codePoint == 13
                    || codePoint >= 0x20 && codePoint <= 0xD7FF
                    || codePoint >= 0xE000 && codePoint <= 0xFFFD
                    || codePoint >= 0x10000 && codePoint <= 0x10FFFF)) {
                throw new IllegalArgumentException(name + " contains a character not supported by SQS");
            }
            offset += Character.charCount(codePoint);
        }
        long bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException(name + " exceeds the SQS limit of 1 MiB");
        }
        return bytes;
    }

    private static void validateNumber(String value) {
        try {
            Matcher matcher = NUMBER.matcher(value);
            if (!matcher.matches()) {
                throw new NumberFormatException();
            }
            String integer = matcher.group(1);
            String fraction = matcher.group(2) == null ? "" : matcher.group(2);
            String digits = integer + fraction;
            if (digits.isEmpty()) {
                throw new NumberFormatException();
            }
            int first = 0;
            int last = digits.length() - 1;
            while (first <= last && digits.charAt(first) == '0') first++;
            while (last >= first && digits.charAt(last) == '0') last--;
            long exponent = matcher.group(3) == null ? 0 : Long.parseLong(matcher.group(3));
            if (first > last) return; // Zero has no magnitude restriction.
            int precision = last - first + 1;
            long power = Math.addExact(Math.subtractExact(exponent, fraction.length()), digits.length() - 1L - last);
            if (precision > 38 || power < -165 || power > 126) {
                throw new IllegalArgumentException("Number message attributes exceed the supported precision or magnitude");
            }
            // Parse only the bounded significant digits; large zero padding or exponents must
            // not cause enormous BigInteger allocations or decimal expansion in a queue worker.
            BigDecimal magnitude = new BigDecimal(digits.substring(first, last + 1)).scaleByPowerOfTen((int) power);
            if (magnitude.compareTo(MAX_NUMBER) > 0 || magnitude.compareTo(MIN_NUMBER) < 0) {
                throw new IllegalArgumentException("Number message attributes require at most 38 significant digits and magnitude between 1E-128 and 1E126, or zero");
            }
        } catch (NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("Number message attribute must contain a valid finite decimal number", e);
        }
    }

    /**
     * Parses a string to int with range validation.
     * Provides a descriptive error if the value is not numeric (e.g. an
     * unresolved Velocity expression like "${sqs.delay}").
     */
    private static int parseIntProperty(String value, String propertyName, int min, int max) {
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < min || parsed > max) {
                throw new IllegalArgumentException(
                        propertyName + " value " + parsed + " is out of range [" + min + ", " + max + "]");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    propertyName + " is not a valid integer: '" + value + "'. "
                            + "If using a replacement variable like ${key}, ensure it resolves to a number.");
        }
    }

    // =========================================================================
    // SQS Client Builder
    // =========================================================================

    /**
     * Builds the SqsClient using resolved (post-Velocity) property values.
     */
    private SqsClient buildSqsClient() {
        SqsClientBuilder builder = SqsClient.builder()
                .credentialsProvider(awsCredentials.getProvider())
                .overrideConfiguration(AwsClientConfiguration.standard());

        if (resolvedRegion != null && !resolvedRegion.isBlank()) {
            builder.region(Region.of(resolvedRegion));
        }

        return builder.build();
    }
}
