/*
 * SPDX-License-Identifier: MIT
 */
package io.github.gibson9583.sqs;

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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
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
            if (queueUrl == null || queueUrl.isBlank()) {
                throw new IllegalArgumentException(
                        "SQS Queue URL is empty after variable substitution");
            }

            SendMessageRequest.Builder requestBuilder = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(props.getTemplate());

            String delaySeconds = props.getDelaySeconds();
            if (delaySeconds != null && !delaySeconds.isBlank()) {
                requestBuilder.delaySeconds(parseIntProperty(delaySeconds, "Delay Seconds", 0, 900));
            }

            String messageGroupId = props.getMessageGroupId();
            if (messageGroupId != null && !messageGroupId.isBlank()) {
                requestBuilder.messageGroupId(messageGroupId.trim());
            }

            String messageDeduplicationId = props.getMessageDeduplicationId();
            if (messageDeduplicationId != null && !messageDeduplicationId.isBlank()) {
                requestBuilder.messageDeduplicationId(messageDeduplicationId.trim());
            }

            SendMessageResponse sendResponse = sqsClient.sendMessage(requestBuilder.build());

            responseData = sendResponse.messageId();
            responseStatus = Status.SENT;
            responseStatusMessage = "Message sent to SQS. MessageId: " + sendResponse.messageId();
            if (sendResponse.sequenceNumber() != null) {
                responseStatusMessage += ", SequenceNumber: " + sendResponse.sequenceNumber();
            }

            logger.debug("Sent message {} to SQS queue {}", sendResponse.messageId(), queueUrl);

        } catch (Exception e) {
            String errorMessage = e instanceof SqsException
                    ? ((SqsException) e).awsErrorDetails().errorMessage()
                    : e.getMessage();

            logger.error("Error sending message to SQS queue: {}", errorMessage, e);

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

    /**
     * Parses a string to int with range validation.
     * Provides a descriptive error if the value is not numeric (e.g. an
     * unresolved Velocity expression like "${sqs.delay}").
     */
    private int parseIntProperty(String value, String propertyName, int min, int max) {
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
                            + "If using a replacement variable like ${configMap.key}, ensure it resolves to a number.");
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
                .credentialsProvider(awsCredentials.getProvider());

        if (resolvedRegion != null && !resolvedRegion.isBlank()) {
            builder.region(Region.of(resolvedRegion));
        }

        return builder.build();
    }
}
