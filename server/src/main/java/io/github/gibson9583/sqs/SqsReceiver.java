/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.gibson9583.sqs;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mirth.connect.donkey.model.event.ConnectionStatusEventType;
import com.mirth.connect.donkey.model.event.ErrorEventType;
import com.mirth.connect.donkey.model.message.BatchRawMessage;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.ChannelException;
import com.mirth.connect.donkey.server.channel.PollConnector;
import com.mirth.connect.donkey.server.event.ConnectionStatusEvent;
import com.mirth.connect.donkey.server.event.ErrorEvent;
import com.mirth.connect.donkey.server.message.batch.BatchMessageReader;
import com.mirth.connect.donkey.server.message.batch.ResponseHandler;
import com.mirth.connect.server.controllers.EventController;
import com.mirth.connect.server.util.TemplateValueReplacer;

import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties.S3EventMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * OIE source connector that polls an AWS SQS queue for messages.
 * <p>
 * Extends {@link PollConnector} to use OIE's built-in polling scheduler
 * (interval, time-of-day, and cron). Supports Standard and FIFO queues,
 * long polling, configurable visibility timeout, and all AWS auth methods.
 * <p>
 * All String properties are resolved through OIE's {@link TemplateValueReplacer}
 * at start time so that Velocity expressions like {@code ${queueUrl}} and
 * {@code ${waitTime}} are substituted from Configuration Map and global/channel maps.
 * <p>
 * Messages are deleted from SQS after successful dispatch to the channel.
 * On failure, messages remain in the queue and reappear after the visibility
 * timeout expires.
 */
public class SqsReceiver extends PollConnector {

    private static final Logger logger = LogManager.getLogger(SqsReceiver.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Set<String> OBJECT_REMOVAL_EVENTS = Set.of(
            "ObjectRemoved:Delete", "ObjectRemoved:DeleteMarkerCreated",
            "LifecycleExpiration:Delete", "LifecycleExpiration:DeleteMarkerCreated");

    private final TemplateValueReplacer replacer = new TemplateValueReplacer();
    private EventController eventController;

    private SqsClient sqsClient;
    private S3Client s3Client;
    private AwsConnectorCredentials awsCredentials;
    private SqsReceiverProperties connectorProperties;

    // Resolved values (after Velocity substitution)
    private String resolvedQueueUrl;
    private String resolvedRegion;
    private String resolvedAccessKeyId;
    private String resolvedSecretAccessKey;
    private String resolvedRoleArn;
    private String resolvedExternalId;
    private int resolvedWaitTimeSeconds;
    private int resolvedMaxMessages;
    private int resolvedVisibilityTimeout;
    private long resolvedS3MaxObjectSizeBytes;
    private boolean resolvedS3BinaryMode;
    private String resolvedS3Encoding;

    // =========================================================================
    // Lifecycle
    // =========================================================================

    @Override
    public void onDeploy() throws ConnectorTaskException {
        eventController = EventController.getInstance();
        connectorProperties = (SqsReceiverProperties) getConnectorProperties();

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
        connectorProperties = (SqsReceiverProperties) getConnectorProperties();

        try {
            // Resolve all Velocity/replacement variables
            resolveProperties();

            if (connectorProperties.isMessageGroupHandling()
                    && (!connectorProperties.getSourceConnectorProperties().isRespondAfterProcessing()
                    || connectorProperties.getSourceConnectorProperties().getProcessingThreads() != 1)) {
                throw new ConnectorTaskException("FIFO source-order handling requires Source Queue OFF "
                        + "and exactly one processing thread. Destination queue ordering is managed separately.");
            }
            if (resolvedS3BinaryMode && connectorProperties.getSourceConnectorProperties().isProcessBatch()) {
                throw new ConnectorTaskException("Process Batch requires text content; select Text for S3 Fetch Object or disable Process Batch.");
            }

            awsCredentials = AwsConnectorCredentials.create(
                    AwsConnectorCredentials.AuthType.valueOf(connectorProperties.getAuthType().name()),
                    resolvedAccessKeyId, resolvedSecretAccessKey,
                    resolvedRoleArn, resolvedExternalId, resolvedRegion);

            sqsClient = buildSqsClient();

            // Build S3 client only when fetching objects
            S3EventMode s3Mode = connectorProperties.getS3EventMode();
            if (s3Mode == S3EventMode.FETCH_OBJECT) {
                s3Client = buildS3Client();
            }
            if (s3Mode != null && s3Mode != S3EventMode.DISABLED) {
                logger.info("S3 event mode enabled: {}", s3Mode);
            }

            // Verify connectivity by requesting queue attributes
            sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(resolvedQueueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build());

            logger.info("SQS Receiver started. Queue: {}, Region: {}, Auth: {}, LongPoll: {}s, MaxMsg: {}, VisTmout: {}s",
                    resolvedQueueUrl,
                    resolvedRegion,
                    connectorProperties.getAuthType(),
                    resolvedWaitTimeSeconds,
                    resolvedMaxMessages,
                    resolvedVisibilityTimeout);

        } catch (SqsException e) {
            closeClients();
            throw new ConnectorTaskException(
                    "Failed to connect to SQS queue: " + e.getMessage(), e);
        } catch (ConnectorTaskException e) {
            closeClients();
            throw e;
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
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (Exception e) {
                logger.warn("Error closing S3 client", e);
            } finally {
                s3Client = null;
            }
        }
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

    @Override
    public void handleRecoveredResponse(DispatchResult dispatchResult) {
        finishDispatch(dispatchResult);
    }

    // =========================================================================
    // Velocity / Replacement Variable Resolution
    // =========================================================================

    /**
     * Resolves all String properties through OIE's TemplateValueReplacer.
     * This handles top-level map keys such as ${queueUrl} and other Velocity
     * expressions that users may have entered in the connector settings.
     * <p>
     * Numeric properties are parsed to int after substitution. If a value
     * cannot be parsed (e.g. unresolved variable), a clear error is thrown.
     */
    private void resolveProperties() throws ConnectorTaskException {
        String channelId = getChannelId();
        String channelName = channel != null ? channel.getName() : "";

        // Reset S3 fetch fields to defaults
        resolvedS3MaxObjectSizeBytes = 0;
        resolvedS3BinaryMode = false;
        resolvedS3Encoding = null;

        resolvedQueueUrl = replacer.replaceValues(connectorProperties.getQueueUrl(), channelId, channelName);
        resolvedRegion = replacer.replaceValues(connectorProperties.getRegion(), channelId, channelName);
        resolvedAccessKeyId = replacer.replaceValues(connectorProperties.getAccessKeyId(), channelId, channelName);
        resolvedSecretAccessKey = replacer.replaceValues(connectorProperties.getSecretAccessKey(), channelId, channelName);
        resolvedRoleArn = replacer.replaceValues(connectorProperties.getRoleArn(), channelId, channelName);
        resolvedExternalId = replacer.replaceValues(connectorProperties.getExternalId(), channelId, channelName);

        resolvedWaitTimeSeconds = parseIntProperty(
                replacer.replaceValues(connectorProperties.getWaitTimeSeconds(), channelId, channelName),
                "Wait Time Seconds", 0, 20);
        resolvedMaxMessages = parseIntProperty(
                replacer.replaceValues(connectorProperties.getMaxMessages(), channelId, channelName),
                "Max Messages", 1, 10);
        resolvedVisibilityTimeout = parseIntProperty(
                replacer.replaceValues(connectorProperties.getVisibilityTimeout(), channelId, channelName),
                "Visibility Timeout", 0, 43200);

        // Resolve S3 max object size if fetch mode is enabled
        S3EventMode s3Mode = connectorProperties.getS3EventMode();
        if (s3Mode == S3EventMode.FETCH_OBJECT) {
            String maxSizeStr = replacer.replaceValues(
                    connectorProperties.getS3MaxObjectSizeKB(), channelId, channelName);
            if (maxSizeStr == null || maxSizeStr.isBlank()) {
                resolvedS3MaxObjectSizeBytes = 0; // no limit
            } else {
                try {
                    long kb = Long.parseLong(maxSizeStr.trim());
                    if (kb < 0) {
                        throw new ConnectorTaskException(
                                "S3 Max Object Size (KB) cannot be negative: " + kb);
                    }
                    resolvedS3MaxObjectSizeBytes = Math.multiplyExact(kb, 1024L);
                } catch (ArithmeticException e) {
                    throw new ConnectorTaskException("S3 Max Object Size (KB) is too large: " + maxSizeStr, e);
                } catch (NumberFormatException e) {
                    throw new ConnectorTaskException(
                            "S3 Max Object Size (KB) is not a valid integer: '" + maxSizeStr + "'. "
                                    + "If using a replacement variable, ensure it resolves to a number.");
                }
            }

            // Resolve file type and encoding (supports Velocity substitution)
            String fileType = replacer.replaceValues(
                    connectorProperties.getS3FileType(), channelId, channelName);
            resolvedS3BinaryMode = "Binary".equalsIgnoreCase(fileType != null ? fileType.trim() : "Text");
            resolvedS3Encoding = replacer.replaceValues(
                    connectorProperties.getS3Encoding(), channelId, channelName);
        }

        // Validate required fields after substitution
        if (resolvedQueueUrl == null || resolvedQueueUrl.isBlank()) {
            throw new ConnectorTaskException("SQS Queue URL is empty after variable substitution. "
                    + "Original value: " + connectorProperties.getQueueUrl());
        }
    }

    /**
     * Parses a string to int with range validation.
     * Provides a descriptive error if the value is not numeric (e.g. an
     * unresolved Velocity expression like "${sqs.waitTime}").
     */
    private int parseIntProperty(String value, String propertyName, int min, int max)
            throws ConnectorTaskException {
        if (value == null || value.isBlank()) {
            throw new ConnectorTaskException(propertyName + " is empty after variable substitution");
        }

        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < min || parsed > max) {
                throw new ConnectorTaskException(
                        propertyName + " value " + parsed + " is out of range [" + min + ", " + max + "]");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new ConnectorTaskException(
                    propertyName + " is not a valid integer: '" + value + "'. "
                            + "If using a replacement variable like ${waitTime}, ensure it resolves to a number.");
        }
    }

    // =========================================================================
    // Polling
    // =========================================================================

    @Override
    protected void poll() throws InterruptedException {
        eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                getMetaDataId(), getSourceName(), ConnectionStatusEventType.POLLING));
        try {
            String queuePath = URI.create(resolvedQueueUrl).getPath();
            boolean fifo = queuePath != null && queuePath.replaceFirst("/+$", "").endsWith(".fifo");
            boolean moreMessages = true;
            while (moreMessages && !isTerminated() && !Thread.currentThread().isInterrupted()) {
                ReceiveMessageRequest.Builder requestBuilder = ReceiveMessageRequest.builder()
                        .queueUrl(resolvedQueueUrl)
                        .maxNumberOfMessages(resolvedMaxMessages)
                        .waitTimeSeconds(resolvedWaitTimeSeconds)
                        .visibilityTimeout(resolvedVisibilityTimeout)
                        .attributeNamesWithStrings("All");
                if (connectorProperties.isIncludeAttributes()) {
                    requestBuilder.messageAttributeNames("All");
                }
                List<Message> messages = sqsClient.receiveMessage(requestBuilder.build()).messages();
                if (messages.isEmpty()) {
                    break;
                }
                // Do not process successors already delivered with a failed group member.
                // A later receive can retry that group's head after visibility expires.
                Set<String> failedGroups = new HashSet<>();
                for (Message message : messages) {
                    if (isTerminated() || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    String group = fifo ? message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID) : null;
                    if (group != null && failedGroups.contains(group)) {
                        continue;
                    }
                    if (!processMessage(message) && group != null) {
                        failedGroups.add(group);
                    }
                }
                moreMessages = messages.size() >= resolvedMaxMessages;
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            reportError("Error receiving messages from SQS queue " + resolvedQueueUrl, e);
        } finally {
            eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                    getMetaDataId(), getSourceName(), ConnectionStatusEventType.IDLE));
        }
    }

    private boolean processMessage(Message message) {
        try {
            S3EventMode mode = connectorProperties.getS3EventMode();
            boolean accepted = mode == null || mode == S3EventMode.DISABLED
                    ? processStandardMessage(message) : processS3EventMessage(message, mode);
            return accepted && deleteMessage(message);
        } catch (Exception e) {
            reportError("Error processing SQS message " + message.messageId(), e);
            return false;
        }
    }

    private boolean processStandardMessage(Message message) throws Exception {
        RawMessage rawMessage = new RawMessage(message.body());
        rawMessage.setSourceMap(buildBaseSourceMap(message));
        return dispatchIncoming(rawMessage);
    }

    /** Admission includes durable source-queue acceptance; it does not require destination success. */
    private boolean dispatchIncoming(RawMessage rawMessage) throws Exception {
        if (isProcessBatch()) {
            if (Boolean.TRUE.equals(rawMessage.isBinary())) {
                throw new IllegalArgumentException("Process Batch requires text content");
            }
            try (StringReader reader = new StringReader(rawMessage.getRawData())) {
                Boolean messagesExist = dispatchBatchMessage(
                        new BatchRawMessage(new BatchMessageReader(reader), rawMessage.getSourceMap()),
                        new ResponseHandler() {
                            @Override
                            public void responseProcess(int sequence, boolean complete) throws Exception {
                                requireAccepted(getDispatchResult());
                            }
                            @Override
                            public void responseError(ChannelException e) {
                                // dispatchBatchMessage propagates this failure to the caller.
                            }
                        });
                if (!Boolean.TRUE.equals(messagesExist)) {
                    throw new IllegalStateException("Batch was not accepted or produced no messages; retaining the SQS receipt");
                }
                return true;
            }
        }
        DispatchResult result = null;
        try {
            result = dispatchRawMessage(rawMessage);
            requireAccepted(result);
            return true;
        } finally {
            // Complete engine bookkeeping before acknowledging the external receipt.
            finishDispatch(result);
        }
    }

    private static void requireAccepted(DispatchResult result) throws Exception {
        if (result == null) {
            throw new IllegalStateException("Channel did not accept the message");
        }
        if (result.getChannelException() != null) {
            throw result.getChannelException();
        }
    }

    private boolean processS3EventMessage(Message message, S3EventMode mode) throws Exception {
        String body = message.body();
        JsonNode root;
        try {
            root = objectMapper.readTree(body);
            if (root != null && "Notification".equals(root.path("Type").asText())) {
                String innerMessage = root.path("Message").asText();
                if (!innerMessage.isEmpty()) {
                    root = objectMapper.readTree(innerMessage);
                }
            }
        } catch (JsonProcessingException e) {
            // A mixed queue may contain ordinary non-JSON messages.
            return processStandardMessage(message);
        }
        if (root != null && isEventBridgeS3Event(root)) {
            return processEventBridgeS3(root, message, body, mode);
        }
        if (root != null && isStandardS3Records(root.path("Records"))) {
            return processStandardS3Records(root.path("Records"), message, body, mode);
        }
        return processStandardMessage(message);
    }

    private boolean isStandardS3Records(JsonNode records) {
        if (!records.isArray() || records.size() == 0) {
            return false;
        }
        for (JsonNode record : records) {
            if (!"aws:s3".equals(record.path("eventSource").asText())
                    || !record.path("s3").path("bucket").path("name").isTextual()
                    || !record.path("s3").path("object").path("key").isTextual()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Detects EventBridge S3 event format.
     * EventBridge events have "source": "aws.s3" and a "detail" object containing bucket/object info.
     */
    private boolean isEventBridgeS3Event(JsonNode root) {
        return "aws.s3".equals(root.path("source").asText(null))
                && root.has("detail")
                && root.path("detail").has("bucket");
    }

    /**
     * Processes an EventBridge-format S3 event notification.
     * Format: { "source": "aws.s3", "detail-type": "Object Created", "detail": { "bucket": {...}, "object": {...} } }
     */
    private boolean processEventBridgeS3(JsonNode root, Message message, String body, S3EventMode s3Mode) throws Exception {
        Map<String, Object> sourceMap = buildBaseSourceMap(message);

        JsonNode detail = root.path("detail");
        JsonNode bucketNode = detail.path("bucket");
        JsonNode objectNode = detail.path("object");

        // detail-type maps to event name (e.g. "Object Created")
        String detailType = root.path("detail-type").asText(null);
        String reason = detail.path("reason").asText(null);
        String eventName = detailType;
        if (reason != null && !reason.isEmpty()) {
            eventName = (detailType != null ? detailType + ":" : "") + reason;
        }

        String awsRegion = root.path("region").asText(null);
        String bucketName = bucketNode.path("name").asText(null);

        // Bucket ARN from the resources array
        String bucketArn = null;
        JsonNode resources = root.path("resources");
        if (resources.isArray()) {
            for (JsonNode res : resources) {
                String arn = res.asText(null);
                if (arn != null && arn.startsWith("arn:aws:s3")) {
                    bucketArn = arn;
                    break;
                }
            }
        }

        String objectKey = objectNode.path("key").asText(null);
        long objectSize = objectNode.path("size").asLong(-1);
        // EventBridge uses lowercase "etag" and hyphenated "version-id"
        String objectETag = objectNode.path("etag").asText(null);
        String objectVersionId = objectNode.path("version-id").asText(null);

        // Populate source map
        if (eventName != null) sourceMap.put("s3EventName", eventName);
        if (bucketName != null) sourceMap.put("s3BucketName", bucketName);
        if (bucketArn != null) sourceMap.put("s3BucketArn", bucketArn);
        if (objectKey != null) sourceMap.put("s3ObjectKey", objectKey);
        if (objectSize >= 0) sourceMap.put("s3ObjectSize", objectSize);
        if (objectETag != null) sourceMap.put("s3ObjectETag", objectETag);
        if (objectVersionId != null) sourceMap.put("s3ObjectVersionId", objectVersionId);
        if (awsRegion != null) sourceMap.put("s3Region", awsRegion);
        sourceMap.put("s3EventFormat", "EventBridge");

        RawMessage rawMessage;

        if (s3Mode == S3EventMode.FETCH_OBJECT && "Object Deleted".equals(detailType)) {
            sourceMap.put("s3FetchStatus", "NOT_APPLICABLE");
            rawMessage = new RawMessage(body);
        } else if (s3Mode == S3EventMode.FETCH_OBJECT && bucketName != null && objectKey != null) {
            RawMessage fetched = fetchS3Object(bucketName, objectKey, sourceMap);
            rawMessage = fetched != null ? fetched : new RawMessage(body);
        } else {
            rawMessage = new RawMessage(body);
        }

        rawMessage.setSourceMap(sourceMap);
        return dispatchIncoming(rawMessage);
    }

    /**
     * Processes standard S3 notification format with Records[] array.
     * Format: { "Records": [{ "eventName": "...", "s3": { "bucket": {...}, "object": {...} } }] }
     */
    private boolean processStandardS3Records(JsonNode records, Message message, String body, S3EventMode s3Mode) throws Exception {
        for (int i = 0; i < records.size(); i++) {
            if (isTerminated() || Thread.currentThread().isInterrupted()) {
                return false;
            }
            JsonNode record = records.get(i);
            Map<String, Object> sourceMap = buildBaseSourceMap(message);

            // Extract S3 event details
            String eventName = record.path("eventName").asText(null);
            String awsRegion = record.path("awsRegion").asText(null);
            JsonNode s3Node = record.path("s3");
            JsonNode bucketNode = s3Node.path("bucket");
            JsonNode objectNode = s3Node.path("object");

            String bucketName = bucketNode.path("name").asText(null);
            String bucketArn = bucketNode.path("arn").asText(null);
            String objectKey = objectNode.path("key").asText(null);
            if (objectKey != null) {
                objectKey = URLDecoder.decode(objectKey, StandardCharsets.UTF_8);
            }
            long objectSize = objectNode.path("size").asLong(-1);
            String objectETag = objectNode.path("eTag").asText(null);
            String objectVersionId = objectNode.path("versionId").asText(null);

            // Populate source map with S3 details
            if (eventName != null) sourceMap.put("s3EventName", eventName);
            if (bucketName != null) sourceMap.put("s3BucketName", bucketName);
            if (bucketArn != null) sourceMap.put("s3BucketArn", bucketArn);
            if (objectKey != null) sourceMap.put("s3ObjectKey", objectKey);
            if (objectSize >= 0) sourceMap.put("s3ObjectSize", objectSize);
            if (objectETag != null) sourceMap.put("s3ObjectETag", objectETag);
            if (objectVersionId != null) sourceMap.put("s3ObjectVersionId", objectVersionId);
            if (awsRegion != null) sourceMap.put("s3Region", awsRegion);
            sourceMap.put("s3EventFormat", "S3Notification");

            if (records.size() > 1) {
                sourceMap.put("s3RecordIndex", i);
                sourceMap.put("s3RecordCount", records.size());
            }

            RawMessage rawMessage;

            if (s3Mode == S3EventMode.FETCH_OBJECT && eventName != null && OBJECT_REMOVAL_EVENTS.contains(eventName)) {
                sourceMap.put("s3FetchStatus", "NOT_APPLICABLE");
                rawMessage = new RawMessage(body);
            } else if (s3Mode == S3EventMode.FETCH_OBJECT && bucketName != null && objectKey != null) {
                RawMessage fetched = fetchS3Object(bucketName, objectKey, sourceMap);
                rawMessage = fetched != null ? fetched : new RawMessage(body);
            } else {
                rawMessage = new RawMessage(body);
            }

            rawMessage.setSourceMap(sourceMap);
            if (!dispatchIncoming(rawMessage)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fetches the event's version (or conditionally its ETag). The actual response is bounded,
     * including absent/incorrect Content-Length; no stale event size or separate HEAD is trusted.
     * Only an explicit size-limit outcome returns null. API/read failures propagate for retry.
     */
    private RawMessage fetchS3Object(String bucket, String key, Map<String, Object> sourceMap) throws Exception {
        GetObjectRequest.Builder request = GetObjectRequest.builder().bucket(bucket).key(key);
        String versionId = (String) sourceMap.get("s3ObjectVersionId");
        String eTag = (String) sourceMap.get("s3ObjectETag");
        if (versionId != null && !versionId.isBlank()) {
            request.versionId(versionId);
        }
        if (eTag != null && !eTag.isBlank()) {
            request.ifMatch(eTag.startsWith("\"") ? eTag : "\"" + eTag + "\"");
        }
        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(request.build())) {
            try {
                GetObjectResponse response = stream.response();
                if (resolvedS3MaxObjectSizeBytes > 0 && response.contentLength() != null
                        && response.contentLength() > resolvedS3MaxObjectSizeBytes) {
                    return oversizedObject(stream, sourceMap, bucket, key);
                }
                ByteArrayOutputStream content = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                long total = 0;
                while (true) {
                    if (isTerminated() || Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("S3 fetch interrupted or source connector stopping");
                    }
                    int requested = resolvedS3MaxObjectSizeBytes > 0
                            ? (int) Math.min(buffer.length, resolvedS3MaxObjectSizeBytes - total + 1)
                            : buffer.length;
                    int count = stream.read(buffer, 0, requested);
                    if (count == -1) break;
                    total += count;
                    if (resolvedS3MaxObjectSizeBytes > 0 && total > resolvedS3MaxObjectSizeBytes) {
                        return oversizedObject(stream, sourceMap, bucket, key);
                    }
                    content.write(buffer, 0, count);
                }
                byte[] bytes = content.toByteArray();
                // Standard object metadata
                if (response.contentType() != null) {
                    sourceMap.put("s3ContentType", response.contentType());
                }
                if (response.contentLength() != null) {
                    sourceMap.put("s3ContentLength", response.contentLength());
                }
                if (response.contentEncoding() != null) {
                    sourceMap.put("s3ContentEncoding", response.contentEncoding());
                }
                if (response.lastModified() != null) {
                    sourceMap.put("s3LastModified", response.lastModified().toString());
                }
                if (response.eTag() != null) {
                    sourceMap.putIfAbsent("s3ObjectETag", response.eTag());
                    sourceMap.put("s3FetchedObjectETag", response.eTag());
                }
                if (response.versionId() != null) {
                    sourceMap.putIfAbsent("s3ObjectVersionId", response.versionId());
                    sourceMap.put("s3FetchedObjectVersionId", response.versionId());
                }
                if (response.storageClassAsString() != null) {
                    sourceMap.put("s3StorageClass", response.storageClassAsString());
                }
                if (response.serverSideEncryptionAsString() != null) {
                    sourceMap.put("s3ServerSideEncryption", response.serverSideEncryptionAsString());
                }
                if (response.cacheControl() != null) {
                    sourceMap.put("s3CacheControl", response.cacheControl());
                }
                if (response.contentDisposition() != null) {
                    sourceMap.put("s3ContentDisposition", response.contentDisposition());
                }

                // User-defined metadata (x-amz-meta-* headers)
                if (response.hasMetadata()) {
                    for (Map.Entry<String, String> entry : response.metadata().entrySet()) {
                        sourceMap.put(entry.getKey(), entry.getValue());
                    }
                }

                sourceMap.put("s3FetchStatus", "FETCHED");
                return resolvedS3BinaryMode ? new RawMessage(bytes)
                        : new RawMessage(new String(bytes, resolveEncoding(response.contentType())));
            } catch (Exception | Error e) {
                // Closing an Apache response can drain its remainder. Abort failures instead.
                stream.abort();
                throw e;
            }
        }
    }

    private RawMessage oversizedObject(ResponseInputStream<GetObjectResponse> stream,
            Map<String, Object> sourceMap, String bucket, String key) {
        stream.abort();
        sourceMap.put("s3FetchStatus", "OVERSIZED");
        sourceMap.put("s3FetchLimitBytes", resolvedS3MaxObjectSizeBytes);
        logger.warn("S3 object s3://{}/{} exceeds max limit ({} bytes); passing original event JSON",
                bucket, key, resolvedS3MaxObjectSizeBytes);
        return null;
    }

    /**
     * Resolves the charset to use for decoding fetched S3 object content.
     * Always tries the Content-Type header first; if no charset is present
     * there, falls back to the user-configured encoding.
     */
    private Charset resolveEncoding(String contentType) {
        if (contentType != null) {
            try {
                Charset charset = org.apache.http.entity.ContentType.parse(contentType).getCharset();
                if (charset != null) return charset;
            } catch (RuntimeException e) {
                logger.warn("Unknown charset or invalid Content-Type '{}', falling back to configured encoding", contentType);
            }
        }

        // Fall back to configured encoding
        String encoding = resolvedS3Encoding;
        if (encoding == null || encoding.isEmpty() || "DEFAULT_ENCODING".equals(encoding)) {
            return Charset.defaultCharset();
        }
        try {
            return Charset.forName(encoding);
        } catch (Exception e) {
            logger.warn("Unknown configured encoding '{}', falling back to JVM default", encoding);
            return Charset.defaultCharset();
        }
    }

    /**
     * Builds the base source map with SQS metadata common to all processing modes.
     */
    private Map<String, Object> buildBaseSourceMap(Message message) {
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("sqsMessageId", message.messageId());
        sourceMap.put("sqsReceiptHandle", message.receiptHandle());
        sourceMap.put("sqsMD5OfBody", message.md5OfBody());

        // System attributes (sent timestamp, sender ID, etc.)
        if (message.hasAttributes()) {
            for (Map.Entry<MessageSystemAttributeName, String> entry : message.attributes().entrySet()) {
                sourceMap.put("sqsAttr" + entry.getKey().toString(), entry.getValue());
            }

            // FIFO-specific attributes
            String messageGroupId = message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
            if (messageGroupId != null) {
                sourceMap.put("sqsMessageGroupId", messageGroupId);
            }
            String sequenceNumber = message.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);
            if (sequenceNumber != null) {
                sourceMap.put("sqsSequenceNumber", sequenceNumber);
            }
        }

        // User-defined message attributes
        if (connectorProperties.isIncludeAttributes() && message.hasMessageAttributes()) {
            Map<String, String> attributeTypes = new HashMap<>();
            message.messageAttributes().forEach((key, attr) -> {
                attributeTypes.put(key, attr.dataType());
                sourceMap.put("sqsMsgAttr" + key, attr.binaryValue() != null
                        ? Base64.getEncoder().encodeToString(attr.binaryValue().asByteArray())
                        : attr.stringValue());
            });
            sourceMap.put("sqsMessageAttributeTypes", attributeTypes);
        }

        return sourceMap;
    }

    private static final int DELETE_MAX_RETRIES = 3;
    private static final long DELETE_RETRY_DELAY_MS = 1000;

    private boolean deleteMessage(Message message) {
        for (int attempt = 1; attempt <= DELETE_MAX_RETRIES; attempt++) {
            try {
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(resolvedQueueUrl).receiptHandle(message.receiptHandle()).build());
                return true;
            } catch (Exception e) {
                boolean retryable = e instanceof SdkClientException
                        || (e instanceof SqsException && (((SqsException) e).statusCode() >= 500
                        || ((SqsException) e).statusCode() == 429
                        || ((SqsException) e).isThrottlingException()));
                if (!retryable || attempt == DELETE_MAX_RETRIES || isTerminated()) {
                    reportError("Failed to delete SQS message " + message.messageId()
                            + "; it may be redelivered. Queue: " + resolvedQueueUrl, e);
                    return false;
                }
                try {
                    Thread.sleep(DELETE_RETRY_DELAY_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    reportError("Interrupted deleting SQS message " + message.messageId(), interrupted);
                    return false;
                }
            }
        }
        return false;
    }

    private void reportError(String description, Throwable error) {
        logger.error(description, error);
        eventController.dispatchEvent(new ErrorEvent(getChannelId(), getMetaDataId(), null,
                ErrorEventType.SOURCE_CONNECTOR, getSourceName(), connectorProperties.getName(), description, error));
    }

    // =========================================================================
    // AWS Client Builders
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

    /**
     * Builds the S3Client using the same credentials as the SQS client.
     */
    private S3Client buildS3Client() {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(awsCredentials.getProvider())
                .overrideConfiguration(AwsClientConfiguration.standard());

        if (resolvedRegion != null && !resolvedRegion.isBlank()) {
            builder.region(Region.of(resolvedRegion));
        }

        return builder.build();
    }
}
