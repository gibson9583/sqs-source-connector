/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.cgibson.sqs;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mirth.connect.donkey.model.event.ConnectionStatusEventType;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.channel.PollConnector;
import com.mirth.connect.donkey.server.event.ConnectionStatusEvent;
import com.mirth.connect.server.controllers.EventController;
import com.mirth.connect.server.util.TemplateValueReplacer;

import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties.S3EventMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * OIE source connector that polls an AWS SQS queue for messages.
 * <p>
 * Extends {@link PollConnector} to use OIE's built-in polling scheduler
 * (interval, time-of-day, and cron). Supports Standard and FIFO queues,
 * long polling, configurable visibility timeout, and all AWS auth methods.
 * <p>
 * All String properties are resolved through OIE's {@link TemplateValueReplacer}
 * at start time so that Velocity expressions like {@code ${sqs.queueUrl}},
 * {@code ${sqs.waitTime}}, {@code ${AWS_ACCESS_KEY}} etc. are substituted
 * from Configuration Map, global/channel maps, and environment variables.
 * <p>
 * Messages are deleted from SQS after successful dispatch to the channel.
 * On failure, messages remain in the queue and reappear after the visibility
 * timeout expires.
 */
public class SqsReceiver extends PollConnector {

    private static final Logger logger = LogManager.getLogger(SqsReceiver.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final TemplateValueReplacer replacer = new TemplateValueReplacer();
    private EventController eventController;

    private SqsClient sqsClient;
    private S3Client s3Client;
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
            throw new ConnectorTaskException(
                    "Failed to connect to SQS queue: " + e.awsErrorDetails().errorMessage(), e);
        } catch (ConnectorTaskException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectorTaskException("Failed to initialize SQS client: " + e.getMessage(), e);
        }
    }

    @Override
    public void onStop() throws ConnectorTaskException {
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
     * This handles ${configMap.key}, ${globalMap.key}, and other Velocity
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
                    resolvedS3MaxObjectSizeBytes = kb == 0 ? 0 : kb * 1024;
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
                            + "If using a replacement variable like ${configMap.key}, ensure it resolves to a number.");
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
            boolean moreMessages = true;

            while (moreMessages && !isTerminated()) {
                ReceiveMessageRequest.Builder requestBuilder = ReceiveMessageRequest.builder()
                        .queueUrl(resolvedQueueUrl)
                        .maxNumberOfMessages(resolvedMaxMessages)
                        .waitTimeSeconds(resolvedWaitTimeSeconds)
                        .visibilityTimeout(resolvedVisibilityTimeout)
                        .attributeNamesWithStrings("All");

                // Request all user-defined message attributes
                if (connectorProperties.isIncludeAttributes()) {
                    requestBuilder.messageAttributeNames("All");
                }

                ReceiveMessageResponse response = sqsClient.receiveMessage(requestBuilder.build());
                List<Message> messages = response.messages();

                if (messages == null || messages.isEmpty()) {
                    moreMessages = false;
                    continue;
                }

                logger.debug("Received {} messages from SQS queue", messages.size());

                for (Message message : messages) {
                    if (isTerminated()) {
                        break;
                    }

                    processMessage(message);
                }

                // If we received the max number, there may be more in the queue
                moreMessages = messages.size() >= resolvedMaxMessages;
            }

        } catch (SqsException e) {
            logger.error("SQS polling error: {}", e.awsErrorDetails().errorMessage(), e);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            logger.error("Unexpected error during SQS polling", e);
        } finally {
            eventController.dispatchEvent(new ConnectionStatusEvent(getChannelId(),
                    getMetaDataId(), getSourceName(), ConnectionStatusEventType.IDLE));
        }
    }

    // =========================================================================
    // Message Processing
    // =========================================================================

    private void processMessage(Message message) {
        S3EventMode s3Mode = connectorProperties.getS3EventMode();
        if (s3Mode == null) {
            s3Mode = S3EventMode.DISABLED;
        }

        if (s3Mode == S3EventMode.DISABLED) {
            processStandardMessage(message);
        } else {
            processS3EventMessage(message, s3Mode);
        }
    }

    private void processStandardMessage(Message message) {
        DispatchResult dispatchResult = null;

        try {
            RawMessage rawMessage = new RawMessage(message.body());
            rawMessage.setSourceMap(buildBaseSourceMap(message));

            dispatchResult = dispatchRawMessage(rawMessage);

            if (dispatchResult != null && dispatchResult.getProcessedMessage() != null) {
                deleteMessage(message);
            }

        } catch (Exception e) {
            logger.error("Error processing SQS message {}: {}", message.messageId(), e.getMessage(), e);
        } finally {
            finishDispatch(dispatchResult);
        }
    }

    private void processS3EventMessage(Message message, S3EventMode s3Mode) {
        boolean deleteMessage = false;

        try {
            String body = message.body();
            JsonNode root = objectMapper.readTree(body);

            // Auto-detect SNS envelope and unwrap
            if (root.has("Type") && "Notification".equals(root.path("Type").asText())) {
                String innerMessage = root.path("Message").asText();
                if (innerMessage != null && !innerMessage.isEmpty()) {
                    logger.debug("Detected SNS envelope, unwrapping inner Message");
                    root = objectMapper.readTree(innerMessage);
                }
            }

            // Detect event format and dispatch accordingly
            if (isEventBridgeS3Event(root)) {
                deleteMessage = processEventBridgeS3(root, message, body, s3Mode);
            } else if (root.path("Records").isArray() && root.path("Records").size() > 0) {
                deleteMessage = processStandardS3Records(root.path("Records"), message, body, s3Mode);
            } else {
                // Not a recognized S3 event format — fall back to standard processing
                logger.debug("SQS message {} is not a recognized S3 event format, processing as standard message",
                        message.messageId());
                processStandardMessage(message);
                return;
            }

        } catch (Exception e) {
            logger.error("Error parsing S3 event from SQS message {}: {}",
                    message.messageId(), e.getMessage(), e);
            // Fall back to standard processing on parse failure
            processStandardMessage(message);
            return;
        }

        if (deleteMessage) {
            deleteMessage(message);
        }
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
    private boolean processEventBridgeS3(JsonNode root, Message message, String body, S3EventMode s3Mode) {
        DispatchResult dispatchResult = null;

        try {
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

            if (s3Mode == S3EventMode.FETCH_OBJECT && bucketName != null && objectKey != null) {
                RawMessage fetched = fetchS3Object(bucketName, objectKey, objectSize, sourceMap);
                rawMessage = fetched != null ? fetched : new RawMessage(body);
            } else {
                rawMessage = new RawMessage(body);
            }

            rawMessage.setSourceMap(sourceMap);
            dispatchResult = dispatchRawMessage(rawMessage);

            return dispatchResult != null && dispatchResult.getProcessedMessage() != null;

        } catch (Exception e) {
            logger.error("Error processing EventBridge S3 event from SQS message {}: {}",
                    message.messageId(), e.getMessage(), e);
            return false;
        } finally {
            finishDispatch(dispatchResult);
        }
    }

    /**
     * Processes standard S3 notification format with Records[] array.
     * Format: { "Records": [{ "eventName": "...", "s3": { "bucket": {...}, "object": {...} } }] }
     */
    private boolean processStandardS3Records(JsonNode records, Message message, String body, S3EventMode s3Mode) {
        boolean allDispatched = true;

        for (int i = 0; i < records.size(); i++) {
            if (isTerminated()) {
                allDispatched = false;
                break;
            }

            JsonNode record = records.get(i);
            DispatchResult dispatchResult = null;

            try {
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

                if (s3Mode == S3EventMode.FETCH_OBJECT && bucketName != null && objectKey != null) {
                    RawMessage fetched = fetchS3Object(bucketName, objectKey, objectSize, sourceMap);
                    rawMessage = fetched != null ? fetched : new RawMessage(body);
                } else {
                    rawMessage = new RawMessage(body);
                }

                rawMessage.setSourceMap(sourceMap);
                dispatchResult = dispatchRawMessage(rawMessage);

                if (dispatchResult == null || dispatchResult.getProcessedMessage() == null) {
                    allDispatched = false;
                }

            } catch (Exception e) {
                logger.error("Error processing S3 event record {} from SQS message {}: {}",
                        i, message.messageId(), e.getMessage(), e);
                allDispatched = false;
            } finally {
                finishDispatch(dispatchResult);
            }
        }

        return allDispatched;
    }

    /**
     * Fetches an S3 object and populates the source map with object metadata
     * (content type, last modified, storage class, user metadata, etc.).
     * Returns a {@link RawMessage} containing the object content (as text or
     * binary depending on the configured file type), or null if the object
     * exceeds the size limit or the fetch fails.
     */
    private RawMessage fetchS3Object(String bucket, String key, long knownSize, Map<String, Object> sourceMap) {
        try {
            // Check size limit
            if (resolvedS3MaxObjectSizeBytes > 0) {
                long sizeToCheck = knownSize;

                // If size not in event, do a HEAD request
                if (sizeToCheck < 0) {
                    HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());
                    sizeToCheck = headResponse.contentLength();
                }

                if (sizeToCheck > resolvedS3MaxObjectSizeBytes) {
                    logger.warn("S3 object s3://{}/{} size ({} bytes) exceeds max limit ({} bytes). "
                            + "Skipping fetch and passing original event JSON.",
                            bucket, key, sizeToCheck, resolvedS3MaxObjectSizeBytes);
                    return null;
                }
            }

            ResponseBytes<GetObjectResponse> responseBytes = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());

            GetObjectResponse response = responseBytes.response();

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
                sourceMap.put("s3ObjectETag", response.eTag());
            }
            if (response.versionId() != null) {
                sourceMap.put("s3ObjectVersionId", response.versionId());
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

            byte[] bytes = responseBytes.asByteArray();

            if (resolvedS3BinaryMode) {
                logger.debug("Fetched S3 object s3://{}/{} ({} bytes, binary mode, contentType={})",
                        bucket, key, bytes.length, response.contentType());
                return new RawMessage(bytes);
            } else {
                Charset charset = resolveEncoding(response.contentType());
                logger.debug("Fetched S3 object s3://{}/{} ({} bytes, text mode, contentType={}, charset={})",
                        bucket, key, bytes.length, response.contentType(), charset.name());
                return new RawMessage(new String(bytes, charset));
            }

        } catch (S3Exception e) {
            logger.error("Failed to fetch S3 object s3://{}/{}: {}", bucket, key,
                    e.awsErrorDetails().errorMessage(), e);
            return null;
        } catch (Exception e) {
            logger.error("Unexpected error fetching S3 object s3://{}/{}: {}", bucket, key,
                    e.getMessage(), e);
            return null;
        }
    }

    /**
     * Resolves the charset to use for decoding fetched S3 object content.
     * Always tries the Content-Type header first; if no charset is present
     * there, falls back to the user-configured encoding.
     */
    private Charset resolveEncoding(String contentType) {
        // Always try Content-Type charset first
        if (contentType != null) {
            for (String param : contentType.split(";")) {
                String trimmed = param.trim();
                if (trimmed.toLowerCase().startsWith("charset=")) {
                    try {
                        return Charset.forName(trimmed.substring("charset=".length()).trim());
                    } catch (Exception e) {
                        logger.warn("Unknown charset in Content-Type '{}', falling back to configured encoding",
                                contentType);
                    }
                    break;
                }
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
            message.messageAttributes().forEach((key, attr) -> {
                sourceMap.put("sqsMsgAttr" + key, attr.stringValue());
            });
        }

        return sourceMap;
    }

    private static final int DELETE_MAX_RETRIES = 3;
    private static final long DELETE_RETRY_DELAY_MS = 1000;

    private void deleteMessage(Message message) {
        for (int attempt = 1; attempt <= DELETE_MAX_RETRIES; attempt++) {
            try {
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(resolvedQueueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());

                logger.debug("Deleted SQS message: {}", message.messageId());
                return;

            } catch (SqsException e) {
                if (attempt < DELETE_MAX_RETRIES) {
                    logger.warn("Failed to delete SQS message {} (attempt {}/{}): {}. Retrying in {}ms...",
                            message.messageId(), attempt, DELETE_MAX_RETRIES,
                            e.awsErrorDetails().errorMessage(), DELETE_RETRY_DELAY_MS);
                    try {
                        Thread.sleep(DELETE_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Interrupted while retrying delete for SQS message {}. "
                                + "Message may be redelivered after visibility timeout. Queue: {}",
                                message.messageId(), resolvedQueueUrl);
                        return;
                    }
                } else {
                    logger.error("Failed to delete SQS message {} after {} attempts. "
                            + "Message will be redelivered after visibility timeout ({}s). "
                            + "Queue: {}, ReceiptHandle: {}",
                            message.messageId(), DELETE_MAX_RETRIES, resolvedVisibilityTimeout,
                            resolvedQueueUrl, message.receiptHandle(), e);
                }
            }
        }
    }

    // =========================================================================
    // SQS Client Builder
    // =========================================================================

    /**
     * Builds the SqsClient using resolved (post-Velocity) property values.
     */
    private SqsClient buildSqsClient() {
        SqsClientBuilder builder = SqsClient.builder();

        if (resolvedRegion != null && !resolvedRegion.isBlank()) {
            builder.region(Region.of(resolvedRegion));
        }

        switch (connectorProperties.getAuthType()) {
            case STATIC:
                builder.credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(resolvedAccessKeyId, resolvedSecretAccessKey)));
                break;

            case ROLE:
                AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
                        .roleArn(resolvedRoleArn)
                        .roleSessionName("oie-sqs-connector");

                if (resolvedExternalId != null && !resolvedExternalId.isBlank()) {
                    roleRequestBuilder.externalId(resolvedExternalId);
                }

                StsClient stsClient = resolvedRegion != null && !resolvedRegion.isBlank()
                        ? StsClient.builder().region(Region.of(resolvedRegion)).build()
                        : StsClient.builder().build();

                builder.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(roleRequestBuilder.build())
                        .build());
                break;

            case DEFAULT:
            default:
                builder.credentialsProvider(DefaultCredentialsProvider.create());
                break;
        }

        return builder.build();
    }

    /**
     * Builds the S3Client using the same auth configuration as the SQS client.
     */
    private S3Client buildS3Client() {
        S3ClientBuilder builder = S3Client.builder();

        if (resolvedRegion != null && !resolvedRegion.isBlank()) {
            builder.region(Region.of(resolvedRegion));
        }

        switch (connectorProperties.getAuthType()) {
            case STATIC:
                builder.credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(resolvedAccessKeyId, resolvedSecretAccessKey)));
                break;

            case ROLE:
                AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
                        .roleArn(resolvedRoleArn)
                        .roleSessionName("mirth-sqs-connector-s3");

                if (resolvedExternalId != null && !resolvedExternalId.isBlank()) {
                    roleRequestBuilder.externalId(resolvedExternalId);
                }

                StsClient stsClient = resolvedRegion != null && !resolvedRegion.isBlank()
                        ? StsClient.builder().region(Region.of(resolvedRegion)).build()
                        : StsClient.builder().build();

                builder.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(roleRequestBuilder.build())
                        .build());
                break;

            case DEFAULT:
            default:
                builder.credentialsProvider(DefaultCredentialsProvider.create());
                break;
        }

        return builder.build();
    }
}
