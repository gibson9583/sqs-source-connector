/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties.S3EventMode;
import com.mirth.connect.donkey.model.event.Event;
import com.mirth.connect.donkey.model.message.BatchRawMessage;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.server.ConnectorTaskException;
import com.mirth.connect.donkey.server.channel.ChannelException;
import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.donkey.server.event.ErrorEvent;
import com.mirth.connect.donkey.server.message.batch.BatchMessageException;
import com.mirth.connect.donkey.server.message.batch.BatchMessageReader;
import com.mirth.connect.donkey.server.message.batch.ResponseHandler;
import com.mirth.connect.server.controllers.EventController;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class SqsReceiverTest {
    private static class Receiver extends SqsReceiver {
        final List<RawMessage> dispatched = new ArrayList<>();
        final List<BatchRawMessage> batches = new ArrayList<>();
        final Set<Integer> dispatchFailures = new HashSet<>();
        final List<String> actions = new ArrayList<>();
        DispatchResult nextResult = accepted();
        RuntimeException finishFailure;
        boolean batch;
        boolean terminated;
        Boolean batchResult = true;

        @Override public boolean isTerminated() { return terminated; }
        @Override public boolean isProcessBatch() { return batch; }
        @Override public DispatchResult dispatchRawMessage(RawMessage message) {
            dispatched.add(message);
            actions.add("dispatch");
            if (dispatchFailures.contains(dispatched.size())) throw new IllegalStateException("injected dispatch failure");
            return nextResult;
        }
        @Override public void finishDispatch(DispatchResult result) {
            actions.add("finish");
            if (finishFailure != null) throw finishFailure;
        }
        @Override public Boolean dispatchBatchMessage(BatchRawMessage message, ResponseHandler handler) throws BatchMessageException {
            batches.add(message);
            if (Boolean.TRUE.equals(batchResult)) {
                handler.setDispatchResult(nextResult);
                try { handler.responseProcess(1, true); }
                catch (Exception e) { throw new BatchMessageException("batch dispatch failed", e); }
            }
            return batchResult;
        }
        void pollOnce() throws InterruptedException { poll(); }
    }

    private static class Fixture {
        final Receiver receiver = new Receiver();
        final SqsReceiverProperties props = new SqsReceiverProperties();
        final SqsClient sqs = mock(SqsClient.class);
        final S3Client s3 = mock(S3Client.class);
        final List<Event> events = new ArrayList<>();
        final AtomicBoolean aborted = new AtomicBoolean();
        final AtomicInteger bytesRead = new AtomicInteger();

        Fixture(S3EventMode mode) throws Exception {
            props.setQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test.fifo");
            props.setRegion("us-east-1");
            props.setS3EventMode(mode);
            receiver.setChannelId("test-channel");
            receiver.setConnectorProperties(props);
            set(receiver, "connectorProperties", props);
            set(receiver, "sqsClient", sqs);
            set(receiver, "s3Client", s3);
            set(receiver, "resolvedQueueUrl", props.getQueueUrl());
            set(receiver, "resolvedMaxMessages", 10);
            set(receiver, "resolvedS3MaxObjectSizeBytes", 1024L);
            set(receiver, "resolvedS3Encoding", "UTF-8");
            EventController controller = mock(EventController.class);
            doAnswer(invocation -> { events.add(invocation.getArgument(0)); return null; }).when(controller).dispatchEvent(any(Event.class));
            set(receiver, "eventController", controller);
            when(sqs.deleteMessage(any(DeleteMessageRequest.class))).thenAnswer(invocation -> {
                receiver.actions.add("delete");
                return DeleteMessageResponse.builder().build();
            });
            when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().build());
        }
        boolean process(String body) throws Exception { return process(message("one", body, "group-a")); }
        boolean process(Message message) throws Exception {
            Method method = SqsReceiver.class.getDeclaredMethod("processMessage", Message.class);
            method.setAccessible(true);
            return (boolean) method.invoke(receiver, message);
        }
        void object(byte[] bytes, Long length, String contentType) {
            InputStream input = new ByteArrayInputStream(bytes) {
                @Override public synchronized int read(byte[] buffer, int offset, int count) {
                    int result = super.read(buffer, offset, count);
                    if (result > 0) bytesRead.addAndGet(result);
                    return result;
                }
            };
            stream(input, length, contentType);
        }
        void stream(InputStream input, Long length, String contentType) {
            GetObjectResponse response = GetObjectResponse.builder().contentLength(length).contentType(contentType)
                    .versionId("event-version").eTag("\"event-etag\"").build();
            when(s3.getObject(any(GetObjectRequest.class))).thenReturn(new ResponseInputStream<>(response,
                    AbortableInputStream.create(input, () -> aborted.set(true))));
        }
        long errors() { return events.stream().filter(ErrorEvent.class::isInstance).count(); }
        void notDeleted() { verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class)); }
    }

    private static DispatchResult accepted() {
        // A valid source-queue admission intentionally has no processedMessage.
        DispatchResult result = mock(DispatchResult.class);
        when(result.getMessageId()).thenReturn(42L);
        return result;
    }
    private static void set(Receiver receiver, String name, Object value) throws Exception {
        Field field = SqsReceiver.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(receiver, value);
    }
    private static Message message(String id, String body, String group) {
        return Message.builder().messageId(id).receiptHandle("receipt-" + id).body(body)
                .attributes(Map.of(MessageSystemAttributeName.MESSAGE_GROUP_ID, group)).build();
    }
    private static String record() {
        return "{\"eventSource\":\"aws:s3\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\"bucket\"},\"object\":{\"key\":\"file+name.txt\",\"size\":1,\"versionId\":\"event-version\",\"eTag\":\"event-etag\"}}}";
    }
    private static String notification(boolean bridge) {
        return bridge ? "{\"source\":\"aws.s3\",\"detail-type\":\"Object Created\",\"detail\":{\"bucket\":{\"name\":\"bucket\"},\"object\":{\"key\":\"file name.txt\",\"size\":1,\"version-id\":\"event-version\",\"etag\":\"event-etag\"}}}"
                : "{\"Records\":[" + record() + "]}";
    }

    @ParameterizedTest @EnumSource(S3EventMode.class)
    void acknowledgesSourceQueueAdmissionAfterEngineFinalization(S3EventMode mode) throws Exception {
        Fixture f = new Fixture(mode);
        f.object("payload".getBytes(StandardCharsets.UTF_8), 7L, "text/plain");
        assertTrue(f.process(mode == S3EventMode.DISABLED ? "body" : notification(false)));
        assertEquals(List.of("dispatch", "finish", "delete"), f.receiver.actions);
        assertEquals(0, f.errors());
    }

    @Test void retainsReceiptWhenDispatchResultContainsChannelException() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        when(f.receiver.nextResult.getChannelException()).thenReturn(new ChannelException(false));
        assertFalse(f.process("body"));
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @Test void retainsReceiptWhenEngineFinalizationFails() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.finishFailure = new IllegalStateException("commit failed");
        assertFalse(f.process("body"));
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @Test void retainsReceiptWhenDispatchReturnsNull() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.nextResult = null;
        assertFalse(f.process("body"));
        f.notDeleted();
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void s3ApiFailureDoesNotSubstituteOrAcknowledge(boolean bridge) throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        // No awsErrorDetails: generic SDK errors must still preserve the receipt and emit an alert.
        when(f.s3.getObject(any(GetObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(503).message("unavailable").build());
        assertFalse(f.process(notification(bridge)));
        assertTrue(f.receiver.dispatched.isEmpty());
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void fetchPinsEventIdentityAndPreservesEventMetadata(boolean bridge) throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[]{65}, 1L, "text/plain");
        assertTrue(f.process(notification(bridge)));
        ArgumentCaptor<GetObjectRequest> capture = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(f.s3).getObject(capture.capture());
        assertEquals("event-version", capture.getValue().versionId());
        assertEquals("\"event-etag\"", capture.getValue().ifMatch());
        assertEquals("file name.txt", capture.getValue().key());
        Map<String,Object> map = f.receiver.dispatched.get(0).getSourceMap();
        assertEquals("event-etag", map.get("s3ObjectETag"));
        assertEquals("\"event-etag\"", map.get("s3FetchedObjectETag"));
        assertEquals("event-version", map.get("s3ObjectVersionId"));
        assertEquals("FETCHED", map.get("s3FetchStatus"));
    }

    @Test void enforcesRealContentLengthInsteadOfStaleSmallEventSize() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[2048], 2048L, "application/octet-stream");
        assertTrue(f.process(notification(false)));
        assertEquals(0, f.bytesRead.get());
        assertTrue(f.aborted.get());
        assertEquals(notification(false), f.receiver.dispatched.get(0).getRawData());
        assertEquals("OVERSIZED", f.receiver.dispatched.get(0).getSourceMap().get("s3FetchStatus"));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void boundsActualBytesWithAbsentOrIncorrectLength(boolean missingLength) throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[10000], missingLength ? null : 1L, "application/octet-stream");
        assertTrue(f.process(notification(false)));
        assertEquals(1025, f.bytesRead.get());
        assertTrue(f.aborted.get());
        assertEquals("OVERSIZED", f.receiver.dispatched.get(0).getSourceMap().get("s3FetchStatus"));
    }

    @Test void acceptsExactSizeLimitAndDoesNotAbortSuccessfulStream() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[1024], null, "application/octet-stream");
        set(f.receiver, "resolvedS3BinaryMode", true);
        assertTrue(f.process(notification(false)));
        assertEquals(1024, f.receiver.dispatched.get(0).getRawBytes().length);
        assertFalse(f.aborted.get());
    }

    @Test void explicitUnlimitedSizeAllowsLargerObject() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        set(f.receiver, "resolvedS3MaxObjectSizeBytes", 0L);
        set(f.receiver, "resolvedS3BinaryMode", true);
        f.object(new byte[2048], 2048L, "application/octet-stream");
        assertTrue(f.process(notification(false)));
        assertEquals(2048, f.receiver.dispatched.get(0).getRawBytes().length);
        assertFalse(f.aborted.get());
    }

    @Test void unversionedEventStillUsesConditionalEtag() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[]{65}, 1L, "text/plain");
        assertTrue(f.process(notification(false).replace(",\"versionId\":\"event-version\"", "")));
        ArgumentCaptor<GetObjectRequest> capture = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(f.s3).getObject(capture.capture());
        assertNull(capture.getValue().versionId());
        assertEquals("\"event-etag\"", capture.getValue().ifMatch());
    }

    @Test void readFailureAbortsStreamAndRetainsReceipt() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.stream(new InputStream() { @Override public int read() throws IOException { throw new IOException("broken stream"); } }, null, "text/plain");
        assertFalse(f.process(notification(false)));
        assertTrue(f.aborted.get());
        assertTrue(f.receiver.dispatched.isEmpty());
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @Test void stopDuringStreamingAbortsBeforeDrainingEntireObject() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        AtomicInteger reads = new AtomicInteger();
        f.stream(new InputStream() {
            @Override public int read() { return 1; }
            @Override public int read(byte[] bytes, int offset, int length) {
                reads.incrementAndGet();
                f.receiver.terminated = true;
                bytes[offset] = 1;
                return 1;
            }
        }, null, "text/plain");
        assertFalse(f.process(notification(false)));
        assertEquals(1, reads.get());
        assertTrue(f.aborted.get());
        assertTrue(f.receiver.dispatched.isEmpty());
        f.notDeleted();
    }

    @ParameterizedTest @ValueSource(strings = {"ObjectRemoved:Delete", "ObjectRemoved:DeleteMarkerCreated",
            "LifecycleExpiration:Delete", "LifecycleExpiration:DeleteMarkerCreated"})
    void removalEventsDeliverNotificationWithoutFetchingDeletedObject(String eventName) throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        String body = notification(false).replace("ObjectCreated:Put", eventName);
        assertTrue(f.process(body));
        assertEquals(body, f.receiver.dispatched.get(0).getRawData());
        assertEquals("NOT_APPLICABLE", f.receiver.dispatched.get(0).getSourceMap().get("s3FetchStatus"));
        verifyNoInteractions(f.s3);
    }

    @ParameterizedTest @ValueSource(strings = {"DeleteObject", "Lifecycle Expiration"})
    void eventBridgeDeletionReasonsDeliverNotificationWithoutGet(String reason) throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        String body = notification(true).replace("Object Created", "Object Deleted")
                .replace("\"detail\":{", "\"detail\":{\"reason\":\"" + reason + "\",");
        assertTrue(f.process(body));
        assertEquals(body, f.receiver.dispatched.get(0).getRawData());
        assertEquals("NOT_APPLICABLE", f.receiver.dispatched.get(0).getSourceMap().get("s3FetchStatus"));
        verifyNoInteractions(f.s3);
    }

    @Test void tagDeletionStillFetchesExistingObjectInsteadOfBroadDeleteFallback() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[]{65}, 1L, "text/plain");
        assertTrue(f.process(notification(false).replace("ObjectCreated:Put", "ObjectTagging:Delete")));
        assertEquals("A", f.receiver.dispatched.get(0).getRawData());
        assertEquals("FETCHED", f.receiver.dispatched.get(0).getSourceMap().get("s3FetchStatus"));
    }

    @Test void fifoDispatchFailureDefersSameGroupButAllowsIndependentGroup() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.dispatchFailures.add(1);
        when(f.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(
                message("a1", "a1", "a"), message("a2", "a2", "a"), message("b1", "b1", "b")).build());
        f.receiver.pollOnce();
        assertEquals(List.of("a1", "b1"), f.receiver.dispatched.stream().map(RawMessage::getRawData).toList());
        ArgumentCaptor<DeleteMessageRequest> capture = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(f.sqs).deleteMessage(capture.capture());
        assertEquals("receipt-b1", capture.getValue().receiptHandle());
    }

    @Test void fifoDeleteFailureDefersSameGroupSuccessorAndDoesNotRetryPermanentError() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        when(f.sqs.deleteMessage(any(DeleteMessageRequest.class))).thenThrow(SqsException.builder().statusCode(403).message("forbidden").build());
        when(f.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(
                message("a1", "a1", "a"), message("a2", "a2", "a")).build());
        f.receiver.pollOnce();
        assertEquals(1, f.receiver.dispatched.size());
        verify(f.sqs, times(1)).deleteMessage(any(DeleteMessageRequest.class));
        assertEquals(1, f.errors());
    }

    @Test void standardFairQueueDoesNotImposeFifoBarrierOnTenantGroup() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        set(f.receiver, "resolvedQueueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/standard");
        f.receiver.dispatchFailures.add(1);
        when(f.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(
                message("a1", "a1", "tenant"), message("a2", "a2", "tenant")).build());
        f.receiver.pollOnce();
        assertEquals(List.of("a1", "a2"), f.receiver.dispatched.stream().map(RawMessage::getRawData).toList());
        ArgumentCaptor<DeleteMessageRequest> capture = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(f.sqs).deleteMessage(capture.capture());
        assertEquals("receipt-a2", capture.getValue().receiptHandle());
    }

    @Test void receiveFailureEmitsSourceErrorEvenWithoutAwsDetails() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        when(f.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.builder().message("unstructured failure").build());
        assertDoesNotThrow(f.receiver::pollOnce);
        assertEquals(1, f.errors());
        assertEquals(3, f.events.size());
    }

    @Test void textBatchUsesEngineBatchApiAndPreservesMetadata() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.batch = true;
        assertTrue(f.process("one\ntwo\n"));
        assertTrue(f.receiver.dispatched.isEmpty());
        assertEquals(1, f.receiver.batches.size());
        assertEquals("one", f.receiver.batches.get(0).getSourceMap().get("sqsMessageId"));
        assertInstanceOf(BatchMessageReader.class, f.receiver.batches.get(0).getBatchMessageSource());
        verify(f.sqs).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test void batchChannelExceptionRetainsEntireSqsEnvelope() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.batch = true;
        when(f.receiver.nextResult.getChannelException()).thenReturn(new ChannelException(false));
        assertFalse(f.process("one\ntwo"));
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @Test void emptyOrUnstartedBatchCannotDeleteUndispatchedPayload() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.receiver.batch = true;
        f.receiver.batchResult = false;
        assertFalse(f.process("body"));
        f.notDeleted();
    }

    @Test void binaryAttributesRoundTripUsingBase64AndTypeMetadata() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        Message message = message("attr", "body", "g").toBuilder().messageAttributes(Map.of("bytes",
                MessageAttributeValue.builder().dataType("Binary.custom").binaryValue(SdkBytes.fromByteArray(new byte[]{1,2,3})).build())).build();
        assertTrue(f.process(message));
        assertEquals("AQID", f.receiver.dispatched.get(0).getSourceMap().get("sqsMsgAttrbytes"));
        assertEquals(Map.of("bytes", "Binary.custom"), f.receiver.dispatched.get(0).getSourceMap().get("sqsMessageAttributeTypes"));
    }

    @Test void typeMetadataCannotOverwriteAnAttributeNamedTypeX() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        Message message = message("attr", "body", "g").toBuilder().messageAttributes(Map.of(
                "X", MessageAttributeValue.builder().dataType("Number").stringValue("123").build(),
                "TypeX", MessageAttributeValue.builder().dataType("String").stringValue("user-value").build())).build();
        assertTrue(f.process(message));
        assertEquals("user-value", f.receiver.dispatched.get(0).getSourceMap().get("sqsMsgAttrTypeX"));
        assertEquals(Map.of("X", "Number", "TypeX", "String"), f.receiver.dispatched.get(0).getSourceMap().get("sqsMessageAttributeTypes"));
    }

    @Test void quotedCharsetDecodesNonUtf8BytesCorrectly() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.object(new byte[]{(byte)0xe9}, 1L, "text/plain; charset=\"ISO-8859-1\"");
        assertTrue(f.process(notification(false)));
        assertEquals("é", f.receiver.dispatched.get(0).getRawData());
    }

    @Test void nonS3RecordsAreDispatchedOnceWithoutFalseMetadata() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        String body = "{\"Records\":[{\"data\":1},{\"data\":2}]}";
        assertTrue(f.process(body));
        assertEquals(1, f.receiver.dispatched.size());
        assertEquals(body, f.receiver.dispatched.get(0).getRawData());
        assertFalse(f.receiver.dispatched.get(0).getSourceMap().containsKey("s3EventFormat"));
        verifyNoInteractions(f.s3);
    }

    @Test void snsWrappingPreservesOriginalBodyAndExtractsS3Metadata() throws Exception {
        Fixture f = new Fixture(S3EventMode.EXTRACT_DETAILS);
        String body = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(Map.of("Type", "Notification", "Message", notification(false)));
        assertTrue(f.process(body));
        assertEquals(body, f.receiver.dispatched.get(0).getRawData());
        assertEquals("file name.txt", f.receiver.dispatched.get(0).getSourceMap().get("s3ObjectKey"));
    }

    @Test void partialS3RecordFailureRetainsEnvelopeAndStopsFurtherRecords() throws Exception {
        Fixture f = new Fixture(S3EventMode.EXTRACT_DETAILS);
        f.receiver.dispatchFailures.add(2);
        assertFalse(f.process("{\"Records\":[" + record() + "," + record() + "," + record() + "]}"));
        assertEquals(2, f.receiver.dispatched.size());
        f.notDeleted();
        assertEquals(1, f.errors());
    }

    @Test void overflowingSizeLimitIsRejectedBeforeAnyAwsConnection() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.props.setS3MaxObjectSizeKB(Long.toString(Long.MAX_VALUE));
        ConnectorTaskException error = assertThrows(ConnectorTaskException.class, f.receiver::onStart);
        assertTrue(error.getMessage().contains("too large"), error.getMessage());
    }

    @Test void fifoSourceOrderRejectsQueueOnAndMultipleThreads() throws Exception {
        Fixture f = new Fixture(S3EventMode.DISABLED);
        f.props.setMessageGroupHandling(true);
        f.props.getSourceConnectorProperties().setRespondAfterProcessing(false);
        ConnectorTaskException error = assertThrows(ConnectorTaskException.class, f.receiver::onStart);
        assertTrue(error.getMessage().contains("Source Queue OFF"), error.getMessage());
        f.props.getSourceConnectorProperties().setRespondAfterProcessing(true);
        f.props.getSourceConnectorProperties().setProcessingThreads(2);
        error = assertThrows(ConnectorTaskException.class, f.receiver::onStart);
        assertTrue(error.getMessage().contains("one processing thread"), error.getMessage());
    }

    @Test void binaryFetchAndBatchCombinationIsRejectedAtStart() throws Exception {
        Fixture f = new Fixture(S3EventMode.FETCH_OBJECT);
        f.props.setS3FileType("Binary");
        f.props.getSourceConnectorProperties().setProcessBatch(true);
        ConnectorTaskException error = assertThrows(ConnectorTaskException.class, f.receiver::onStart);
        assertTrue(error.getMessage().contains("Process Batch requires text"), error.getMessage());
    }
}
