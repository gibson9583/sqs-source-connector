/* SPDX-License-Identifier: MPL-2.0 */
package com.mirth.connect.donkey.server.channel;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.donkey.model.channel.DeployedState;
import com.mirth.connect.donkey.model.message.BatchRawMessage;
import com.mirth.connect.donkey.model.message.RawMessage;
import com.mirth.connect.donkey.server.message.batch.BatchAdaptor;
import com.mirth.connect.donkey.server.message.batch.BatchAdaptorFactory;
import com.mirth.connect.donkey.server.message.batch.BatchMessageReader;
import com.mirth.connect.server.controllers.EventController;
import io.github.gibson9583.sqs.SqsReceiver;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;

/** Exercises the engine's real batch dispatch loop; only persistence and AWS are replaced. */
class SqsReceiverBatchIntegrationTest {
    private static class Receiver extends SqsReceiver {
        int finished;
        @Override public boolean isTerminated() { return false; }
        @Override public void finishDispatch(DispatchResult result) { finished++; }
    }
    private static class Lines extends BatchAdaptorFactory {
        boolean cleaned;
        Lines(SourceConnector connector) { super(connector); }
        @Override public BatchAdaptor createBatchAdaptor(BatchRawMessage message) {
            BufferedReader reader = new BufferedReader(((BatchMessageReader) message.getBatchMessageSource()).getReader());
            return new BatchAdaptor(this, sourceConnector, message) {
                @Override protected String getNextMessage(int sequence) throws Exception { return reader.readLine(); }
                @Override public void cleanup() { cleaned = true; }
            };
        }
        @Override public void onDeploy() {}
        @Override public void onUndeploy() {}
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void splitsRecordsAndRetainsWholeEnvelopeOnPartialAdmissionFailure(boolean failSecond) throws Exception {
        Receiver receiver = new Receiver();
        receiver.setCurrentState(DeployedState.STARTED);
        receiver.setChannelId("batch-test");
        SqsReceiverProperties properties = new SqsReceiverProperties();
        receiver.setConnectorProperties(properties);
        inject(receiver, "connectorProperties", properties);
        inject(receiver, "resolvedQueueUrl", "https://example.invalid/queue");
        inject(receiver, "eventController", mock(EventController.class));
        SqsClient sqs = mock(SqsClient.class);
        when(sqs.deleteMessage(any(DeleteMessageRequest.class))).thenReturn(DeleteMessageResponse.builder().build());
        inject(receiver, "sqsClient", sqs);
        List<RawMessage> dispatched = new ArrayList<>();
        Channel channel = mock(Channel.class);
        when(channel.dispatchRawMessage(any(RawMessage.class), eq(true))).thenAnswer(invocation -> {
            dispatched.add(invocation.getArgument(0));
            return new DispatchResult(dispatched.size(), null, null, false, false,
                    failSecond && dispatched.size() == 2 ? new ChannelException(false) : null);
        });
        receiver.setChannel(channel);
        Lines factory = new Lines(receiver);
        receiver.setBatchAdaptorFactory(factory);
        factory.start();
        Method process = SqsReceiver.class.getDeclaredMethod("processMessage", Message.class);
        process.setAccessible(true);
        boolean accepted = (boolean) process.invoke(receiver,
                Message.builder().messageId("batch-envelope").receiptHandle("receipt").body("one\ntwo\nthree\n").build());
        assertEquals(!failSecond, accepted);
        assertEquals(failSecond ? List.of("one", "two") : List.of("one", "two", "three"),
                dispatched.stream().map(RawMessage::getRawData).toList());
        assertTrue(dispatched.stream().allMatch(message -> "batch-envelope".equals(message.getSourceMap().get("sqsMessageId"))));
        assertEquals(dispatched.size(), receiver.finished);
        assertTrue(factory.cleaned);
        verify(sqs, times(failSecond ? 0 : 1)).deleteMessage(any(DeleteMessageRequest.class));
        // Also detects a leaked in-flight batch count: stop must complete after success/failure.
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(1), factory::stop);
    }

    private static void inject(Receiver receiver, String name, Object value) throws Exception {
        Field field = SqsReceiver.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(receiver, value);
    }
}
