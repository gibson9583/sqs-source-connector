/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;

import org.apache.velocity.VelocityContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsMessageAttribute;
import com.mirth.connect.donkey.model.message.ConnectorMessage;
import com.mirth.connect.donkey.model.message.Response;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.server.controllers.EventController;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ValueReplacer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsDispatcherTest {
    private static SqsDispatcherProperties properties() {
        SqsDispatcherProperties props = new SqsDispatcherProperties();
        props.setQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/review");
        props.setTemplate("body");
        return props;
    }

    @Test
    void buildsTypedAttributesAndKeepsStandardQueueFairness() {
        SqsDispatcherProperties props = properties();
        props.setMessageGroupId("tenant-1");
        props.setMessageAttributes(List.of(new SqsMessageAttribute("text", "String", " Unicode ☃ \n"),
                new SqsMessageAttribute("number", "Number", "-123.45"),
                new SqsMessageAttribute("binary", "Binary", "AAH/")));
        SendMessageRequest request = SqsDispatcher.buildSendRequest(props);
        assertEquals("tenant-1", request.messageGroupId());
        assertNull(request.delaySeconds());
        assertEquals(" Unicode ☃ \n", request.messageAttributes().get("text").stringValue());
        assertEquals("Number", request.messageAttributes().get("number").dataType());
        assertEquals("-123.45", request.messageAttributes().get("number").stringValue());
        assertArrayEquals(new byte[]{0, 1, (byte) 255}, request.messageAttributes().get("binary").binaryValue().asByteArray());
    }

    @Test
    void replacementResolvesEachEnvelopeWithoutChangingOriginalOrPreviouslyQueuedEnvelope() throws Exception {
        SqsDispatcher sender = new SqsDispatcher();
        // Exercise the production replacer/map loader without starting engine controllers.
        inject(sender, "replacer", new ConfiguredReplacer());
        ConnectorMessage message = new ConnectorMessage();
        message.setChannelId("sender-unit-test");
        message.getChannelMap().put("attributeName", "tenant");
        message.getChannelMap().put("attributeValue", "first");
        SqsDispatcherProperties original = properties();
        original.setMessageAttributes(List.of(new SqsMessageAttribute("${attributeName}", "String", "${attributeValue}")));
        SqsDispatcherProperties queued = original.clone();
        sender.replaceConnectorProperties(queued, message);
        assertEquals("first", SqsDispatcher.buildSendRequest(queued).messageAttributes().get("tenant").stringValue());
        message.getChannelMap().put("attributeValue", "second");
        assertEquals("first", SqsDispatcher.buildSendRequest(queued).messageAttributes().get("tenant").stringValue());
        SqsDispatcherProperties regenerated = original.clone();
        sender.replaceConnectorProperties(regenerated, message);
        assertEquals("second", SqsDispatcher.buildSendRequest(regenerated).messageAttributes().get("tenant").stringValue());
        assertEquals("${attributeValue}", original.getMessageAttributes().get(0).getValue());

        original.setMessageAttributes(List.of(new SqsMessageAttribute("${attributeName}", "String", "one"),
                new SqsMessageAttribute("tenant", "String", "two")));
        SqsDispatcherProperties duplicate = original.clone();
        sender.replaceConnectorProperties(duplicate, message);
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(duplicate));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"AWS.reserved", "aMaZoN.reserved", ".first", "last.", "two..dots", "has space", "bad!", "☃"})
    void rejectsInvalidAttributeNames(String name) {
        SqsDispatcherProperties props = properties();
        props.setMessageAttributes(List.of(new SqsMessageAttribute(name, "String", "value")));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @Test
    void enforcesNameLengthCountAndDuplicateRules() {
        SqsDispatcherProperties props = properties();
        for (int index = 0; index < 10; index++) {
            props.getMessageAttributes().add(new SqsMessageAttribute("a" + index, "String", "value"));
        }
        assertEquals(10, SqsDispatcher.buildSendRequest(props).messageAttributes().size());
        props.getMessageAttributes().add(new SqsMessageAttribute("extra", "String", "value"));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setMessageAttributes(List.of(new SqsMessageAttribute("a".repeat(256), "String", "value")));
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props));
        props.getMessageAttributes().get(0).setName("a".repeat(257));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setMessageAttributes(Arrays.asList((SqsMessageAttribute) null));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-0", "1e-128", "-1e126", "1e126", "12345678901234567890123456789012345678", "1.000000"})
    void acceptsFiniteNumbersWithinAwsRangeAndPrecision(String number) {
        SqsDispatcherProperties props = properties();
        props.setMessageAttributes(List.of(new SqsMessageAttribute("amount", "Number", number)));
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props));
    }

    @ParameterizedTest
    @ValueSource(strings = {"NaN", "Infinity", "1e-129", "1e127", "123456789012345678901234567890123456789", "1e999999999", "1e999999999999999", "not-a-number", " 1 "})
    void rejectsInvalidNumbersWithoutExpandingHugeExponents(String number) {
        SqsDispatcherProperties props = properties();
        props.setMessageAttributes(List.of(new SqsMessageAttribute("amount", "Number", number)));
        assertTimeout(Duration.ofSeconds(2), () -> assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props)));
    }

    @Test
    void numericValidationBoundsWorkForVeryLargeMantissas() {
        SqsDispatcherProperties props = properties();
        props.setMessageAttributes(List.of(new SqsMessageAttribute("amount", "Number", "1" + "0".repeat(100000) + "e-100000")));
        assertTimeout(Duration.ofSeconds(2), () -> assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props)));
        props.getMessageAttributes().get(0).setValue("1".repeat(100000));
        assertTimeout(Duration.ofSeconds(2), () -> assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props)));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"***", "A", "AA===", "AA\nE="})
    void rejectsEmptyOrMalformedBase64(String value) {
        SqsDispatcherProperties props = properties();
        props.setMessageAttributes(List.of(new SqsMessageAttribute("bytes", "Binary", value)));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @Test
    void rejectsUnsupportedTypesEmptyValuesAndInvalidUnicode() {
        SqsDispatcherProperties props = properties();
        for (String type : Arrays.asList(null, "", "string", "Number.custom")) {
            props.setMessageAttributes(List.of(new SqsMessageAttribute("value", type, "1")));
            assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        }
        for (String value : Arrays.asList(null, "", "bad\u0001", "unpaired\uD800")) {
            props.setMessageAttributes(List.of(new SqsMessageAttribute("value", "String", value)));
            assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        }
        props.setMessageAttributes(List.of());
        props.setTemplate("\t\n\r ☃😀");
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props));
        props.setTemplate("bad\u0000");
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @Test
    void enforcesAggregateUtf8AndDecodedBinarySizeIncludingNamesAndTypes() {
        SqsDispatcherProperties props = properties();
        props.setTemplate("x".repeat(1024 * 1024 + 1));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setTemplate("x".repeat(1024 * 1024));
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props));
        props.setMessageAttributes(List.of(new SqsMessageAttribute("a", "String", "v")));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setTemplate("é".repeat(524288));
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setTemplate("x");
        byte[] exactSize = new byte[1024 * 1024 - 1 - 1 - "Binary".length()];
        props.setMessageAttributes(List.of(new SqsMessageAttribute("a", "Binary", Base64.getEncoder().encodeToString(exactSize))));
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props));
        props.setTemplate("xx");
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @Test
    void validatesFifoConstraintsAfterQueueUrlResolution() {
        SqsDispatcherProperties props = properties();
        props.setQueueUrl(props.getQueueUrl() + ".fifo");
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setMessageGroupId("group");
        assertDoesNotThrow(() -> SqsDispatcher.buildSendRequest(props)); // Content-based dedup may be configured on queue.
        props.setDelaySeconds("0");
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setDelaySeconds("");
        props.setMessageDeduplicationId(" event-1 ");
        assertEquals("event-1", SqsDispatcher.buildSendRequest(props).messageDeduplicationId());
        props.setQueueUrl(properties().getQueueUrl());
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
        props.setMessageDeduplicationId("");
        props.setDelaySeconds("900");
        assertEquals(900, SqsDispatcher.buildSendRequest(props).delaySeconds());
        props.setDelaySeconds("901");
        assertThrows(IllegalArgumentException.class, () -> SqsDispatcher.buildSendRequest(props));
    }

    @Test
    void validationAndUnstructuredAwsFailuresRemainRecoverableEngineResponses() throws Exception {
        SqsDispatcher sender = new SqsDispatcher();
        EventController events = mock(EventController.class);
        SqsClient sqs = mock(SqsClient.class);
        inject(sender, "eventController", events);
        inject(sender, "sqsClient", sqs);
        sender.setChannelId("sender-unit-test");
        sender.setMetaDataId(1);
        sender.setDestinationName("sender");
        SqsDispatcherProperties props = properties();
        sender.setConnectorProperties(props);
        ConnectorMessage message = new ConnectorMessage();
        props.setMessageAttributes(List.of(new SqsMessageAttribute("bad name", "String", "value")));
        Response invalid = sender.send(props, message);
        assertEquals(Status.QUEUED, invalid.getStatus());
        verifyNoInteractions(sqs);
        props.setMessageAttributes(List.of());
        when(sqs.sendMessage(any(SendMessageRequest.class))).thenThrow(SqsException.builder().message("unstructured failure").build());
        Response error = assertDoesNotThrow(() -> sender.send(props, message));
        assertEquals(Status.QUEUED, error.getStatus());
        assertTrue(error.getError().contains("unstructured failure"));
        error.fixStatus(false);
        assertEquals(Status.ERROR, error.getStatus());
        verify(events, times(6)).dispatchEvent(any()); // SENDING, error, IDLE for each attempt.
        reset(sqs);
        when(sqs.sendMessage(any(SendMessageRequest.class))).thenReturn(SendMessageResponse.builder().messageId("id").build());
        Response success = sender.send(props, message);
        assertEquals(Status.SENT, success.getStatus());
        assertEquals("id", success.getMessage());
    }

    private static void inject(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class PlainReplacer extends ValueReplacer {
        VelocityContext context() { return super.getDefaultContext(); }
    }

    private static class ConfiguredReplacer extends TemplateValueReplacer {
        protected VelocityContext getDefaultContext() {
            VelocityContext context = new PlainReplacer().context();
            loadContextFromMap(context, Map.of("exampleConfigKey", "configured"));
            return context;
        }

    }
}
