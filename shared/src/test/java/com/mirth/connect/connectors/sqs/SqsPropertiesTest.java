/*
 * SPDX-License-Identifier: MPL-2.0
 */
package com.mirth.connect.connectors.sqs;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;

import org.junit.jupiter.api.Test;

import com.mirth.connect.donkey.model.channel.ConnectorPluginProperties;

class SqsPropertiesTest {
    @Test
    void destinationEqualityIncludesQueueSettingsAndKeepsCloneHashConsistent() {
        SqsDispatcherProperties original = new SqsDispatcherProperties();
        original.getDestinationConnectorProperties().setQueueEnabled(true);
        original.setPluginProperties(new HashSet<>(Set.of(new ExtraSettings("one"))));
        original.getDestinationConnectorProperties().setPluginProperties(new HashSet<>(Set.of(new ExtraSettings("nested"))));
        SqsDispatcherProperties copy = original.clone();
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        copy.getDestinationConnectorProperties().setRetryCount(5);
        assertNotEquals(original, copy);
        copy = original.clone();
        copy.setPluginProperties(Set.of(new ExtraSettings("two")));
        assertNotEquals(original, copy);
    }

    @Test
    void sourceEqualityIncludesScheduleProcessingAndExtensionSettings() throws Exception {
        SqsReceiverProperties original = new SqsReceiverProperties();
        SqsReceiverProperties copy = roundTrip(original);
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        copy.getPollConnectorProperties().setPollingFrequency(321);
        assertNotEquals(original, copy);
        copy = roundTrip(original);
        copy.getSourceConnectorProperties().setProcessingThreads(5);
        assertNotEquals(original, copy);
        copy = roundTrip(original);
        copy.getPollConnectorProperties().getPollConnectorPropertiesAdvanced().setAllDay(false);
        assertNotEquals(original, copy);
        copy = roundTrip(original);
        copy.setPluginProperties(Set.of(new ExtraSettings("extra")));
        assertNotEquals(original, copy);
    }

    @Test
    void absentLegacyOptionalFieldsNormalizeForEqualityHashAndPurge() throws Exception {
        SqsReceiverProperties legacy = new SqsReceiverProperties();
        legacy.setAuthType(null);
        legacy.setS3EventMode(null);
        legacy.setS3MaxObjectSizeKB(null);
        legacy.setS3FileType(null);
        legacy.setS3Encoding(null);
        SqsReceiverProperties defaults = new SqsReceiverProperties();
        assertEquals(defaults, legacy);
        assertEquals(defaults.hashCode(), legacy.hashCode());
        assertEquals("DEFAULT", legacy.getPurgedProperties().get("authType"));
        assertEquals("10240", legacy.getPurgedProperties().get("s3MaxObjectSizeKB"));

        SqsDispatcherProperties sender = new SqsDispatcherProperties();
        sender.setAuthType(null);
        Field attributes = SqsDispatcherProperties.class.getDeclaredField("messageAttributes");
        attributes.setAccessible(true);
        attributes.set(sender, null);
        sender = roundTrip(sender);
        assertTrue(sender.getMessageAttributes().isEmpty());
        assertEquals(new SqsDispatcherProperties(), sender);
        assertEquals(new SqsDispatcherProperties().hashCode(), sender.hashCode());
        assertEquals("DEFAULT", sender.getPurgedProperties().get("authType"));
    }

    @Test
    void attributesRoundTripAndCloneWithoutSharingRows() throws Exception {
        SqsDispatcherProperties original = new SqsDispatcherProperties();
        original.setMessageAttributes(List.of(new SqsMessageAttribute("tenant", "String", "sensitive"),
                new SqsMessageAttribute("signature", "Binary", "AAE=")));
        SqsDispatcherProperties copy = original.clone();
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertEquals(original, roundTrip(original));
        assertNotSame(original.getMessageAttributes(), copy.getMessageAttributes());
        assertNotSame(original.getMessageAttributes().get(0), copy.getMessageAttributes().get(0));
        copy.getMessageAttributes().get(0).setValue("changed");
        assertEquals("sensitive", original.getMessageAttributes().get(0).getValue());
        assertNotEquals(original, copy);
        assertEquals(2, original.getPurgedProperties().get("messageAttributeCount"));
        assertFalse(original.getPurgedProperties().toString().contains("sensitive"));
        assertFalse(original.getPurgedProperties().toString().contains("tenant"));
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }

    private static final class ExtraSettings extends ConnectorPluginProperties {
        private final String value;
        ExtraSettings(String value) { this.value = value; }
        public String getName() { return "Review test extension"; }
        public ExtraSettings clone() { return new ExtraSettings(value); }
        public Map<String, Object> getPurgedProperties() { return Map.of(); }
        public boolean equals(Object other) { return other instanceof ExtraSettings && Objects.equals(value, ((ExtraSettings) other).value); }
        public int hashCode() { return Objects.hashCode(value); }
    }
}
