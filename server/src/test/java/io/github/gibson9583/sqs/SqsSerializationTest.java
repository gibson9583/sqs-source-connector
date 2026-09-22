/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsMessageAttribute;
import com.mirth.connect.donkey.util.xstream.XStreamSerializer;

class SqsSerializationTest {
    @Test
    void engineXmlRoundTripPreservesTypedAttributesAndQueueEnvelope() {
        XStreamSerializer serializer = new XStreamSerializer();
        SqsDispatcherProperties original = new SqsDispatcherProperties();
        original.getDestinationConnectorProperties().setQueueEnabled(true);
        original.setMessageAttributes(List.of(new SqsMessageAttribute("kind", "String", "a<&>b"),
                new SqsMessageAttribute("count", "Number", "1.25"),
                new SqsMessageAttribute("bytes", "Binary", "AAH/")));
        String xml = serializer.serialize(original);
        SqsDispatcherProperties decoded = serializer.deserialize(xml, SqsDispatcherProperties.class);
        assertEquals(original, decoded);
        assertEquals(original.hashCode(), decoded.hashCode());
        assertEquals("Binary", decoded.getMessageAttributes().get(2).getDataType());
        assertEquals("AAH/", decoded.getMessageAttributes().get(2).getValue());
    }

    @Test
    void engineXmlWithoutNewAttributeElementLoadsAsEmptyAndCanBeCloned() {
        XStreamSerializer serializer = new XStreamSerializer();
        SqsDispatcherProperties original = new SqsDispatcherProperties();
        String legacyXml = serializer.serialize(original).replaceAll("<messageAttributes\\s*/>", "");
        assertFalse(legacyXml.contains("messageAttributes"));
        SqsDispatcherProperties decoded = serializer.deserialize(legacyXml, SqsDispatcherProperties.class);
        assertTrue(decoded.getMessageAttributes().isEmpty());
        assertEquals(original, decoded);
        assertEquals(decoded, decoded.clone());
    }
}
