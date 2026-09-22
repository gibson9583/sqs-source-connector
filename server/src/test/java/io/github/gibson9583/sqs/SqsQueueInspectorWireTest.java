/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ConnectionTestResponse;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Real SDK signing, request marshalling, and XML parsing with an in-memory HTTP boundary. */
class SqsQueueInspectorWireTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void inspectsStandardAndFifoQueuesThroughTheActualSdkProtocol(boolean fifo) {
        TemplateValueReplacer replacer = mock(TemplateValueReplacer.class);
        when(replacer.replaceValues(anyString(), anyString(), anyString()))
                .thenAnswer(call -> call.getArgument(0));
        List<String> actions = new ArrayList<>();
        List<List<String>> attributeRequests = new ArrayList<>();
        SdkHttpClient transport = new SdkHttpClient() {
            @Override public void close() {}
            @Override public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
                return new ExecutableHttpRequest() {
                    @Override public void abort() {}
                    @Override public HttpExecuteResponse call() throws IOException {
                        String body;
                        try (var input = request.contentStreamProvider().orElseThrow().newStream()) {
                            body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                        }
                        List<String> attributes = new ArrayList<>();
                        for (String field : body.split("&")) {
                            String[] pair = field.split("=", 2);
                            String name = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
                            String value = pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
                            if (name.equals("Action")) actions.add(value);
                            if (name.startsWith("AttributeName.")) attributes.add(value);
                        }
                        attributeRequests.add(attributes);
                        // The observed standard-queue failure: explicit FIFO-only attributes are invalid.
                        boolean unsupported = !fifo && (attributes.contains("FifoQueue")
                                || attributes.contains("ContentBasedDeduplication"));
                        String xml = unsupported
                                ? "<ErrorResponse><Error><Type>Sender</Type><Code>InvalidAttributeName</Code>"
                                  + "<Message>Unknown Attribute FifoQueue.</Message></Error>"
                                  + "<RequestId>fixture-rejected</RequestId></ErrorResponse>"
                                : "<GetQueueAttributesResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">"
                                  + "<GetQueueAttributesResult>"
                                  + "<Attribute><Name>VisibilityTimeout</Name><Value>30</Value></Attribute>"
                                  + "<Attribute><Name>Policy</Name><Value>policy-not-for-display</Value></Attribute>"
                                  + "<Attribute><Name>FutureAttribute</Name><Value>future-not-for-display</Value></Attribute>"
                                  + (fifo ? "<Attribute><Name>FifoQueue</Name><Value>true</Value></Attribute>"
                                          + "<Attribute><Name>ContentBasedDeduplication</Name><Value>true</Value></Attribute>" : "")
                                  + "</GetQueueAttributesResult><ResponseMetadata><RequestId>fixture-success</RequestId>"
                                  + "</ResponseMetadata></GetQueueAttributesResponse>";
                        return HttpExecuteResponse.builder()
                                .response(SdkHttpResponse.builder().statusCode(unsupported ? 400 : 200)
                                        .putHeader("Content-Type", "text/xml").build())
                                .responseBody(AbortableInputStream.create(new ByteArrayInputStream(
                                        xml.getBytes(StandardCharsets.UTF_8)))).build();
                    }
                };
            }
        };
        SqsQueueInspector inspector = new SqsQueueInspector(replacer) {
            @Override protected SqsClient buildClient(Region region, AwsConnectorCredentials credentials) {
                return SqsClient.builder().region(region).credentialsProvider(credentials.getProvider())
                        .overrideConfiguration(AwsClientConfiguration.inspection()).httpClient(transport).build();
            }
        };
        SqsDispatcherProperties properties = new SqsDispatcherProperties();
        properties.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
        properties.setAccessKeyId("fixture-access-key");
        properties.setSecretAccessKey("fixture-secret-key");
        properties.setRegion("us-east-1");
        properties.setQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/orders" + (fifo ? ".fifo" : ""));
        ConnectionTestResponse result = inspector.inspect("channel", "Channel", properties);
        assertEquals(ConnectionTestResponse.Type.SUCCESS, result.getType(), result.getMessage());
        assertEquals(List.of("GetQueueAttributes"), actions);
        assertEquals(List.of(List.of("All")), attributeRequests);
        assertTrue(result.getMessage().contains("Default visibility timeout (seconds): 30"));
        assertEquals(fifo, result.getMessage().contains("FIFO queue: true"));
        assertFalse(result.getMessage().contains("policy-not-for-display"));
        assertFalse(result.getMessage().contains("future-not-for-display"));
    }
}
