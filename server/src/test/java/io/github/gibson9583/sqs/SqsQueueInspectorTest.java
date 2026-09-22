/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import com.mirth.connect.connectors.sqs.SqsConnectorServletInterface;
import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.client.core.api.MirthOperation;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.server.util.TemplateValueReplacer;
import com.mirth.connect.util.ConnectionTestResponse;
import com.mirth.connect.util.ConnectionTestResponse.Type;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

class SqsQueueInspectorTest {
    private static final String URL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo";
    private TemplateValueReplacer replacer;
    private SqsClient client;
    private AwsConnectorCredentials credentials;
    private Inspector inspector;

    class Inspector extends SqsQueueInspector {
        boolean failBuild;
        int credentialCreates;
        Region clientRegion;
        String resolvedKey;
        Inspector() { super(replacer); }
        @Override protected Region defaultRegion() { return Region.US_WEST_2; }
        @Override protected AwsConnectorCredentials createCredentials(AwsConnectorCredentials.AuthType auth,
                String key, String secret, String role, String external, String region) {
            credentialCreates++;
            resolvedKey = key;
            return credentials;
        }
        @Override protected SqsClient buildClient(Region region, AwsConnectorCredentials provider) {
            clientRegion = region;
            if (failBuild) throw new IllegalStateException("client construction failed");
            return client;
        }
    }

    @BeforeEach void setup() {
        replacer = mock(TemplateValueReplacer.class);
        when(replacer.replaceValues(anyString(), nullable(String.class), anyString()))
                .thenAnswer(call -> call.getArgument(0));
        client = mock(SqsClient.class);
        credentials = mock(AwsConnectorCredentials.class);
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
                GetQueueAttributesResponse.builder().attributesWithStrings(Map.of(
                        "QueueArn", "arn:aws:sqs:us-east-1:123456789012:orders.fifo",
                        "FifoQueue", "true", "ContentBasedDeduplication", "true", "VisibilityTimeout", "30"))
                        .build());
        inspector = new Inspector();
    }

    SqsDispatcherProperties sender() {
        SqsDispatcherProperties p = new SqsDispatcherProperties();
        p.setQueueUrl(URL);
        p.setRegion("us-east-1");
        return p;
    }

    @Test void usesOnlyGetQueueAttributesAndClosesResources() {
        SqsDispatcherProperties p = sender();
        p.setTemplate(""); // Inspection does not require a valid message body.
        ConnectionTestResponse result = inspector.inspect("channel", "Channel", p);
        assertEquals(Type.SUCCESS, result.getType());
        assertTrue(result.getMessage().contains("FIFO queue: true"));
        assertTrue(result.getMessage().contains("not tested"));
        ArgumentCaptor<GetQueueAttributesRequest> request = ArgumentCaptor.forClass(GetQueueAttributesRequest.class);
        verify(client).getQueueAttributes(request.capture());
        assertEquals(URL, request.getValue().queueUrl());
        assertEquals(List.of(QueueAttributeName.ALL), request.getValue().attributeNames());
        verify(client).close();
        verifyNoMoreInteractions(client);
        verify(credentials).close();
    }

    @Test void standardQueueUsesAllRatherThanUnsupportedFifoAttributeNames() {
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenAnswer(call -> {
            GetQueueAttributesRequest request = call.getArgument(0);
            if (!request.attributeNames().equals(List.of(QueueAttributeName.ALL))) {
                throw SqsException.builder().statusCode(400).awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("InvalidAttributeName").errorMessage("Unknown Attribute FifoQueue").build()).build();
            }
            return GetQueueAttributesResponse.builder().attributesWithStrings(Map.of(
                    "QueueArn", "arn:aws:sqs:us-east-1:123456789012/orders",
                    "VisibilityTimeout", "30", "Policy", "undisplayed-policy")).build();
        });
        SqsDispatcherProperties properties = sender();
        properties.setQueueUrl(URL.replace(".fifo", ""));
        ConnectionTestResponse result = inspector.inspect("channel", "Channel", properties);
        assertEquals(Type.SUCCESS, result.getType());
        assertTrue(result.getMessage().contains("Default visibility timeout (seconds): 30"));
        assertFalse(result.getMessage().contains("FIFO queue:"));
        assertFalse(result.getMessage().contains("undisplayed-policy"));
        verify(client).getQueueAttributes(any(GetQueueAttributesRequest.class));
        verify(client).close();
        verifyNoMoreInteractions(client);
    }

    @Test void readerUsesSameInspectionAndDefaultRegionChain() {
        SqsReceiverProperties p = new SqsReceiverProperties();
        p.setQueueUrl(URL);
        p.setWaitTimeSeconds("invalid but unrelated to inspection");
        assertEquals(Type.SUCCESS, inspector.inspect("channel", "Channel", p).getType());
        assertEquals(Region.US_WEST_2, inspector.clientRegion);
    }

    @Test void resolvesCurrentUnsavedValuesWithoutMutatingProperties() {
        SqsDispatcherProperties p = sender();
        p.setQueueUrl("${queueUrl}");
        p.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
        p.setAccessKeyId("${key}");
        p.setSecretAccessKey("${secret}");
        when(replacer.replaceValues("${queueUrl}", "channel", "Unsaved name")).thenReturn(URL);
        when(replacer.replaceValues("${key}", "channel", "Unsaved name")).thenReturn("resolved-key");
        when(replacer.replaceValues("${secret}", "channel", "Unsaved name")).thenReturn("resolved-secret");
        assertEquals(Type.SUCCESS, inspector.inspect("channel", "Unsaved name", p).getType());
        assertEquals("resolved-key", inspector.resolvedKey);
        assertEquals("${queueUrl}", p.getQueueUrl());
        assertEquals("${secret}", p.getSecretAccessKey());
    }

    @Test void permissionFailureIsActionableAndDoesNotExposeCredentials() {
        SqsDispatcherProperties p = sender();
        p.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
        p.setAccessKeyId("test-access-key");
        p.setSecretAccessKey("test-secret-key");
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(SqsException.builder()
                .statusCode(403).requestId("request-123").awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDenied").errorMessage("test-secret-key test-access-key").build()).build());
        ConnectionTestResponse result = inspector.inspect("channel", "Channel", p);
        assertEquals(Type.FAILURE, result.getType());
        assertTrue(result.getMessage().contains("sqs:GetQueueAttributes"));
        assertTrue(result.getMessage().contains("request-123"));
        assertFalse(result.getMessage().contains("test-secret-key"));
        assertFalse(result.getMessage().contains("test-access-key"));
        verify(client).close();
        verify(credentials).close();
    }

    @Test void networkTimeoutClosesBothResources() {
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                .thenThrow(ApiCallTimeoutException.create(30_000));
        assertEquals(Type.TIME_OUT, inspector.inspect("channel", "Channel", sender()).getType());
        verify(client).close();
        verify(credentials).close();
    }

    @Test void rejectedStaticCredentialsExplainTheSelectedModeWithoutSuggestingQueuePermission() {
        SqsDispatcherProperties p = sender();
        p.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
        p.setAccessKeyId("synthetic-access");
        p.setSecretAccessKey("synthetic-secret");
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(SqsException.builder()
                .statusCode(403).requestId("authentication-request").awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("InvalidClientTokenId").errorMessage("synthetic-access synthetic-secret").build()).build());
        ConnectionTestResponse result = inspector.inspect("channel", "Channel", p);
        assertEquals(Type.FAILURE, result.getType());
        assertTrue(result.getMessage().contains("InvalidClientTokenId (HTTP 403)"));
        assertTrue(result.getMessage().contains("authentication-request"));
        assertTrue(result.getMessage().contains("Authentication mode: STATIC"));
        assertTrue(result.getMessage().contains("session token"));
        assertFalse(result.getMessage().contains("sqs:GetQueueAttributes"));
        assertFalse(result.getMessage().contains("synthetic-access"));
        assertFalse(result.getMessage().contains("synthetic-secret"));
    }

    @Test void expiredDefaultCredentialsExplainTheServerCredentialSource() {
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(SqsException.builder()
                .statusCode(403).awsErrorDetails(AwsErrorDetails.builder().errorCode("ExpiredToken").build()).build());
        String message = inspector.inspect("channel", "Channel", sender()).getMessage();
        assertTrue(message.contains("Authentication mode: DEFAULT"));
        assertTrue(message.contains("OIE server process"));
        assertTrue(message.contains("refresh expired credentials"));
        assertFalse(message.contains("sqs:GetQueueAttributes"));
    }

    @Test void rejectedStsBaseCredentialsExplainRoleAuthentication() {
        SqsDispatcherProperties p = sender();
        p.setAuthType(SqsDispatcherProperties.AuthType.ROLE);
        p.setRoleArn("arn:aws:iam::123456789012:role/inspection");
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(StsException.builder()
                .statusCode(403).awsErrorDetails(AwsErrorDetails.builder().errorCode("InvalidClientTokenId").build()).build());
        String message = inspector.inspect("channel", "Channel", p).getMessage();
        assertTrue(message.contains("Authentication mode: ROLE"));
        assertTrue(message.contains("base identity for STS"));
        assertFalse(message.contains("sqs:GetQueueAttributes"));
    }

    @Test void deniedStsRoleRequestNamesRolePermissionAndTrustPolicy() {
        SqsDispatcherProperties p = sender();
        p.setAuthType(SqsDispatcherProperties.AuthType.ROLE);
        p.setRoleArn("arn:aws:iam::123456789012:role/inspection");
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(StsException.builder()
                .statusCode(403).requestId("sts-request").awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDenied").errorMessage("Role assumption denied").build()).build());
        String message = inspector.inspect("channel", "Channel", p).getMessage();
        assertTrue(message.contains("sts:AssumeRole"));
        assertTrue(message.contains("trust policy"));
        assertTrue(message.contains("external ID"));
        assertTrue(message.contains("sts-request"));
        assertFalse(message.contains("sqs:GetQueueAttributes"));
    }

    @Test void invalidAttributeDoesNotSuggestChangingQueuePermissions() {
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenThrow(SqsException.builder()
                .statusCode(400).awsErrorDetails(AwsErrorDetails.builder().errorCode("InvalidAttributeName")
                        .errorMessage("Unknown Attribute FifoQueue").build()).build());
        String message = inspector.inspect("channel", "Channel", sender()).getMessage();
        assertTrue(message.contains("InvalidAttributeName (HTTP 400): Unknown Attribute FifoQueue"));
        assertFalse(message.contains("permission"));
        assertFalse(message.contains("credentials"));
    }

    @Test void failureDuringClientConstructionStillClosesCredentials() {
        inspector.failBuild = true;
        assertEquals(Type.FAILURE, inspector.inspect("channel", "Channel", sender()).getType());
        verify(credentials).close();
        verifyNoInteractions(client);
    }

    @Test void blankOrUnresolvedQueueNeverCreatesCredentialsOrCallsAws() {
        for (String queue : List.of("", "${missingQueue}", "https://example.com/", "not a URL")) {
            SqsDispatcherProperties p = sender();
            p.setQueueUrl(queue);
            assertEquals(Type.FAILURE, inspector.inspect("channel", "Channel", p).getType());
        }
        assertEquals(0, inspector.credentialCreates);
        verifyNoInteractions(client, credentials);
    }

    @Test void missingActiveAuthenticationFieldsFailBeforeAws() {
        SqsDispatcherProperties p = sender();
        p.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
        assertEquals(Type.FAILURE, inspector.inspect(null, null, p).getType());
        p.setAuthType(SqsDispatcherProperties.AuthType.ROLE);
        assertEquals(Type.FAILURE, inspector.inspect(null, null, p).getType());
        assertEquals(0, inspector.credentialCreates);
    }

    @Test void unstructuredAwsFailureStillReturnsAResponse() {
        when(client.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                .thenThrow(SqsException.builder().statusCode(502).build());
        assertEquals(Type.FAILURE, inspector.inspect("channel", "Channel", sender()).getType());
    }

    @Test void servletAuthorizesBeforeConstructingInspector() {
        SqsConnectorServlet servlet = mock(SqsConnectorServlet.class, CALLS_REAL_METHODS);
        doThrow(new SecurityException("denied")).when(servlet).checkUserAuthorized("restricted");
        try (MockedConstruction<SqsQueueInspector> constructions = mockConstruction(SqsQueueInspector.class)) {
            assertThrows(SecurityException.class, () -> servlet.inspectQueue("restricted", "Channel", sender()));
            assertTrue(constructions.constructed().isEmpty());
        }
    }

    @Test void servletChecksChannelAndOperationPermissionForCurrentRequest() {
        SqsConnectorServlet servlet = mock(SqsConnectorServlet.class, CALLS_REAL_METHODS);
        doNothing().when(servlet).checkUserAuthorized("allowed");
        try (MockedConstruction<SqsQueueInspector> constructions = mockConstruction(SqsQueueInspector.class,
                (mock, context) -> when(mock.inspect(anyString(), anyString(), any()))
                        .thenReturn(new ConnectionTestResponse(Type.SUCCESS, "ok")))) {
            assertEquals(Type.SUCCESS, servlet.inspectQueue("allowed", "Channel", sender()).getType());
            verify(servlet).checkUserAuthorized("allowed");
            assertEquals(1, constructions.constructed().size());
        }
    }

    @Test void inspectionOperationIsExplicitlyPermissionedAndDoesNotAuditSecrets() throws Exception {
        MirthOperation op = SqsConnectorServletInterface.class.getMethod("inspectQueue", String.class,
                String.class, ConnectorProperties.class).getAnnotation(MirthOperation.class);
        assertEquals(SqsConnectorServletInterface.PERMISSION_INSPECT, op.permission());
        assertFalse(op.auditable());
        assertEquals("inspectSqsQueue", op.name());
        assertEquals(1, new SqsConnectorServicePlugin().getExtensionPermissions().length);
    }
}
