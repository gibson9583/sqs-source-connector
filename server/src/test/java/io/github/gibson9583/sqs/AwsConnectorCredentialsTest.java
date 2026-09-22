/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

class AwsConnectorCredentialsTest {
    @Test void roleClosesBothResourcesEvenIfProviderCloseFailsAndDoesNotCloseAgain() {
        AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class,
                withSettings().extraInterfaces(SdkAutoCloseable.class));
        StsClient sts = mock(StsClient.class);
        doThrow(new IllegalStateException("close failed")).when((SdkAutoCloseable) provider).close();
        AwsConnectorCredentials credentials = new AwsConnectorCredentials(provider, sts);
        assertThrows(IllegalStateException.class, credentials::close);
        credentials.close();
        verify((SdkAutoCloseable) provider, times(1)).close();
        verify(sts, times(1)).close();
    }

    @Test void sharedDefaultProviderIsNotClosed() {
        AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class,
                withSettings().extraInterfaces(SdkAutoCloseable.class));
        new AwsConnectorCredentials(provider, null).close();
        verifyNoInteractions(provider);
    }
}
