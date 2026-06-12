/*
 * SPDX-License-Identifier: MIT
 */
package io.github.gibson9583.sqs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Builds an AWS credentials provider from connector auth settings and owns
 * the closeable resources behind it.
 * <p>
 * The AWS SDK does not close user-supplied credential providers when a
 * service client is closed, so for ROLE auth the STS client and the
 * assume-role provider must be closed explicitly via {@link #close()}.
 * The provider may be shared by multiple service clients (e.g. SQS and S3).
 */
public class AwsConnectorCredentials implements AutoCloseable {

    public enum AuthType {
        /** Use AWS default credential provider chain (env vars, instance profile, etc.) */
        DEFAULT,
        /** Use explicit access key and secret key */
        STATIC,
        /** Assume an IAM role via STS */
        ROLE
    }

    private final AwsCredentialsProvider provider;
    private final StsClient stsClient;

    private AwsConnectorCredentials(AwsCredentialsProvider provider, StsClient stsClient) {
        this.provider = provider;
        this.stsClient = stsClient;
    }

    /**
     * Creates a credentials provider for the given auth settings. All values
     * must already be resolved (post-Velocity substitution).
     */
    public static AwsConnectorCredentials create(AuthType authType, String accessKeyId,
            String secretAccessKey, String roleArn, String externalId, String region) {
        switch (authType) {
            case STATIC:
                return new AwsConnectorCredentials(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)), null);

            case ROLE:
                AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName("oie-sqs-connector");

                if (externalId != null && !externalId.isBlank()) {
                    roleRequestBuilder.externalId(externalId);
                }

                StsClient stsClient = region != null && !region.isBlank()
                        ? StsClient.builder().region(Region.of(region)).build()
                        : StsClient.builder().build();

                AwsCredentialsProvider provider = StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(roleRequestBuilder.build())
                        .build();

                return new AwsConnectorCredentials(provider, stsClient);

            case DEFAULT:
            default:
                return new AwsConnectorCredentials(DefaultCredentialsProvider.create(), null);
        }
    }

    public AwsCredentialsProvider getProvider() {
        return provider;
    }

    /**
     * Closes the STS client and assume-role provider for ROLE auth.
     * DEFAULT and STATIC providers hold no resources; in particular the
     * DefaultCredentialsProvider is a JVM-wide singleton and must never
     * be closed here.
     */
    @Override
    public void close() {
        if (stsClient != null) {
            if (provider instanceof SdkAutoCloseable) {
                ((SdkAutoCloseable) provider).close();
            }
            stsClient.close();
        }
    }
}
