/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import java.time.Duration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/** Shared request deadlines; long-poll waits (up to 20 seconds) fit within an attempt. */
public final class AwsClientConfiguration {
    private AwsClientConfiguration() {}

    public static ClientOverrideConfiguration standard() {
        return ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofSeconds(60))
                .apiCallTimeout(Duration.ofSeconds(120)).build();
    }

    public static ClientOverrideConfiguration inspection() {
        return ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofSeconds(10))
                .apiCallTimeout(Duration.ofSeconds(30)).build();
    }
}
