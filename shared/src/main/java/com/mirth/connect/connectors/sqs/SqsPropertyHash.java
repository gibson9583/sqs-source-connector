/*
 * SPDX-License-Identifier: MPL-2.0
 */
package com.mirth.connect.connectors.sqs;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import com.mirth.connect.donkey.model.channel.CronProperty;
import com.mirth.connect.donkey.model.channel.DestinationConnectorProperties;
import com.mirth.connect.donkey.model.channel.PollConnectorProperties;
import com.mirth.connect.donkey.model.channel.PollConnectorPropertiesAdvanced;
import com.mirth.connect.donkey.model.channel.SourceConnectorProperties;

/** Engine settings implement value equality but do not override Object.hashCode(). */
final class SqsPropertyHash {
    private SqsPropertyHash() {}

    static int destination(DestinationConnectorProperties p) {
        return p == null ? 0 : Objects.hash(p.isQueueEnabled(), p.isSendFirst(), p.getRetryIntervalMillis(),
                p.isRegenerateTemplate(), p.getRetryCount(), p.isRotate(), p.isIncludeFilterTransformer(),
                p.getThreadCount(), p.getThreadAssignmentVariable(), p.isValidateResponse(), p.getResourceIds(),
                p.getQueueBufferSize(), p.isReattachAttachments(), plugins(p.getPluginProperties()));
    }

    static int source(SourceConnectorProperties p) {
        return p == null ? 0 : Objects.hash(p.getResponseVariable(), p.isRespondAfterProcessing(),
                p.isProcessBatch(), p.isFirstResponse(), p.getProcessingThreads(), p.getResourceIds(),
                p.getQueueBufferSize());
    }

    static int polling(PollConnectorProperties p) {
        if (p == null) return 0;
        int cronHash = 1;
        if (p.getCronJobs() != null) {
            for (CronProperty cron : p.getCronJobs()) {
                cronHash = 31 * cronHash + (cron == null ? 0 : Objects.hash(cron.getDescription(), cron.getExpression()));
            }
        }
        PollConnectorPropertiesAdvanced a = p.getPollConnectorPropertiesAdvanced();
        int advancedHash = a == null ? 0 : Objects.hash(a.isWeekly(), Arrays.hashCode(a.getInactiveDays()),
                a.getDayOfMonth(), a.isAllDay(), a.getStartingHour(), a.getStartingMinute(),
                a.getEndingHour(), a.getEndingMinute());
        return Objects.hash(p.getPollingType(), p.isPollOnStart(), p.getPollingHour(), p.getPollingMinute(),
                p.getPollingFrequency(), cronHash, advancedHash);
    }

    static int plugins(Set<?> properties) {
        // Extension property implementations may also lack hashCode. Equal sets must have
        // equal sizes; retaining this conservative hash avoids depending on identity hashes.
        return properties == null ? 0 : properties.size();
    }
}
