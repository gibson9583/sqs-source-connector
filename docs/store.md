# AWS SQS Connector

Integrate [Amazon SQS](https://aws.amazon.com/sqs/) with Open Integration Engine.
This extension ships **two** connectors:

- **SQS Reader** — a channel **source** that polls an SQS queue and turns each
  message into an engine message.
- **SQS Sender** — a channel **destination** that publishes messages to an SQS queue.

![SQS Connector settings](docs/sqs-connector-plugin.png)

## Features

- Standard and FIFO queues
- Long polling with a configurable wait time
- Batch receive (up to 10 messages per poll)
- Message attributes surfaced to the channel
- Authentication via the default AWS credential chain (IAM role, profile, or static keys)
- Built on AWS SDK for Java v2

## Requirements

- Open Integration Engine **4.5.2** or newer
- Network access from the engine host to the SQS endpoint (or a VPC endpoint)
- AWS credentials with the appropriate `sqs:*` permissions for the target queue

## Installing

Install from the Community Store, then **restart the engine** to activate the
connectors. Afterward the **SQS Reader** appears in the source connector list and
**SQS Sender** in the destination list of the channel editor.

## Configuration

Set the queue URL and region on the connector, choose your polling/visibility
options, and configure credentials through the standard AWS credential chain on the
engine host. See the [project README](https://github.com/gibson9583/sqs-source-connector#readme)
for the full field reference.

## Support

Report issues at
[github.com/gibson9583/sqs-source-connector/issues](https://github.com/gibson9583/sqs-source-connector/issues).
