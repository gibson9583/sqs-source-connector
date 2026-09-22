# AWS SQS Connector

Integrate [Amazon SQS](https://aws.amazon.com/sqs/) with Open Integration Engine.
This extension ships **two** connectors:

- **SQS Reader** — a channel **source** that polls an SQS queue and turns each
  message into an engine message.
- **SQS Sender** — a channel **destination** that publishes messages to an SQS queue.

![SQS Connector settings](sqs-connector-plugin.png)

## Features

- Standard and FIFO queues
- Long polling with a configurable wait time
- Batch receive (up to 10 messages per poll)
- Typed outbound message attributes (String, Number, Binary), with replacement variables
- Incoming attributes and data types surfaced to the channel
- Read-only queue inspection in Swing and Web Administrator
- S3 notifications with bounded, version-aware object fetching
- Text batch splitting through the engine batch adaptor
- Authentication via the default AWS credential chain (IAM role, profile, or static keys)
- Built on AWS SDK for Java v2

## Requirements

- Community Store / Web Administrator: Open Integration Engine **4.6.0** or newer
- Server / Swing extension descriptors also declare **4.5.2** compatibility
- Network access from the engine host to the SQS endpoint (or a VPC endpoint)
- Reader: `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:GetQueueAttributes`
- Sender: `sqs:SendMessage`
- Inspect Queue: `sqs:GetQueueAttributes` on the supplied queue; no list/discovery permissions
- Assume Role and S3/KMS configurations require their corresponding permissions

## Installing

Install from the Community Store, then **restart the engine** to activate the
connectors. Afterward the **SQS Reader** appears in the source connector list and
**SQS Sender** in the destination list of the channel editor.

## Configuration

Set the queue URL and region on the connector, choose your polling/visibility
options, and choose the default AWS credential chain, static keys, or Assume Role.
Use **Inspect Queue** to check the current connection settings without consuming
messages. Restricted engine roles need the **Inspect SQS Queue** permission.
In the sender, add up to ten **Message Attributes** with type and value; Binary
values use Base64 input. Configuration-map values use top-level `${key}` syntax. See the [project README](https://github.com/gibson9583/sqs-source-connector#readme)
for the full field reference.

## Support

Report issues at
[github.com/gibson9583/sqs-source-connector/issues](https://github.com/gibson9583/sqs-source-connector/issues).
