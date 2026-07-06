# OIE SQS Connector

An Open Integration Engine (OIE) connector plugin for AWS SQS, providing both a
source connector (SQS Reader) that polls queues for messages and a destination
connector (SQS Sender) that sends messages to queues.

![SQS Connector Settings](docs/sqs-connector-plugin.png)

## Web Administrator support

This plugin ships UI for **all three** OIE surfaces:

- **Server**: `server/` (SqsReceiver / SqsDispatcher)
- **Swing Administrator**: `client/` (SqsReceiverPanel / SqsSenderPanel)
- **Web Administrator**: `package/webadmin/` — a web admin plugin
  (`plugin.json` + `web/plugin.js`) bundled into the extension zip under
  `sqs-connector/webadmin/`. It registers the "SQS Reader" / "SQS Sender"
  connector panels using the web administrator's plugin API.

The web administrator discovers it automatically when its plugin search path
includes the engine's extensions directory — set
`WEBADMIN_PLUGIN_DIRS=/path/to/oie/extensions` (or `"pluginDirs"` in the web
administrator's `config.json`). No separate install step: installing this
extension on the engine makes it available to both administrators.

Note: `package/webadmin/` is copied into the zip **without** Maven resource
filtering (see the `copy-webadmin` execution in `package/pom.xml`) so Velocity
tokens like `${message.encodedData}` in the JavaScript survive the build.

## Features

### SQS Reader (source)
- Long polling with configurable wait time, max messages, and visibility timeout
- All AWS authentication methods: Default Credential Chain, Static Credentials, Assume Role (STS)
- Standard and FIFO queue support with message group handling
- S3 event notification support (EventBridge and standard S3 notification formats, with SNS envelope auto-detection)
- Fetch S3 objects directly as message content with configurable file type (Text/Binary) and encoding
- S3 object metadata and user-defined metadata added to source map
- SQS message attributes and system attributes in source map
- All text fields support replacement variables (Velocity expressions)
- Delete retry with configurable attempts on transient failures

### SQS Sender (destination)
- Send messages to Standard and FIFO queues
- All AWS authentication methods: Default Credential Chain, Static Credentials, Assume Role (STS)
- Configurable message body template (defaults to `${message.encodedData}`)
- FIFO message group ID and deduplication ID support
- Optional delivery delay for standard queues
- Per-message replacement variables for queue URL, body, delay, and FIFO IDs
- Works with OIE destination queueing/retry settings (failed sends are queued)

## Requirements

- OIE 4.5.2+
- Java 17+

## Building

Requires OIE libraries in your Maven repository.

```bash
mvn clean package
```

The plugin zip will be in `package/target/sqs-connector-0.1.0.zip`.

## Installation

Install using the Extensions manager in the OIE Administrator, or manually extract
to the `extensions` directory. A restart is required after installation.

## Configuration — SQS Reader (source)

### AWS Connection
- **Queue URL** (required): Full SQS queue URL
- **Region** (optional): AWS region. If blank, uses the default region from the AWS credential provider chain

### Authentication
- **Default Credential Chain**: Uses environment variables, instance profile, ECS task role, `~/.aws/credentials`
- **Static Credentials**: Explicit AWS Access Key ID and Secret Access Key
- **Assume Role (STS)**: Assume an IAM role with optional external ID

### SQS Settings
- **Long Poll Wait Time**: 0-20 seconds (higher values reduce API costs)
- **Max Messages Per Poll**: 1-10 messages per request
- **Visibility Timeout**: 0-43200 seconds (should exceed expected processing time)

### Message Handling
- **Include SQS message attributes**: Adds user-defined and system attributes to the source map
- **FIFO queue message group handling**: Includes MessageGroupId and SequenceNumber in source map

### S3 Event Notifications
- **Disabled**: Treat SQS message body as-is
- **Extract Details**: Parse S3 event JSON, add bucket/key/event details to source map, keep original SQS body
- **Fetch Object**: Parse S3 event JSON, add details to source map, and replace message body with the fetched S3 object content
  - **Max Object Size (KB)**: Objects larger than this are skipped (0 = no limit)
  - **File Type**: Text (decoded to string) or Binary (raw bytes)
  - **Encoding**: Fallback encoding when the S3 object's Content-Type header does not specify a charset. Content-Type charset is always tried first.

## Configuration — SQS Sender (destination)

### AWS Connection
- **Queue URL** (required): Full SQS queue URL — supports per-message replacement variables
- **Region** (optional): AWS region. If blank, uses the default region from the AWS credential provider chain

### Authentication
Same options as the SQS Reader: Default Credential Chain, Static Credentials, or Assume Role (STS).

### Send Settings
- **Delay Seconds** (optional): Delivery delay 0-900 seconds. Leave blank to use the queue default. Not supported on FIFO queues
- **FIFO Message Group ID**: Required for FIFO queues, ignored for standard queues
- **FIFO Deduplication ID** (optional): Leave blank if the queue uses content-based deduplication

### Message Body Template
The SQS message body to send. Defaults to `${message.encodedData}` (the transformed
channel message). Any replacement variables are resolved per message.

The SQS `MessageId` returned by AWS is stored as the destination response data.

## Source Map Variables

### SQS Variables
| Key | Description |
|-----|-------------|
| `sqsMessageId` | SQS message ID |
| `sqsReceiptHandle` | Receipt handle for message deletion |
| `sqsMD5OfBody` | MD5 hash of the message body |
| `sqsAttr*` | System attributes (e.g. `sqsAttrSentTimestamp`) |
| `sqsMessageGroupId` | FIFO queue message group ID |
| `sqsSequenceNumber` | FIFO queue sequence number |
| `sqsMsgAttr*` | User-defined message attributes (e.g. `sqsMsgAttrMyKey`) |

### S3 Event Variables
| Key | Description |
|-----|-------------|
| `s3EventName` | Event name (e.g. `ObjectCreated:Put`) |
| `s3EventFormat` | `EventBridge` or `S3Notification` |
| `s3BucketName` | S3 bucket name |
| `s3BucketArn` | S3 bucket ARN |
| `s3ObjectKey` | S3 object key (URL-decoded) |
| `s3ObjectSize` | Object size in bytes |
| `s3ObjectETag` | Object ETag |
| `s3ObjectVersionId` | Object version ID (if versioned) |
| `s3Region` | AWS region from the event |

### S3 Object Metadata (Fetch Object mode)
| Key | Description |
|-----|-------------|
| `s3ContentType` | Content-Type header |
| `s3ContentLength` | Content length in bytes |
| `s3ContentEncoding` | Content encoding |
| `s3LastModified` | Last modified timestamp |
| `s3StorageClass` | Storage class |
| `s3ServerSideEncryption` | Server-side encryption type |
| `s3CacheControl` | Cache-Control header |
| `s3ContentDisposition` | Content-Disposition header |

User-defined S3 object metadata (`x-amz-meta-*` headers) is added to the source map using the original key names.

## License

MIT License
