/*
 * SQS Connector — web admin plugin (React).
 *
 * Companion UI for the AWS SQS engine connector
 * (https://github.com/gibson9583/sqs-source-connector). The engine half ships
 * source.xml / destination.xml descriptors whose <name> values are the
 * transport names used below; defaults() mirrors the Java constructors of
 *   com.mirth.connect.connectors.sqs.SqsReceiverProperties
 *   com.mirth.connect.connectors.sqs.SqsDispatcherProperties
 * field for field (numeric fields are Strings on purpose — the engine resolves
 * Velocity expressions like ${sqs.queueUrl} before parsing them).
 *
 * The web admin half of a connector is only a property panel. The framework is
 * imported from @oie/web-ui — the React connector-panel helpers (ConnectorForm,
 * PollSection, the default property shapes). Even though this plugin is served
 * from inside the engine extension zip, the host page's import map resolves
 * @oie/web-ui (and @oie/web-shell) to the shell's loaded copy at runtime, so
 * there is no bundling and one shared framework instance.
 *
 * This is the React source. It is compiled to web/plugin.js (the file
 * plugin.json points at) by web/build.mjs — see that file. Connector panels now
 * register { defaults, component } where component({ properties, onChange })
 * returns JSX, replacing the old { defaults, render(host, ctx) }. Field schemas
 * and defaults are reused VERBATIM; only the rendering layer became React.
 */

import { platform } from '@oie/web-shell';
const React = platform.React;

import {
    ConnectorForm, PollSection,
    defaultPollProperties, defaultSourceProperties, defaultDestinationProperties,
    CHARSETS
} from '@oie/web-ui';

const AUTH_TYPES = [
    { value: 'DEFAULT', label: 'Default Provider Chain' },
    { value: 'STATIC', label: 'Access Key / Secret Key' },
    { value: 'ROLE', label: 'Assume IAM Role (STS)' }
];

/* Shared AWS connection/auth rows (both properties classes declare the same
   queueUrl/region/authType/accessKeyId/secretAccessKey/roleArn/externalId). */
function awsFields() {
    return [
        { section: 'SQS Queue' },
        {
            key: 'queueUrl', label: 'Queue URL', width: '420px',
            placeholder: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue'
        },
        {
            key: 'region', label: 'Region', width: '160px', placeholder: 'us-east-1',
            hint: 'Leave blank to use the region from the default provider chain'
        },
        { section: 'Authentication' },
        { key: 'authType', label: 'Auth Type', type: 'select', options: AUTH_TYPES, refresh: true },
        { key: 'accessKeyId', label: 'Access Key Id', visible: (p) => p.authType === 'STATIC' },
        { key: 'secretAccessKey', label: 'Secret Access Key', visible: (p) => p.authType === 'STATIC' },
        { key: 'roleArn', label: 'Role ARN', visible: (p) => p.authType === 'ROLE' },
        {
            key: 'externalId', label: 'External Id', visible: (p) => p.authType === 'ROLE',
            hint: 'Optional STS external id for the AssumeRole call'
        }
    ];
}

/* ---- SQS Reader (source) ------------------------------------------------- */

const S3_EVENT_MODES = [
    { value: 'DISABLED', label: 'Disabled' },
    { value: 'EXTRACT_DETAILS', label: 'Extract Details to Source Map' },
    { value: 'FETCH_OBJECT', label: 'Fetch S3 Object as Message' }
];

const sqsReader = {
    defaults(version) {
        return {
            '@class': 'com.mirth.connect.connectors.sqs.SqsReceiverProperties',
            '@version': version,
            pluginProperties: null,
            pollConnectorProperties: defaultPollProperties(version),
            sourceConnectorProperties: defaultSourceProperties(version),
            queueUrl: '',
            region: '',
            authType: 'DEFAULT',
            accessKeyId: '',
            secretAccessKey: '',
            roleArn: '',
            externalId: '',
            waitTimeSeconds: '20',
            maxMessages: '10',
            visibilityTimeout: '30',
            includeAttributes: true,
            messageGroupHandling: false,
            s3EventMode: 'DISABLED',
            s3MaxObjectSizeKB: '10240',
            s3FileType: 'Text',
            s3Encoding: 'DEFAULT_ENCODING'
        };
    },
    component({ properties, onChange }) {
        return (
            <div>
                <ConnectorForm properties={properties} onChange={onChange} fields={[
                    ...awsFields(),
                    { section: 'Receive Settings' },
                    {
                        key: 'waitTimeSeconds', label: 'Long Poll Wait (s)', width: '110px',
                        hint: '0–20; 20 enables SQS long polling'
                    },
                    { key: 'maxMessages', label: 'Max Messages / Receive', width: '110px', hint: '1–10' },
                    { key: 'visibilityTimeout', label: 'Visibility Timeout (s)', width: '110px' },
                    {
                        key: 'includeAttributes', label: 'Include Attributes', type: 'checkbox',
                        checkLabel: 'Add SQS message attributes to the source map'
                    },
                    {
                        key: 'messageGroupHandling', label: 'FIFO Message Groups', type: 'checkbox',
                        checkLabel: 'Preserve message group ordering (FIFO queues)'
                    },
                    { section: 'S3 Event Notifications' },
                    { key: 's3EventMode', label: 'S3 Event Mode', type: 'select', width: '260px', options: S3_EVENT_MODES, refresh: true },
                    {
                        key: 's3MaxObjectSizeKB', label: 'Max Object Size (KB)', width: '110px',
                        visible: (p) => p.s3EventMode === 'FETCH_OBJECT'
                    },
                    {
                        key: 's3FileType', label: 'File Type', type: 'select', options: ['Text', 'Binary'], refresh: true,
                        visible: (p) => p.s3EventMode === 'FETCH_OBJECT'
                    },
                    {
                        key: 's3Encoding', label: 'Encoding', type: 'select', options: CHARSETS,
                        visible: (p) => p.s3EventMode === 'FETCH_OBJECT' && p.s3FileType !== 'Binary'
                    }
                ]} />
                <PollSection properties={properties} onChange={onChange} />
            </div>
        );
    }
};

/* ---- SQS Sender (destination) --------------------------------------------- */

const sqsSender = {
    defaults(version) {
        return {
            '@class': 'com.mirth.connect.connectors.sqs.SqsDispatcherProperties',
            '@version': version,
            pluginProperties: null,
            destinationConnectorProperties: defaultDestinationProperties(version),
            queueUrl: '',
            region: '',
            authType: 'DEFAULT',
            accessKeyId: '',
            secretAccessKey: '',
            roleArn: '',
            externalId: '',
            template: '${message.encodedData}',
            delaySeconds: '',
            messageGroupId: '',
            messageDeduplicationId: ''
        };
    },
    component({ properties, onChange }) {
        return (
            <ConnectorForm properties={properties} onChange={onChange} fields={[
                ...awsFields(),
                { section: 'Send Settings' },
                { key: 'delaySeconds', label: 'Delay (s)', width: '110px', hint: 'Optional per-message delay (0–900); blank for none' },
                { key: 'messageGroupId', label: 'Message Group Id', hint: 'Required for FIFO queues' },
                { key: 'messageDeduplicationId', label: 'Deduplication Id', hint: 'Optional; FIFO queues without content-based deduplication' },
                { key: 'template', label: 'Template', type: 'code', minHeight: '160px' }
            ]} />
        );
    }
};

export function register(platform) {
    // Transport names must match the engine descriptors' <name> values
    // (source.xml → "SQS Reader", destination.xml → "SQS Sender").
    platform.registerConnectorPanel('SQS Reader', 'SOURCE', sqsReader);
    platform.registerConnectorPanel('SQS Sender', 'DESTINATION', sqsSender);
}
