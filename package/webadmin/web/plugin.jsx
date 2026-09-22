/* SPDX-License-Identifier: MPL-2.0 */
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
const canInspect = () => platform.checkTask('channel', 'doInspectSqsQueue');

import {
    ConnectorForm, PollSection,
    defaultPollProperties, defaultSourceProperties, defaultDestinationProperties,
    CHARSETS, postConnectorProperties
} from '@oie/web-ui';
import { attributeEntries, writeAttributes, validateProperties, validateConnection, createInspectionController } from './ui-model.mjs';
export { attributeEntries, writeAttributes, validateProperties, validateConnection, createInspectionController } from './ui-model.mjs';

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
            tooltip: 'Leave blank to use the region from the default provider chain'
        },
        { section: 'Authentication' },
        { key: 'authType', label: 'Auth Type', type: 'select', options: AUTH_TYPES, refresh: true },
        { key: 'accessKeyId', label: 'Access Key Id', visible: (p) => p.authType === 'STATIC' },
        { key: 'secretAccessKey', label: 'Secret Access Key', type: 'password', visible: (p) => p.authType === 'STATIC' },
        { key: 'roleArn', label: 'Role ARN', visible: (p) => p.authType === 'ROLE' },
        {
            key: 'externalId', label: 'External Id', visible: (p) => p.authType === 'ROLE',
            tooltip: 'Optional STS external id for the AssumeRole call'
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
    validate(properties) { return validateProperties(properties, true); },
    component({ properties, channel, connector, onChange }) {
        const inspection = useInspection(properties, channel, connector, onChange);
        onChange = inspection.onChange;
        return (
            <div>
                <ConnectorForm properties={properties} onChange={onChange} fields={[
                    ...awsFields(),
                    { section: 'Receive Settings' },
                    {
                        key: 'waitTimeSeconds', label: 'Long Poll Wait (s)', width: '110px',
                        tooltip: '0–20; 20 enables SQS long polling'
                    },
                    { key: 'maxMessages', label: 'Max Messages / Receive', width: '110px', tooltip: '1–10' },
                    { key: 'visibilityTimeout', label: 'Visibility Timeout (s)', width: '110px' },
                    {
                        key: 'includeAttributes', label: 'Include Attributes', type: 'checkbox',
                        checkLabel: 'Add SQS message attributes to the source map'
                    },
                    {
                        key: 'messageGroupHandling', label: 'FIFO Message Groups', type: 'checkbox',
                        checkLabel: 'Process FIFO source messages in order (Source Queue OFF, one processing thread)',
                        tooltip: 'Wait for source processing before receiving the next message. Queued destinations can finish later.'
                    },
                    { section: 'S3 Event Notifications' },
                    { key: 's3EventMode', label: 'S3 Event Mode', type: 'select', width: '260px', options: S3_EVENT_MODES, refresh: true },
                    {
                        key: 's3MaxObjectSizeKB', label: 'Max Object Size (KB)', width: '110px',
                        tooltip: 'Blank or 0 means no limit. The default is 10240 KB. Oversized objects retain the event JSON with an OVERSIZED status.',
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
                <QueueInspector inspection={inspection} properties={properties} />
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
            messageDeduplicationId: '',
            messageAttributes: writeAttributes([])
        };
    },
    validate(properties) { return validateProperties(properties, false); },
    component({ properties, channel, connector, onChange }) {
        const inspection = useInspection(properties, channel, connector, onChange);
        onChange = inspection.onChange;
        return (
            <div>
            <ConnectorForm properties={properties} onChange={onChange} fields={[
                ...awsFields(),
                { section: 'Send Settings' },
                { key: 'delaySeconds', label: 'Delay (s)', width: '110px', tooltip: 'Optional per-message delay (0–900); blank uses the queue default. Not supported on FIFO queues.' },
                { key: 'messageGroupId', label: 'Message Group Id', tooltip: 'Required for FIFO queues; optional fair-queue tenant grouping for standard queues.' },
                { key: 'messageDeduplicationId', label: 'Deduplication Id', tooltip: 'Optional; FIFO queues without content-based deduplication' },
                { key: 'template', label: 'Template', type: 'code', minHeight: '160px' }
            ]} />
            <MessageAttributes properties={properties} onChange={onChange} />
            <QueueInspector inspection={inspection} properties={properties} />
            </div>
        );
    }
};


function useInspection(properties, channel, connector, onChange) {
    const [state, setState] = React.useState({ pending: false, message: '', error: false });
    const [, redraw] = React.useReducer(n => n + 1, 0);
    const propertySnapshot = JSON.stringify(properties);
    const context = React.useRef();
    context.current = { properties, channel, connector };
    const controller = React.useRef();
    if (!controller.current) controller.current = createInspectionController(
        () => context.current, setState,
        (snapshot, target) => postConnectorProperties('/connectors/sqs/_inspectQueue', snapshot, target), () => 'Queue inspection failed. Check the engine connection and server logs.', canInspect);
    React.useEffect(() => {
        controller.current.invalidate();
    }, [properties, channel, connector, propertySnapshot, channel?.id, channel?.name]);
    React.useEffect(() => {
        controller.current.activate();
        return () => controller.current.dispose();
    }, []);
    return { state, inspect: () => controller.current.inspect(), onChange() {
        controller.current.invalidate(); onChange(); redraw();
    } };
}

function QueueInspector({ inspection, properties }) {
    if (!canInspect()) return null;
    return <div className="cform-section" style={{ marginTop: '16px', minWidth: 0 }}>
        <div className="cform-section-title">Queue Inspection</div>
        <div className="cform-control">
            <button type="button" className="btn" disabled={inspection.state.pending || validateConnection(properties).length > 0}
                onClick={inspection.inspect}>{inspection.state.pending ? 'Inspecting queue…' : 'Inspect Queue'}</button>
            <p className="hint" style={{ margin: 0 }}>Reads the supplied queue's attributes. Does not receive, send, or delete messages.</p>
        </div>
        {inspection.state.message && <pre role={inspection.state.error ? 'alert' : 'status'} aria-live="polite"
            style={{ margin: '10px 0 0', padding: '10px 12px', whiteSpace: 'pre-wrap', overflowWrap: 'anywhere',
                maxWidth: '100%', boxSizing: 'border-box', border: '1px solid var(--line)', borderRadius: '6px',
                background: 'var(--bg1)', fontFamily: 'var(--font-mono)', fontSize: '11px', lineHeight: 1.5 }}>
            {inspection.state.message}
        </pre>}
    </div>;
}

function MessageAttributes({ properties, onChange }) {
    const rows = attributeEntries(properties.messageAttributes);
    const change = next => { properties.messageAttributes = writeAttributes(next); onChange(); };
    const update = (index, key, value) => change(rows.map((row, i) => i === index ? { ...row, [key]: value } : row));
    return <div className="cform-section" data-fkey="messageAttributes" style={{ marginTop: '16px' }}>
        <div className="cform-section-title">Message Attributes</div>
        <p>Up to 10 attributes. Names and values support replacement variables such as {'${tenantId}'}. Binary values use Base64.</p>
        <table><thead><tr><th>Name</th><th>Data Type</th><th>Value</th><th /></tr></thead><tbody>
            {rows.map((row, index) => <tr key={index}>
                <td><input aria-label={`Attribute ${index + 1} name`} value={row.name} onChange={e => update(index, 'name', e.target.value)} /></td>
                <td><select aria-label={`Attribute ${index + 1} type`} value={row.dataType} onChange={e => update(index, 'dataType', e.target.value)}>
                    {['String', 'Number', 'Binary'].map(type => <option key={type}>{type}</option>)}
                </select></td>
                <td><input aria-label={`Attribute ${index + 1} value`} value={row.value} onChange={e => update(index, 'value', e.target.value)} /></td>
                <td><button type="button" className="btn" aria-label={`Remove attribute ${index + 1}`} onClick={() => change(rows.filter((_, i) => i !== index))}>Remove</button></td>
            </tr>)}
        </tbody></table>
        <button type="button" className="btn" disabled={rows.length >= 10} onClick={() => change([...rows, { name: '', dataType: 'String', value: '' }])}>Add Attribute</button>
    </div>;
}

export function register(platform) {
    // Transport names must match the engine descriptors' <name> values
    // (source.xml → "SQS Reader", destination.xml → "SQS Sender").
    platform.registerConnectorPanel('SQS Reader', 'SOURCE', sqsReader);
    platform.registerConnectorPanel('SQS Sender', 'DESTINATION', sqsSender);
}
