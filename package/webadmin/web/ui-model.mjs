/* SPDX-License-Identifier: MPL-2.0 */
/* Pure wire/validation/request-lifecycle helpers shared by the SQS panels. */
export const ATTRIBUTE_CLASS = 'com.mirth.connect.connectors.sqs.SqsMessageAttribute';
export const hasExpression = value => /\$!?\{[^}]+\}|\$!?[A-Za-z_][\w.]*/.test(String(value ?? ''));
const blank = value => value == null || String(value).trim() === '';
const truth = value => value === true || value === 'true';

export function attributeEntries(list) {
    if (!list || typeof list !== 'object') return [];
    const entries = Array.isArray(list) ? list : list[ATTRIBUTE_CLASS];
    if (!entries || entries === '') return [];
    return (Array.isArray(entries) ? entries : [entries]).map(row => ({
        name: String(row?.name ?? ''), dataType: String(row?.dataType ?? 'String'), value: String(row?.value ?? '')
    }));
}

export function writeAttributes(rows) {
    const list = { '@class': 'java.util.ArrayList' };
    if (rows.length) list[ATTRIBUTE_CLASS] = rows.map(({ name, dataType, value }) => ({ name, dataType, value }));
    return list;
}

export function validateConnection(p) {
    const errors = [];
    const required = (key, label) => { if (blank(p[key])) errors.push({ key, label }); };
    required('queueUrl', 'Queue URL');
    if (p.authType === 'STATIC') { required('accessKeyId', 'Access Key ID'); required('secretAccessKey', 'Secret Access Key'); }
    if (p.authType === 'ROLE') required('roleArn', 'Role ARN');
    return errors;
}

export function validateProperties(p, source) {
    const errors = validateConnection(p);
    const add = (key, label) => errors.push({ key, label });
    const range = (key, label, min, max, optional = false) => {
        const value = String(p[key] ?? '').trim();
        if (optional && !value) return;
        if (hasExpression(value)) return;
        const numeric = /^[+-]?\d+$/.test(value) ? (typeof max === 'bigint' ? BigInt(value) : Number(value)) : null;
        if (numeric === null || numeric < min || numeric > max)
            add(key, `${label}: an integer from ${min} to ${max}, or a replacement variable`);
    };
    if (source) {
        range('waitTimeSeconds', 'Long Poll Wait', 0, 20);
        range('maxMessages', 'Max Messages', 1, 10);
        range('visibilityTimeout', 'Visibility Timeout', 0, 43200);
        if (p.s3EventMode === 'FETCH_OBJECT') range('s3MaxObjectSizeKB', 'Max Object Size', 0n, 9223372036854775807n / 1024n, true);
        const s = p.sourceConnectorProperties || {};
        if (truth(p.messageGroupHandling) && (!truth(s.respondAfterProcessing) || Number(s.processingThreads) !== 1))
            add('messageGroupHandling', 'FIFO source ordering: Source Queue OFF and one processing thread');
        if (p.s3EventMode === 'FETCH_OBJECT' && p.s3FileType === 'Binary' && truth(s.processBatch))
            add('s3FileType', 'Binary S3 objects: Process Batch disabled');
    } else {
        if (blank(p.template)) add('template', 'Template');
        range('delaySeconds', 'Delay', 0, 900, true);
        const queue = String(p.queueUrl ?? '').trim();
        if (!hasExpression(queue) && queue.endsWith('.fifo')) {
            if (blank(p.messageGroupId)) add('messageGroupId', 'Message Group ID for a FIFO queue');
            if (!blank(p.delaySeconds)) add('delaySeconds', 'FIFO queue: leave per-message delay blank');
        }
        if (!hasExpression(queue) && queue && !queue.endsWith('.fifo') && !blank(p.messageDeduplicationId))
            add('messageDeduplicationId', 'Standard queue: leave Deduplication ID blank');
        const rows = attributeEntries(p.messageAttributes);
        if (rows.length > 10) add('messageAttributes', 'At most 10 message attributes');
        const seen = new Set();
        for (const row of rows) {
            if (blank(row.name) || (!hasExpression(row.name) && (!/^[A-Za-z0-9_.-]{1,256}$/.test(row.name) || /^\.|\.$|\.\./.test(row.name) || /^(aws|amazon)\./i.test(row.name))))
                add('messageAttributes', 'A valid attribute name');
            if (!hasExpression(row.name) && seen.has(row.name)) add('messageAttributes', 'Unique attribute names');
            seen.add(row.name);
            if (!['String', 'Number', 'Binary'].includes(row.dataType)) add('messageAttributes', 'Attribute type String, Number, or Binary');
            if (row.value === '') add('messageAttributes', 'A value for every attribute');
            else if (!hasExpression(row.value) && row.dataType === 'Binary' && !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}(?:==)?|[A-Za-z0-9+/]{3}=?)?$/.test(row.value))
                add('messageAttributes', 'Base64 for a Binary attribute');
            else if (!hasExpression(row.value) && row.dataType === 'Number' && !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(row.value))
                add('messageAttributes', 'A numeric value for a Number attribute');
        }
    }
    return errors;
}

/* No network call occurs until inspect() is explicitly invoked. Context identity
 * and a complete property snapshot gate both successes and failures. */
export function createInspectionController(getContext, publish, request, errorMessage = e => e?.message || 'Queue inspection failed.', allowed = () => true) {
    let generation = 0;
    let disposed = false;
    const idle = () => ({ pending: false, message: '', error: false });
    return {
        activate() { disposed = false; generation++; },
        invalidate() { generation++; if (!disposed) publish(idle()); },
        dispose() { disposed = true; generation++; },
        async inspect() {
            if (disposed) return;
            if (!allowed()) { generation++; publish({ pending: false, error: true, message: 'Queue inspection permission is required.' }); return; }
            const context = getContext();
            const errors = validateConnection(context.properties);
            if (errors.length) { publish({ pending: false, error: true, message: `Provide ${errors.map(e => e.label).join(', ')}.` }); return; }
            const snapshot = JSON.stringify(context.properties);
            const ticket = ++generation;
            const channelId = context.channel?.id;
            const channelName = context.channel?.name;
            const current = () => {
                const now = getContext();
                return !disposed && allowed() && ticket === generation && now.properties === context.properties && now.channel === context.channel && now.connector === context.connector
                    && now.channel?.id === channelId && now.channel?.name === channelName && JSON.stringify(now.properties) === snapshot;
            };
            publish({ pending: true, message: '', error: false });
            try {
                const response = await request(JSON.parse(snapshot), { id: channelId, name: channelName });
                if (current()) publish({ pending: false, error: response?.type !== 'SUCCESS', message: response?.message || 'No inspection result received.' });
            } catch (error) {
                if (current()) publish({ pending: false, error: true, message: errorMessage(error) });
            }
        }
    };
}
