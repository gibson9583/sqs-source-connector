/* SPDX-License-Identifier: MPL-2.0 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { SourceTextModule, SyntheticModule, createContext } from 'node:vm';

// Load the shipped bundle, not a duplicate of the source. Only host services
// and React rendering primitives are substituted; all plugin code executes.
const context = createContext({});
const panels = {};
const form = () => {};
const platform = {
    checkTask: () => true,
    React: {
        createElement: (type, props, ...children) => ({ type, props, children }),
        useState: initial => [initial, () => {}], useReducer: () => [0, () => {}],
        useRef: value => ({ current: value }), useEffect: () => {}
    },
    registerConnectorPanel: (name, mode, panel) => { panels[name] = panel; }
};
const host = new SyntheticModule(['platform'], function () { this.setExport('platform', platform); }, { context });
const uiValues = { ConnectorForm: form, PollSection: () => {}, CHARSETS: [],
    defaultPollProperties: () => ({}), defaultSourceProperties: () => ({ respondAfterProcessing: true, processingThreads: 1 }),
    defaultDestinationProperties: () => ({}), postConnectorProperties: () => { throw new Error('No automatic network access'); },
    apiErrorMessage: e => e.message };
const ui = new SyntheticModule(Object.keys(uiValues), function () { for (const [key, value] of Object.entries(uiValues)) this.setExport(key, value); }, { context });
const plugin = new SourceTextModule(readFileSync(new URL('../web/plugin.js', import.meta.url), 'utf8'), { context });
await plugin.link(name => name === '@oie/web-shell' ? host : ui);
await plugin.evaluate();
plugin.namespace.register(platform);
const { attributeEntries, writeAttributes, validateProperties, validateConnection, createInspectionController } = plugin.namespace;
const local = value => JSON.parse(JSON.stringify(value));
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const defaults = source => { const p = panels[source ? 'SQS Reader' : 'SQS Sender'].defaults('4.6.0'); p.queueUrl = 'https://example.invalid/q'; return p; };
function forms(node) { return [ ...(node?.type === form ? [node] : []), ...(node?.children || []).flat().flatMap(child => child && typeof child === 'object' ? forms(child) : []) ]; }

test('both shipped panels mask secrets, supply tooltips and register validation', () => {
    for (const name of ['SQS Reader', 'SQS Sender']) {
        const p = panels[name].defaults('4.6.0'); p.authType = 'STATIC';
        const fields = forms(panels[name].component({ properties: p, onChange() {} })).flatMap(node => node.props.fields);
        const secret = fields.find(f => f.key === 'secretAccessKey');
        assert.equal(secret.type, 'password');
        assert.equal(secret.visible(p), true); assert.equal(secret.visible({ ...p, authType: 'DEFAULT' }), false);
        assert.ok(fields.find(f => f.key === 'region').tooltip);
        assert.ok(fields.every(f => !('hint' in f)));
        assert.ok(panels[name].validate(p).some(e => e.key === 'queueUrl'));
        assert.ok(panels[name].validate(p).some(e => e.key === 'secretAccessKey'));
    }
});

test('literal ranges reject bad values and preserve Velocity values', () => {
    const p = defaults(true);
    assert.equal(validateProperties(p, true).length, 0);
    for (const value of ['-1', '21', '1.5', 'bad', '']) {
        assert.ok(validateProperties({ ...p, waitTimeSeconds: value }, true).some(e => e.key === 'waitTimeSeconds'));
    }
    for (const value of ['${waitSeconds}', '$waitSeconds', '$!{waitSeconds}'])
        assert.equal(validateProperties({ ...p, waitTimeSeconds: value }, true).length, 0);
    assert.ok(validateProperties({ ...p, maxMessages: '11' }, true).some(e => e.key === 'maxMessages'));
    assert.ok(validateProperties({ ...p, visibilityTimeout: '43201' }, true).some(e => e.key === 'visibilityTimeout'));
    assert.ok(validateProperties({ ...p, s3EventMode: 'FETCH_OBJECT', s3MaxObjectSizeKB: '-1' }, true).some(e => e.key === 's3MaxObjectSizeKB'));
    for (const value of ['', '0', '2147483648', '9007199254740991'])
        assert.equal(validateProperties({ ...p, s3EventMode: 'FETCH_OBJECT', s3MaxObjectSizeKB: value }, true).length, 0);
    assert.ok(validateProperties({ ...p, s3EventMode: 'FETCH_OBJECT', s3MaxObjectSizeKB: '9007199254740992' }, true).some(e => e.key === 's3MaxObjectSizeKB'));
});

test('source FIFO and binary batch prerequisites match engine constraints', () => {
    const p = defaults(true); p.messageGroupHandling = true;
    assert.equal(validateProperties(p, true).length, 0);
    p.sourceConnectorProperties.respondAfterProcessing = false;
    assert.ok(validateProperties(p, true).some(e => e.key === 'messageGroupHandling'));
    p.sourceConnectorProperties.respondAfterProcessing = true; p.sourceConnectorProperties.processingThreads = 2;
    assert.ok(validateProperties(p, true).some(e => e.key === 'messageGroupHandling'));
    p.messageGroupHandling = false; p.s3EventMode = 'FETCH_OBJECT'; p.s3FileType = 'Binary'; p.sourceConnectorProperties.processBatch = true;
    assert.ok(validateProperties(p, true).some(e => e.key === 's3FileType'));
});

test('sender FIFO delay/group checks do not prohibit standard fair queues', () => {
    const p = defaults(false); p.queueUrl += '.fifo';
    assert.ok(validateProperties(p, false).some(e => e.key === 'messageGroupId'));
    p.messageGroupId = '${tenantId}'; p.delaySeconds = '0';
    assert.ok(validateProperties(p, false).some(e => e.key === 'delaySeconds'));
    p.delaySeconds = ''; assert.equal(validateProperties(p, false).length, 0);
    p.queueUrl = 'https://example.invalid/standard'; p.delaySeconds = '900'; assert.equal(validateProperties(p, false).length, 0);
    p.delaySeconds = '901'; assert.ok(validateProperties(p, false).some(e => e.key === 'delaySeconds'));
});

test('attribute XStream singleton, many, empty and detached round trips', () => {
    const key = 'com.mirth.connect.connectors.sqs.SqsMessageAttribute';
    const row = { name: 'tenant', dataType: 'String', value: '${tenantId}' };
    assert.deepEqual(local(attributeEntries({ [key]: row })), [row]);
    const rows = [row, { name: 'count', dataType: 'Number', value: '2' }, { name: 'raw', dataType: 'Binary', value: 'YQ==' }];
    const wire = writeAttributes(rows);
    assert.equal(wire['@class'], 'java.util.ArrayList');
    assert.deepEqual(local(attributeEntries(wire)), rows);
    rows[0].value = 'changed'; assert.equal(attributeEntries(wire)[0].value, '${tenantId}');
    assert.deepEqual(local(attributeEntries(writeAttributes([]))), []);
    assert.deepEqual(local(attributeEntries(null)), []);
});

test('attribute validation covers duplicate names, types, size, Number and Base64', () => {
    const p = defaults(false);
    const good = { name: 'id', dataType: 'String', value: '${id}' };
    const check = rows => validateProperties({ ...p, messageAttributes: writeAttributes(rows) }, false).filter(e => e.key === 'messageAttributes');
    assert.equal(check([{ name: 'spaces', dataType: 'String', value: ' ' }]).length, 0);
    assert.equal(check([good, { name: 'bytes', dataType: 'Binary', value: 'YQ' }, { name: 'amount', dataType: 'Number', value: '2.5e3' }]).length, 0);
    for (const rows of [[good, good], [{ ...good, name: 'AWS.test' }], [{ ...good, dataType: 'Other' }], [{ ...good, dataType: 'Binary', value: '!!!' }], [{ ...good, dataType: 'Number', value: 'nan' }], Array.from({ length: 11 }, (_, i) => ({ ...good, name: `a${i}` }))]) assert.ok(check(rows).length);
});

test('inspector validates connection only and performs no automatic call', async () => {
    const p = defaults(false); p.template = ''; p.messageAttributes = writeAttributes([{ name: '', dataType: 'Number', value: '' }]);
    assert.equal(validateConnection(p).length, 0);
    let count = 0; const states = [];
    const c = createInspectionController(() => ({ properties: p }), state => states.push(state), async snapshot => { count++; assert.notEqual(snapshot, p); return { type: 'SUCCESS', message: 'GetQueueAttributes only' }; });
    assert.equal(count, 0); await c.inspect(); assert.equal(count, 1); assert.equal(states.at(-1).message, 'GetQueueAttributes only');
    p.queueUrl = ''; await c.inspect(); assert.equal(count, 1); assert.equal(states.at(-1).error, true);
});

test('inspector suppresses stale successes and failures after edits, switches and disposal', async () => {
    for (const action of ['edit', 'switch', 'dispose']) for (const error of [false, true]) {
        let current = { properties: defaults(false), channel: { id: 'a', name: 'A' }, connector: {} };
        const pending = deferred(); const states = [];
        const c = createInspectionController(() => current, state => states.push(state), () => pending.promise);
        const running = c.inspect();
        if (action === 'edit') { current.properties.region = 'new-region'; c.invalidate(); }
        if (action === 'switch') { current = { properties: defaults(false), channel: { id: 'b', name: 'B' }, connector: {} }; c.invalidate(); }
        if (action === 'dispose') c.dispose();
        const before = states.length;
        if (error) pending.reject(new Error('stale failure')); else pending.resolve({ type: 'SUCCESS', message: 'stale success' });
        await running; assert.equal(states.length, before, `${action}/${error}`);
    }
});

test('older inspection completion cannot replace a newer pending/result state', async () => {
    const context = { properties: defaults(false), channel: {}, connector: {} };
    const first = deferred(), second = deferred(); const states = []; let calls = 0;
    const c = createInspectionController(() => context, state => states.push(state), () => (++calls === 1 ? first.promise : second.promise));
    const a = c.inspect(); c.invalidate(); const b = c.inspect();
    first.reject(new Error('old')); await a; assert.equal(states.at(-1).pending, true);
    second.resolve({ type: 'SUCCESS', message: 'new' }); await b; assert.equal(states.at(-1).message, 'new');
    c.dispose(); c.activate(); const again = c.inspect(); await again; assert.equal(calls, 3);
});

test('inspector rechecks permission on action and before publishing a response', async () => {
    const context = { properties: defaults(false), channel: {}, connector: {} };
    let allowed = false, calls = 0;
    const pending = deferred(), states = [];
    const c = createInspectionController(() => context, state => states.push(state), () => { calls++; return pending.promise; }, e => e.message, () => allowed);
    await c.inspect(); assert.equal(calls, 0); assert.equal(states.at(-1).error, true);
    allowed = true; const running = c.inspect(); assert.equal(calls, 1);
    allowed = false; const before = states.length;
    pending.resolve({ type: 'SUCCESS', message: 'should remain hidden' }); await running;
    assert.equal(states.length, before);
});
