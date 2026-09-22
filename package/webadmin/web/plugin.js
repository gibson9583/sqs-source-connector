/* SPDX-License-Identifier: MPL-2.0 */

// web/plugin.jsx
import { platform } from "@oie/web-shell";
import {
  ConnectorForm,
  PollSection,
  defaultPollProperties,
  defaultSourceProperties,
  defaultDestinationProperties,
  CHARSETS,
  postConnectorProperties
} from "@oie/web-ui";

// web/ui-model.mjs
var ATTRIBUTE_CLASS = "com.mirth.connect.connectors.sqs.SqsMessageAttribute";
var hasExpression = (value) => /\$!?\{[^}]+\}|\$!?[A-Za-z_][\w.]*/.test(String(value ?? ""));
var blank = (value) => value == null || String(value).trim() === "";
var truth = (value) => value === true || value === "true";
function attributeEntries(list) {
  if (!list || typeof list !== "object") return [];
  const entries = Array.isArray(list) ? list : list[ATTRIBUTE_CLASS];
  if (!entries || entries === "") return [];
  return (Array.isArray(entries) ? entries : [entries]).map((row) => ({
    name: String(row?.name ?? ""),
    dataType: String(row?.dataType ?? "String"),
    value: String(row?.value ?? "")
  }));
}
function writeAttributes(rows) {
  const list = { "@class": "java.util.ArrayList" };
  if (rows.length) list[ATTRIBUTE_CLASS] = rows.map(({ name, dataType, value }) => ({ name, dataType, value }));
  return list;
}
function validateConnection(p) {
  const errors = [];
  const required = (key, label) => {
    if (blank(p[key])) errors.push({ key, label });
  };
  required("queueUrl", "Queue URL");
  if (p.authType === "STATIC") {
    required("accessKeyId", "Access Key ID");
    required("secretAccessKey", "Secret Access Key");
  }
  if (p.authType === "ROLE") required("roleArn", "Role ARN");
  return errors;
}
function validateProperties(p, source) {
  const errors = validateConnection(p);
  const add = (key, label) => errors.push({ key, label });
  const range = (key, label, min, max, optional = false) => {
    const value = String(p[key] ?? "").trim();
    if (optional && !value) return;
    if (hasExpression(value)) return;
    const numeric = /^[+-]?\d+$/.test(value) ? typeof max === "bigint" ? BigInt(value) : Number(value) : null;
    if (numeric === null || numeric < min || numeric > max)
      add(key, `${label}: an integer from ${min} to ${max}, or a replacement variable`);
  };
  if (source) {
    range("waitTimeSeconds", "Long Poll Wait", 0, 20);
    range("maxMessages", "Max Messages", 1, 10);
    range("visibilityTimeout", "Visibility Timeout", 0, 43200);
    if (p.s3EventMode === "FETCH_OBJECT") range("s3MaxObjectSizeKB", "Max Object Size", 0n, 9223372036854775807n / 1024n, true);
    const s = p.sourceConnectorProperties || {};
    if (truth(p.messageGroupHandling) && (!truth(s.respondAfterProcessing) || Number(s.processingThreads) !== 1))
      add("messageGroupHandling", "FIFO source ordering: Source Queue OFF and one processing thread");
    if (p.s3EventMode === "FETCH_OBJECT" && p.s3FileType === "Binary" && truth(s.processBatch))
      add("s3FileType", "Binary S3 objects: Process Batch disabled");
  } else {
    if (blank(p.template)) add("template", "Template");
    range("delaySeconds", "Delay", 0, 900, true);
    const queue = String(p.queueUrl ?? "").trim();
    if (!hasExpression(queue) && queue.endsWith(".fifo")) {
      if (blank(p.messageGroupId)) add("messageGroupId", "Message Group ID for a FIFO queue");
      if (!blank(p.delaySeconds)) add("delaySeconds", "FIFO queue: leave per-message delay blank");
    }
    if (!hasExpression(queue) && queue && !queue.endsWith(".fifo") && !blank(p.messageDeduplicationId))
      add("messageDeduplicationId", "Standard queue: leave Deduplication ID blank");
    const rows = attributeEntries(p.messageAttributes);
    if (rows.length > 10) add("messageAttributes", "At most 10 message attributes");
    const seen = /* @__PURE__ */ new Set();
    for (const row of rows) {
      if (blank(row.name) || !hasExpression(row.name) && (!/^[A-Za-z0-9_.-]{1,256}$/.test(row.name) || /^\.|\.$|\.\./.test(row.name) || /^(aws|amazon)\./i.test(row.name)))
        add("messageAttributes", "A valid attribute name");
      if (!hasExpression(row.name) && seen.has(row.name)) add("messageAttributes", "Unique attribute names");
      seen.add(row.name);
      if (!["String", "Number", "Binary"].includes(row.dataType)) add("messageAttributes", "Attribute type String, Number, or Binary");
      if (row.value === "") add("messageAttributes", "A value for every attribute");
      else if (!hasExpression(row.value) && row.dataType === "Binary" && !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}(?:==)?|[A-Za-z0-9+/]{3}=?)?$/.test(row.value))
        add("messageAttributes", "Base64 for a Binary attribute");
      else if (!hasExpression(row.value) && row.dataType === "Number" && !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(row.value))
        add("messageAttributes", "A numeric value for a Number attribute");
    }
  }
  return errors;
}
function createInspectionController(getContext, publish, request, errorMessage = (e) => e?.message || "Queue inspection failed.", allowed = () => true) {
  let generation = 0;
  let disposed = false;
  const idle = () => ({ pending: false, message: "", error: false });
  return {
    activate() {
      disposed = false;
      generation++;
    },
    invalidate() {
      generation++;
      if (!disposed) publish(idle());
    },
    dispose() {
      disposed = true;
      generation++;
    },
    async inspect() {
      if (disposed) return;
      if (!allowed()) {
        generation++;
        publish({ pending: false, error: true, message: "Queue inspection permission is required." });
        return;
      }
      const context = getContext();
      const errors = validateConnection(context.properties);
      if (errors.length) {
        publish({ pending: false, error: true, message: `Provide ${errors.map((e) => e.label).join(", ")}.` });
        return;
      }
      const snapshot = JSON.stringify(context.properties);
      const ticket = ++generation;
      const channelId = context.channel?.id;
      const channelName = context.channel?.name;
      const current = () => {
        const now = getContext();
        return !disposed && allowed() && ticket === generation && now.properties === context.properties && now.channel === context.channel && now.connector === context.connector && now.channel?.id === channelId && now.channel?.name === channelName && JSON.stringify(now.properties) === snapshot;
      };
      publish({ pending: true, message: "", error: false });
      try {
        const response = await request(JSON.parse(snapshot), { id: channelId, name: channelName });
        if (current()) publish({ pending: false, error: response?.type !== "SUCCESS", message: response?.message || "No inspection result received." });
      } catch (error) {
        if (current()) publish({ pending: false, error: true, message: errorMessage(error) });
      }
    }
  };
}

// web/plugin.jsx
var React = platform.React;
var canInspect = () => platform.checkTask("channel", "doInspectSqsQueue");
var AUTH_TYPES = [
  { value: "DEFAULT", label: "Default Provider Chain" },
  { value: "STATIC", label: "Access Key / Secret Key" },
  { value: "ROLE", label: "Assume IAM Role (STS)" }
];
function awsFields() {
  return [
    { section: "SQS Queue" },
    {
      key: "queueUrl",
      label: "Queue URL",
      width: "420px",
      placeholder: "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
    },
    {
      key: "region",
      label: "Region",
      width: "160px",
      placeholder: "us-east-1",
      tooltip: "Leave blank to use the region from the default provider chain"
    },
    { section: "Authentication" },
    { key: "authType", label: "Auth Type", type: "select", options: AUTH_TYPES, refresh: true },
    { key: "accessKeyId", label: "Access Key Id", visible: (p) => p.authType === "STATIC" },
    { key: "secretAccessKey", label: "Secret Access Key", type: "password", visible: (p) => p.authType === "STATIC" },
    { key: "roleArn", label: "Role ARN", visible: (p) => p.authType === "ROLE" },
    {
      key: "externalId",
      label: "External Id",
      visible: (p) => p.authType === "ROLE",
      tooltip: "Optional STS external id for the AssumeRole call"
    }
  ];
}
var S3_EVENT_MODES = [
  { value: "DISABLED", label: "Disabled" },
  { value: "EXTRACT_DETAILS", label: "Extract Details to Source Map" },
  { value: "FETCH_OBJECT", label: "Fetch S3 Object as Message" }
];
var sqsReader = {
  defaults(version) {
    return {
      "@class": "com.mirth.connect.connectors.sqs.SqsReceiverProperties",
      "@version": version,
      pluginProperties: null,
      pollConnectorProperties: defaultPollProperties(version),
      sourceConnectorProperties: defaultSourceProperties(version),
      queueUrl: "",
      region: "",
      authType: "DEFAULT",
      accessKeyId: "",
      secretAccessKey: "",
      roleArn: "",
      externalId: "",
      waitTimeSeconds: "20",
      maxMessages: "10",
      visibilityTimeout: "30",
      includeAttributes: true,
      messageGroupHandling: false,
      s3EventMode: "DISABLED",
      s3MaxObjectSizeKB: "10240",
      s3FileType: "Text",
      s3Encoding: "DEFAULT_ENCODING"
    };
  },
  validate(properties) {
    return validateProperties(properties, true);
  },
  component({ properties, channel, connector, onChange }) {
    const inspection = useInspection(properties, channel, connector, onChange);
    onChange = inspection.onChange;
    return /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement(ConnectorForm, { properties, onChange, fields: [
      ...awsFields(),
      { section: "Receive Settings" },
      {
        key: "waitTimeSeconds",
        label: "Long Poll Wait (s)",
        width: "110px",
        tooltip: "0\u201320; 20 enables SQS long polling"
      },
      { key: "maxMessages", label: "Max Messages / Receive", width: "110px", tooltip: "1\u201310" },
      { key: "visibilityTimeout", label: "Visibility Timeout (s)", width: "110px" },
      {
        key: "includeAttributes",
        label: "Include Attributes",
        type: "checkbox",
        checkLabel: "Add SQS message attributes to the source map"
      },
      {
        key: "messageGroupHandling",
        label: "FIFO Message Groups",
        type: "checkbox",
        checkLabel: "Process FIFO source messages in order (Source Queue OFF, one processing thread)",
        tooltip: "Wait for source processing before receiving the next message. Queued destinations can finish later."
      },
      { section: "S3 Event Notifications" },
      { key: "s3EventMode", label: "S3 Event Mode", type: "select", width: "260px", options: S3_EVENT_MODES, refresh: true },
      {
        key: "s3MaxObjectSizeKB",
        label: "Max Object Size (KB)",
        width: "110px",
        tooltip: "Blank or 0 means no limit. The default is 10240 KB. Oversized objects retain the event JSON with an OVERSIZED status.",
        visible: (p) => p.s3EventMode === "FETCH_OBJECT"
      },
      {
        key: "s3FileType",
        label: "File Type",
        type: "select",
        options: ["Text", "Binary"],
        refresh: true,
        visible: (p) => p.s3EventMode === "FETCH_OBJECT"
      },
      {
        key: "s3Encoding",
        label: "Encoding",
        type: "select",
        options: CHARSETS,
        visible: (p) => p.s3EventMode === "FETCH_OBJECT" && p.s3FileType !== "Binary"
      }
    ] }), /* @__PURE__ */ React.createElement(QueueInspector, { inspection, properties }), /* @__PURE__ */ React.createElement(PollSection, { properties, onChange }));
  }
};
var sqsSender = {
  defaults(version) {
    return {
      "@class": "com.mirth.connect.connectors.sqs.SqsDispatcherProperties",
      "@version": version,
      pluginProperties: null,
      destinationConnectorProperties: defaultDestinationProperties(version),
      queueUrl: "",
      region: "",
      authType: "DEFAULT",
      accessKeyId: "",
      secretAccessKey: "",
      roleArn: "",
      externalId: "",
      template: "${message.encodedData}",
      delaySeconds: "",
      messageGroupId: "",
      messageDeduplicationId: "",
      messageAttributes: writeAttributes([])
    };
  },
  validate(properties) {
    return validateProperties(properties, false);
  },
  component({ properties, channel, connector, onChange }) {
    const inspection = useInspection(properties, channel, connector, onChange);
    onChange = inspection.onChange;
    return /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement(ConnectorForm, { properties, onChange, fields: [
      ...awsFields(),
      { section: "Send Settings" },
      { key: "delaySeconds", label: "Delay (s)", width: "110px", tooltip: "Optional per-message delay (0\u2013900); blank uses the queue default. Not supported on FIFO queues." },
      { key: "messageGroupId", label: "Message Group Id", tooltip: "Required for FIFO queues; optional fair-queue tenant grouping for standard queues." },
      { key: "messageDeduplicationId", label: "Deduplication Id", tooltip: "Optional; FIFO queues without content-based deduplication" },
      { key: "template", label: "Template", type: "code", minHeight: "160px" }
    ] }), /* @__PURE__ */ React.createElement(MessageAttributes, { properties, onChange }), /* @__PURE__ */ React.createElement(QueueInspector, { inspection, properties }));
  }
};
function useInspection(properties, channel, connector, onChange) {
  const [state, setState] = React.useState({ pending: false, message: "", error: false });
  const [, redraw] = React.useReducer((n) => n + 1, 0);
  const propertySnapshot = JSON.stringify(properties);
  const context = React.useRef();
  context.current = { properties, channel, connector };
  const controller = React.useRef();
  if (!controller.current) controller.current = createInspectionController(
    () => context.current,
    setState,
    (snapshot, target) => postConnectorProperties("/connectors/sqs/_inspectQueue", snapshot, target),
    () => "Queue inspection failed. Check the engine connection and server logs.",
    canInspect
  );
  React.useEffect(() => {
    controller.current.invalidate();
  }, [properties, channel, connector, propertySnapshot, channel?.id, channel?.name]);
  React.useEffect(() => {
    controller.current.activate();
    return () => controller.current.dispose();
  }, []);
  return { state, inspect: () => controller.current.inspect(), onChange() {
    controller.current.invalidate();
    onChange();
    redraw();
  } };
}
function QueueInspector({ inspection, properties }) {
  if (!canInspect()) return null;
  return /* @__PURE__ */ React.createElement("div", { className: "cform-section", style: { marginTop: "16px", minWidth: 0 } }, /* @__PURE__ */ React.createElement("div", { className: "cform-section-title" }, "Queue Inspection"), /* @__PURE__ */ React.createElement("div", { className: "cform-control" }, /* @__PURE__ */ React.createElement(
    "button",
    {
      type: "button",
      className: "btn",
      disabled: inspection.state.pending || validateConnection(properties).length > 0,
      onClick: inspection.inspect
    },
    inspection.state.pending ? "Inspecting queue\u2026" : "Inspect Queue"
  ), /* @__PURE__ */ React.createElement("p", { className: "hint", style: { margin: 0 } }, "Reads the supplied queue's attributes. Does not receive, send, or delete messages.")), inspection.state.message && /* @__PURE__ */ React.createElement(
    "pre",
    {
      role: inspection.state.error ? "alert" : "status",
      "aria-live": "polite",
      style: {
        margin: "10px 0 0",
        padding: "10px 12px",
        whiteSpace: "pre-wrap",
        overflowWrap: "anywhere",
        maxWidth: "100%",
        boxSizing: "border-box",
        border: "1px solid var(--line)",
        borderRadius: "6px",
        background: "var(--bg1)",
        fontFamily: "var(--font-mono)",
        fontSize: "11px",
        lineHeight: 1.5
      }
    },
    inspection.state.message
  ));
}
function MessageAttributes({ properties, onChange }) {
  const rows = attributeEntries(properties.messageAttributes);
  const change = (next) => {
    properties.messageAttributes = writeAttributes(next);
    onChange();
  };
  const update = (index, key, value) => change(rows.map((row, i) => i === index ? { ...row, [key]: value } : row));
  return /* @__PURE__ */ React.createElement("div", { className: "cform-section", "data-fkey": "messageAttributes", style: { marginTop: "16px" } }, /* @__PURE__ */ React.createElement("div", { className: "cform-section-title" }, "Message Attributes"), /* @__PURE__ */ React.createElement("p", null, "Up to 10 attributes. Names and values support replacement variables such as ", "${tenantId}", ". Binary values use Base64."), /* @__PURE__ */ React.createElement("table", null, /* @__PURE__ */ React.createElement("thead", null, /* @__PURE__ */ React.createElement("tr", null, /* @__PURE__ */ React.createElement("th", null, "Name"), /* @__PURE__ */ React.createElement("th", null, "Data Type"), /* @__PURE__ */ React.createElement("th", null, "Value"), /* @__PURE__ */ React.createElement("th", null))), /* @__PURE__ */ React.createElement("tbody", null, rows.map((row, index) => /* @__PURE__ */ React.createElement("tr", { key: index }, /* @__PURE__ */ React.createElement("td", null, /* @__PURE__ */ React.createElement("input", { "aria-label": `Attribute ${index + 1} name`, value: row.name, onChange: (e) => update(index, "name", e.target.value) })), /* @__PURE__ */ React.createElement("td", null, /* @__PURE__ */ React.createElement("select", { "aria-label": `Attribute ${index + 1} type`, value: row.dataType, onChange: (e) => update(index, "dataType", e.target.value) }, ["String", "Number", "Binary"].map((type) => /* @__PURE__ */ React.createElement("option", { key: type }, type)))), /* @__PURE__ */ React.createElement("td", null, /* @__PURE__ */ React.createElement("input", { "aria-label": `Attribute ${index + 1} value`, value: row.value, onChange: (e) => update(index, "value", e.target.value) })), /* @__PURE__ */ React.createElement("td", null, /* @__PURE__ */ React.createElement("button", { type: "button", className: "btn", "aria-label": `Remove attribute ${index + 1}`, onClick: () => change(rows.filter((_, i) => i !== index)) }, "Remove")))))), /* @__PURE__ */ React.createElement("button", { type: "button", className: "btn", disabled: rows.length >= 10, onClick: () => change([...rows, { name: "", dataType: "String", value: "" }]) }, "Add Attribute"));
}
function register(platform2) {
  platform2.registerConnectorPanel("SQS Reader", "SOURCE", sqsReader);
  platform2.registerConnectorPanel("SQS Sender", "DESTINATION", sqsSender);
}
export {
  attributeEntries,
  createInspectionController,
  register,
  validateConnection,
  validateProperties,
  writeAttributes
};
