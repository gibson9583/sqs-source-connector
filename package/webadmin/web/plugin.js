// web/plugin.jsx
import { platform } from "@oie/web-shell";
import {
  ConnectorForm,
  PollSection,
  defaultPollProperties,
  defaultSourceProperties,
  defaultDestinationProperties,
  CHARSETS
} from "@oie/web-ui";
var React = platform.React;
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
      hint: "Leave blank to use the region from the default provider chain"
    },
    { section: "Authentication" },
    { key: "authType", label: "Auth Type", type: "select", options: AUTH_TYPES, refresh: true },
    { key: "accessKeyId", label: "Access Key Id", visible: (p) => p.authType === "STATIC" },
    { key: "secretAccessKey", label: "Secret Access Key", visible: (p) => p.authType === "STATIC" },
    { key: "roleArn", label: "Role ARN", visible: (p) => p.authType === "ROLE" },
    {
      key: "externalId",
      label: "External Id",
      visible: (p) => p.authType === "ROLE",
      hint: "Optional STS external id for the AssumeRole call"
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
  component({ properties, onChange }) {
    return /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement(ConnectorForm, { properties, onChange, fields: [
      ...awsFields(),
      { section: "Receive Settings" },
      {
        key: "waitTimeSeconds",
        label: "Long Poll Wait (s)",
        width: "110px",
        hint: "0\u201320; 20 enables SQS long polling"
      },
      { key: "maxMessages", label: "Max Messages / Receive", width: "110px", hint: "1\u201310" },
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
        checkLabel: "Preserve message group ordering (FIFO queues)"
      },
      { section: "S3 Event Notifications" },
      { key: "s3EventMode", label: "S3 Event Mode", type: "select", width: "260px", options: S3_EVENT_MODES, refresh: true },
      {
        key: "s3MaxObjectSizeKB",
        label: "Max Object Size (KB)",
        width: "110px",
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
    ] }), /* @__PURE__ */ React.createElement(PollSection, { properties, onChange }));
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
      messageDeduplicationId: ""
    };
  },
  component({ properties, onChange }) {
    return /* @__PURE__ */ React.createElement(ConnectorForm, { properties, onChange, fields: [
      ...awsFields(),
      { section: "Send Settings" },
      { key: "delaySeconds", label: "Delay (s)", width: "110px", hint: "Optional per-message delay (0\u2013900); blank for none" },
      { key: "messageGroupId", label: "Message Group Id", hint: "Required for FIFO queues" },
      { key: "messageDeduplicationId", label: "Deduplication Id", hint: "Optional; FIFO queues without content-based deduplication" },
      { key: "template", label: "Template", type: "code", minHeight: "160px" }
    ] });
  }
};
function register(platform2) {
  platform2.registerConnectorPanel("SQS Reader", "SOURCE", sqsReader);
  platform2.registerConnectorPanel("SQS Sender", "DESTINATION", sqsSender);
}
export {
  register
};
