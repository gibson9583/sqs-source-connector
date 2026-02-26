/*
 * SPDX-License-Identifier: MIT
 */
package io.github.gibson9583.sqs;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.mirth.connect.client.ui.PlatformUI;
import com.mirth.connect.client.ui.UIConstants;
import com.mirth.connect.client.ui.panels.connectors.ConnectorSettingsPanel;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;

import net.miginfocom.swing.MigLayout;

/**
 * Swing settings panel for the SQS source connector.
 * <p>
 * All configurable fields are plain text fields (not spinners) so that users
 * can enter OIE replacement variables like {@code ${configMap.queueUrl}},
 * {@code ${sqs.waitTime}}, {@code ${AWS_SECRET_KEY}}, etc. The region
 * dropdown is editable for the same reason.
 * <p>
 * Displayed in the OIE Administrator when configuring a channel
 * with the SQS Reader source connector.
 */
public class SqsReceiverPanel extends ConnectorSettingsPanel {

    // --- AWS Connection ---
    private JTextField queueUrlField;
    private JComboBox<String> regionCombo;

    // --- Auth ---
    private JRadioButton authDefaultRadio;
    private JRadioButton authStaticRadio;
    private JRadioButton authRoleRadio;
    private JPanel authCardsPanel;
    private CardLayout authCardLayout;

    // Static credentials
    private JTextField accessKeyIdField;
    private JPasswordField secretAccessKeyField;

    // Role assumption
    private JTextField roleArnField;
    private JTextField externalIdField;

    // --- SQS Settings (text fields for Velocity substitution support) ---
    private JTextField waitTimeField;
    private JTextField maxMessagesField;
    private JTextField visibilityTimeoutField;
    private JCheckBox includeAttributesCheck;
    private JCheckBox messageGroupHandlingCheck;

    // --- S3 Event Notifications ---
    private JRadioButton s3DisabledRadio;
    private JRadioButton s3ExtractRadio;
    private JRadioButton s3FetchRadio;
    private JLabel s3MaxSizeLabel;
    private JTextField s3MaxSizeField;
    private JLabel s3FileTypeLabel;
    private JComboBox<String> s3FileTypeCombo;
    private JLabel s3EncodingLabel;
    @SuppressWarnings("rawtypes")
    private JComboBox s3EncodingCombo;

    // AWS regions (user can also type a replacement variable)
    private static final String[] AWS_REGIONS = {
            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
            "af-south-1", "ap-east-1", "ap-south-1", "ap-south-2",
            "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
            "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
            "ca-central-1", "eu-central-1", "eu-central-2",
            "eu-west-1", "eu-west-2", "eu-west-3",
            "eu-south-1", "eu-south-2", "eu-north-1",
            "me-south-1", "me-central-1",
            "sa-east-1",
            "us-gov-east-1", "us-gov-west-1"
    };

    private static final String AUTH_CARD_DEFAULT = "default";
    private static final String AUTH_CARD_STATIC = "static";
    private static final String AUTH_CARD_ROLE = "role";

    private static final String[] S3_FILE_TYPES = { "Text", "Binary" };

    public SqsReceiverPanel() {
        initComponents();
        initLayout();
    }

    // =========================================================================
    // ConnectorSettingsPanel overrides
    // =========================================================================

    @Override
    public String getConnectorName() {
        return new SqsReceiverProperties().getName();
    }

    @Override
    public ConnectorProperties getProperties() {
        SqsReceiverProperties props = new SqsReceiverProperties();

        // AWS Connection
        props.setQueueUrl(queueUrlField.getText().trim());
        // Editable combo: user may have typed a variable like ${configMap.region}
        Object regionSelection = regionCombo.getEditor().getItem();
        props.setRegion(regionSelection != null ? regionSelection.toString().trim() : "");

        // Auth
        if (authStaticRadio.isSelected()) {
            props.setAuthType(SqsReceiverProperties.AuthType.STATIC);
            props.setAccessKeyId(accessKeyIdField.getText().trim());
            props.setSecretAccessKey(new String(secretAccessKeyField.getPassword()));
        } else if (authRoleRadio.isSelected()) {
            props.setAuthType(SqsReceiverProperties.AuthType.ROLE);
            props.setRoleArn(roleArnField.getText().trim());
            props.setExternalId(externalIdField.getText().trim());
        } else {
            props.setAuthType(SqsReceiverProperties.AuthType.DEFAULT);
        }

        // SQS Settings (stored as String for Velocity substitution)
        props.setWaitTimeSeconds(waitTimeField.getText().trim());
        props.setMaxMessages(maxMessagesField.getText().trim());
        props.setVisibilityTimeout(visibilityTimeoutField.getText().trim());
        props.setIncludeAttributes(includeAttributesCheck.isSelected());
        props.setMessageGroupHandling(messageGroupHandlingCheck.isSelected());

        // S3 Event Notifications
        if (s3FetchRadio.isSelected()) {
            props.setS3EventMode(SqsReceiverProperties.S3EventMode.FETCH_OBJECT);
        } else if (s3ExtractRadio.isSelected()) {
            props.setS3EventMode(SqsReceiverProperties.S3EventMode.EXTRACT_DETAILS);
        } else {
            props.setS3EventMode(SqsReceiverProperties.S3EventMode.DISABLED);
        }
        props.setS3MaxObjectSizeKB(s3MaxSizeField.getText().trim());
        Object fileTypeSelection = s3FileTypeCombo.getSelectedItem();
        props.setS3FileType(fileTypeSelection != null ? fileTypeSelection.toString() : "Text");
        props.setS3Encoding(PlatformUI.MIRTH_FRAME.getSelectedEncodingForConnector(s3EncodingCombo));

        return props;
    }

    @Override
    public void setProperties(ConnectorProperties properties) {
        SqsReceiverProperties props = (SqsReceiverProperties) properties;

        // Preserve save state so that populating fields doesn't falsely trigger dirty
        boolean saveEnabled = PlatformUI.MIRTH_FRAME.isSaveEnabled();

        // AWS Connection
        queueUrlField.setText(props.getQueueUrl());
        regionCombo.getEditor().setItem(props.getRegion());
        regionCombo.setSelectedItem(props.getRegion());

        // Auth
        switch (props.getAuthType()) {
            case STATIC:
                authStaticRadio.setSelected(true);
                accessKeyIdField.setText(props.getAccessKeyId());
                secretAccessKeyField.setText(props.getSecretAccessKey());
                authCardLayout.show(authCardsPanel, AUTH_CARD_STATIC);
                break;
            case ROLE:
                authRoleRadio.setSelected(true);
                roleArnField.setText(props.getRoleArn());
                externalIdField.setText(props.getExternalId());
                authCardLayout.show(authCardsPanel, AUTH_CARD_ROLE);
                break;
            case DEFAULT:
            default:
                authDefaultRadio.setSelected(true);
                authCardLayout.show(authCardsPanel, AUTH_CARD_DEFAULT);
                break;
        }

        // SQS Settings (plain text — may contain Velocity expressions)
        waitTimeField.setText(props.getWaitTimeSeconds());
        maxMessagesField.setText(props.getMaxMessages());
        visibilityTimeoutField.setText(props.getVisibilityTimeout());
        includeAttributesCheck.setSelected(props.isIncludeAttributes());
        messageGroupHandlingCheck.setSelected(props.isMessageGroupHandling());

        // S3 Event Notifications
        SqsReceiverProperties.S3EventMode s3Mode = props.getS3EventMode();
        boolean fetchMode = s3Mode == SqsReceiverProperties.S3EventMode.FETCH_OBJECT;
        switch (s3Mode) {
            case FETCH_OBJECT:
                s3FetchRadio.setSelected(true);
                break;
            case EXTRACT_DETAILS:
                s3ExtractRadio.setSelected(true);
                break;
            case DISABLED:
            default:
                s3DisabledRadio.setSelected(true);
                break;
        }
        s3MaxSizeField.setText(props.getS3MaxObjectSizeKB());
        s3FileTypeCombo.setSelectedItem(props.getS3FileType());
        PlatformUI.MIRTH_FRAME.setPreviousSelectedEncodingForConnector(s3EncodingCombo, props.getS3Encoding());

        // Visibility
        s3MaxSizeLabel.setVisible(fetchMode);
        s3MaxSizeField.setVisible(fetchMode);
        s3FileTypeLabel.setVisible(fetchMode);
        s3FileTypeCombo.setVisible(fetchMode);
        boolean isText = "Text".equals(props.getS3FileType());
        s3EncodingLabel.setVisible(fetchMode && isText);
        s3EncodingCombo.setVisible(fetchMode && isText);

        // Restore save state
        PlatformUI.MIRTH_FRAME.setSaveEnabled(saveEnabled);
    }

    @Override
    public ConnectorProperties getDefaults() {
        return new SqsReceiverProperties();
    }

    @Override
    public boolean checkProperties(ConnectorProperties properties, boolean highlight) {
        SqsReceiverProperties props = (SqsReceiverProperties) properties;
        boolean valid = true;

        // Queue URL is required (but may be a replacement variable)
        if (props.getQueueUrl() == null || props.getQueueUrl().isBlank()) {
            valid = false;
            if (highlight) {
                queueUrlField.setBackground(UIConstants.INVALID_COLOR);
            }
        } else {
            queueUrlField.setBackground(null);
        }

        // Wait time, max messages, visibility timeout must be non-empty
        if (props.getWaitTimeSeconds() == null || props.getWaitTimeSeconds().isBlank()) {
            valid = false;
            if (highlight) {
                waitTimeField.setBackground(UIConstants.INVALID_COLOR);
            }
        } else {
            waitTimeField.setBackground(null);
        }

        if (props.getMaxMessages() == null || props.getMaxMessages().isBlank()) {
            valid = false;
            if (highlight) {
                maxMessagesField.setBackground(UIConstants.INVALID_COLOR);
            }
        } else {
            maxMessagesField.setBackground(null);
        }

        if (props.getVisibilityTimeout() == null || props.getVisibilityTimeout().isBlank()) {
            valid = false;
            if (highlight) {
                visibilityTimeoutField.setBackground(UIConstants.INVALID_COLOR);
            }
        } else {
            visibilityTimeoutField.setBackground(null);
        }

        // Static auth requires key and secret
        if (props.getAuthType() == SqsReceiverProperties.AuthType.STATIC) {
            if (props.getAccessKeyId() == null || props.getAccessKeyId().isBlank()) {
                valid = false;
                if (highlight) {
                    accessKeyIdField.setBackground(UIConstants.INVALID_COLOR);
                }
            } else {
                accessKeyIdField.setBackground(null);
            }
            if (props.getSecretAccessKey() == null || props.getSecretAccessKey().isBlank()) {
                valid = false;
                if (highlight) {
                    secretAccessKeyField.setBackground(UIConstants.INVALID_COLOR);
                }
            } else {
                secretAccessKeyField.setBackground(null);
            }
        }

        // Role auth requires ARN
        if (props.getAuthType() == SqsReceiverProperties.AuthType.ROLE) {
            if (props.getRoleArn() == null || props.getRoleArn().isBlank()) {
                valid = false;
                if (highlight) {
                    roleArnField.setBackground(UIConstants.INVALID_COLOR);
                }
            } else {
                roleArnField.setBackground(null);
            }
        }

        // S3 Fetch Object mode requires max size to be non-empty
        if (props.getS3EventMode() == SqsReceiverProperties.S3EventMode.FETCH_OBJECT) {
            if (props.getS3MaxObjectSizeKB() == null || props.getS3MaxObjectSizeKB().isBlank()) {
                valid = false;
                if (highlight) {
                    s3MaxSizeField.setBackground(UIConstants.INVALID_COLOR);
                }
            } else {
                s3MaxSizeField.setBackground(null);
            }
        }

        return valid;
    }

    @Override
    public void resetInvalidProperties() {
        queueUrlField.setBackground(null);
        accessKeyIdField.setBackground(null);
        secretAccessKeyField.setBackground(null);
        roleArnField.setBackground(null);
        waitTimeField.setBackground(null);
        maxMessagesField.setBackground(null);
        visibilityTimeoutField.setBackground(null);
        s3MaxSizeField.setBackground(null);
    }

    // =========================================================================
    // UI Component Initialization
    // =========================================================================

    private void initComponents() {
        String velocityHint = " — supports replacement variables e.g. ${configMap.key}";

        // Queue URL
        queueUrlField = new JTextField();
        queueUrlField.setToolTipText(
                "Full SQS queue URL (e.g. https://sqs.us-east-1.amazonaws.com/123456789012/my-queue)"
                        + velocityHint);

        // Region (editable combo so user can type a variable)
        regionCombo = new JComboBox<>(AWS_REGIONS);
        regionCombo.setEditable(true);
        regionCombo.setSelectedItem("");
        regionCombo.setToolTipText(
                "AWS region (optional). If blank, uses the default region from the AWS credential provider chain."
                        + velocityHint);

        // Auth radio buttons
        authDefaultRadio = new JRadioButton("Default Credential Chain");
        authDefaultRadio.setToolTipText("Uses environment variables, instance profile, ECS task role, etc.");
        authDefaultRadio.setBackground(UIConstants.BACKGROUND_COLOR);

        authStaticRadio = new JRadioButton("Static Credentials");
        authStaticRadio.setToolTipText("Use an explicit AWS Access Key ID and Secret Access Key");
        authStaticRadio.setBackground(UIConstants.BACKGROUND_COLOR);

        authRoleRadio = new JRadioButton("Assume Role (STS)");
        authRoleRadio.setToolTipText("Assume an IAM role via AWS Security Token Service");
        authRoleRadio.setBackground(UIConstants.BACKGROUND_COLOR);

        ButtonGroup authGroup = new ButtonGroup();
        authGroup.add(authDefaultRadio);
        authGroup.add(authStaticRadio);
        authGroup.add(authRoleRadio);
        authDefaultRadio.setSelected(true);

        ActionListener authSwitcher = (ActionEvent e) -> {
            if (authDefaultRadio.isSelected()) {
                authCardLayout.show(authCardsPanel, AUTH_CARD_DEFAULT);
            } else if (authStaticRadio.isSelected()) {
                authCardLayout.show(authCardsPanel, AUTH_CARD_STATIC);
            } else if (authRoleRadio.isSelected()) {
                authCardLayout.show(authCardsPanel, AUTH_CARD_ROLE);
            }
        };
        authDefaultRadio.addActionListener(authSwitcher);
        authStaticRadio.addActionListener(authSwitcher);
        authRoleRadio.addActionListener(authSwitcher);

        // Static creds fields
        accessKeyIdField = new JTextField();
        accessKeyIdField.setToolTipText("AWS Access Key ID" + velocityHint);
        secretAccessKeyField = new JPasswordField();
        secretAccessKeyField.setToolTipText("AWS Secret Access Key" + velocityHint);

        // Role fields
        roleArnField = new JTextField();
        roleArnField.setToolTipText(
                "ARN of the IAM role to assume (e.g. arn:aws:iam::123456789012:role/MyRole)"
                        + velocityHint);
        externalIdField = new JTextField();
        externalIdField.setToolTipText("Optional external ID for cross-account role assumption"
                + velocityHint);

        // Auth cards
        authCardLayout = new CardLayout();
        authCardsPanel = new JPanel(authCardLayout);
        authCardsPanel.setBackground(UIConstants.BACKGROUND_COLOR);

        // Default card (informational message)
        JPanel defaultPanel = new JPanel(new MigLayout("insets 5, fillx"));
        defaultPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        JLabel defaultLabel = new JLabel(
                "Uses AWS default credential provider chain "
                        + "(env vars, instance profile, ECS task role, ~/.aws/credentials)");
        defaultLabel.setFont(defaultLabel.getFont().deriveFont(Font.ITALIC));
        defaultPanel.add(defaultLabel);

        // Static card
        JPanel staticPanel = new JPanel(new MigLayout("insets 5, fillx, wrap 2", "[right]10[grow,fill]"));
        staticPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        staticPanel.add(new JLabel("Access Key ID:"));
        staticPanel.add(accessKeyIdField);
        staticPanel.add(new JLabel("Secret Access Key:"));
        staticPanel.add(secretAccessKeyField);

        // Role card
        JPanel rolePanel = new JPanel(new MigLayout("insets 5, fillx, wrap 2", "[right]10[grow,fill]"));
        rolePanel.setBackground(UIConstants.BACKGROUND_COLOR);
        rolePanel.add(new JLabel("Role ARN:"));
        rolePanel.add(roleArnField);
        rolePanel.add(new JLabel("External ID (optional):"));
        rolePanel.add(externalIdField);

        authCardsPanel.add(defaultPanel, AUTH_CARD_DEFAULT);
        authCardsPanel.add(staticPanel, AUTH_CARD_STATIC);
        authCardsPanel.add(rolePanel, AUTH_CARD_ROLE);

        // SQS settings — plain text fields for Velocity substitution support
        waitTimeField = new JTextField("20");
        waitTimeField.setToolTipText(
                "Long poll wait time in seconds (0 = short polling, max 20). "
                        + "Higher values reduce SQS API costs." + velocityHint);

        maxMessagesField = new JTextField("10");
        maxMessagesField.setToolTipText(
                "Maximum messages to retrieve per poll request (1-10)" + velocityHint);

        visibilityTimeoutField = new JTextField("30");
        visibilityTimeoutField.setToolTipText(
                "Seconds a message is hidden from other consumers after retrieval (0-43200). "
                        + "Should exceed expected processing time." + velocityHint);

        includeAttributesCheck = new JCheckBox("Include SQS message attributes in channel source map");
        includeAttributesCheck.setBackground(UIConstants.BACKGROUND_COLOR);
        includeAttributesCheck.setSelected(true);
        includeAttributesCheck.setToolTipText(
                "Adds SQS message attributes (user-defined and system) to the source map");

        messageGroupHandlingCheck = new JCheckBox("FIFO queue message group handling");
        messageGroupHandlingCheck.setBackground(UIConstants.BACKGROUND_COLOR);
        messageGroupHandlingCheck.setSelected(false);
        messageGroupHandlingCheck.setToolTipText(
                "Enable for FIFO queues. Includes MessageGroupId and SequenceNumber in source map.");

        // S3 Event Notification controls
        s3DisabledRadio = new JRadioButton("Disabled");
        s3DisabledRadio.setBackground(UIConstants.BACKGROUND_COLOR);
        s3DisabledRadio.setToolTipText("Treat SQS message body as-is (default behavior)");

        s3ExtractRadio = new JRadioButton("Extract Details");
        s3ExtractRadio.setBackground(UIConstants.BACKGROUND_COLOR);
        s3ExtractRadio.setToolTipText(
                "Parse S3 event JSON and add bucket/key/event details to source map. "
                        + "Keep original SQS body as message content.");

        s3FetchRadio = new JRadioButton("Fetch Object");
        s3FetchRadio.setBackground(UIConstants.BACKGROUND_COLOR);
        s3FetchRadio.setToolTipText(
                "Parse S3 event JSON, add details to source map, AND replace message body "
                        + "with the fetched S3 object content.");

        ButtonGroup s3Group = new ButtonGroup();
        s3Group.add(s3DisabledRadio);
        s3Group.add(s3ExtractRadio);
        s3Group.add(s3FetchRadio);
        s3DisabledRadio.setSelected(true);

        s3MaxSizeLabel = new JLabel("Max Object Size (KB):");
        s3MaxSizeField = new JTextField("10240");
        s3MaxSizeField.setToolTipText(
                "Maximum S3 object size in KB to fetch. Objects larger than this are skipped "
                        + "and the original event JSON is passed instead. 0 = no limit."
                        + velocityHint);

        // File Type combo (Text / Binary)
        s3FileTypeLabel = new JLabel("File Type:");
        s3FileTypeCombo = new JComboBox<>(S3_FILE_TYPES);
        s3FileTypeCombo.setSelectedItem("Text");
        s3FileTypeCombo.setToolTipText(
                "Text: decode the S3 object to a string using the selected encoding. "
                        + "Binary: pass raw bytes to the channel.");

        // Encoding combo — populated by OIE's standard charset list
        s3EncodingLabel = new JLabel("Encoding:");
        s3EncodingCombo = new JComboBox<>();
        PlatformUI.MIRTH_FRAME.setupCharsetEncodingForConnector(s3EncodingCombo);
        s3EncodingCombo.setToolTipText(
                "Fallback encoding when the S3 object's Content-Type header does not specify a charset. "
                        + "Content-Type charset is always tried first.");

        // Show/hide encoding combo based on file type
        s3FileTypeCombo.addActionListener((ActionEvent e) -> {
            boolean isText = "Text".equals(s3FileTypeCombo.getSelectedItem());
            s3EncodingLabel.setVisible(isText);
            s3EncodingCombo.setVisible(isText);
        });

        // Show/hide fetch options based on S3 mode
        ActionListener s3ModeSwitcher = (ActionEvent e) -> {
            boolean fetchMode = s3FetchRadio.isSelected();
            s3MaxSizeLabel.setVisible(fetchMode);
            s3MaxSizeField.setVisible(fetchMode);
            s3FileTypeLabel.setVisible(fetchMode);
            s3FileTypeCombo.setVisible(fetchMode);
            boolean showEncoding = fetchMode && "Text".equals(s3FileTypeCombo.getSelectedItem());
            s3EncodingLabel.setVisible(showEncoding);
            s3EncodingCombo.setVisible(showEncoding);
        };
        s3DisabledRadio.addActionListener(s3ModeSwitcher);
        s3ExtractRadio.addActionListener(s3ModeSwitcher);
        s3FetchRadio.addActionListener(s3ModeSwitcher);

        // Initial state — all fetch options hidden
        s3MaxSizeLabel.setVisible(false);
        s3MaxSizeField.setVisible(false);
        s3FileTypeLabel.setVisible(false);
        s3FileTypeCombo.setVisible(false);
        s3EncodingLabel.setVisible(false);
        s3EncodingCombo.setVisible(false);

        // --- Change notification for all components ---
        // OIE requires explicit save notification; text fields use DocumentListener,
        // radio buttons and checkboxes use ActionListener.
        DocumentListener saveDocListener = new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) { PlatformUI.MIRTH_FRAME.setSaveEnabled(true); }
            @Override
            public void removeUpdate(DocumentEvent e) { PlatformUI.MIRTH_FRAME.setSaveEnabled(true); }
            @Override
            public void changedUpdate(DocumentEvent e) { PlatformUI.MIRTH_FRAME.setSaveEnabled(true); }
        };
        ActionListener saveActionListener = (ActionEvent e) -> PlatformUI.MIRTH_FRAME.setSaveEnabled(true);

        // Text fields
        queueUrlField.getDocument().addDocumentListener(saveDocListener);
        accessKeyIdField.getDocument().addDocumentListener(saveDocListener);
        secretAccessKeyField.getDocument().addDocumentListener(saveDocListener);
        roleArnField.getDocument().addDocumentListener(saveDocListener);
        externalIdField.getDocument().addDocumentListener(saveDocListener);
        waitTimeField.getDocument().addDocumentListener(saveDocListener);
        maxMessagesField.getDocument().addDocumentListener(saveDocListener);
        visibilityTimeoutField.getDocument().addDocumentListener(saveDocListener);
        s3MaxSizeField.getDocument().addDocumentListener(saveDocListener);

        // Editable combo box
        JTextField regionEditor = (JTextField) regionCombo.getEditor().getEditorComponent();
        regionEditor.getDocument().addDocumentListener(saveDocListener);
        regionCombo.addActionListener(saveActionListener);

        // Radio buttons
        authDefaultRadio.addActionListener(saveActionListener);
        authStaticRadio.addActionListener(saveActionListener);
        authRoleRadio.addActionListener(saveActionListener);
        s3DisabledRadio.addActionListener(saveActionListener);
        s3ExtractRadio.addActionListener(saveActionListener);
        s3FetchRadio.addActionListener(saveActionListener);

        // Checkboxes
        includeAttributesCheck.addActionListener(saveActionListener);
        messageGroupHandlingCheck.addActionListener(saveActionListener);

        // S3 fetch combos
        s3FileTypeCombo.addActionListener(saveActionListener);
        s3EncodingCombo.addActionListener(saveActionListener);
    }

    // =========================================================================
    // Layout
    // =========================================================================

    private void initLayout() {
        setLayout(new MigLayout("insets 8, fillx, wrap 1, hidemode 3", "[grow,fill]"));
        setBackground(UIConstants.BACKGROUND_COLOR);

        // --- AWS Connection section ---
        JPanel connectionPanel = new JPanel(
                new MigLayout("insets 8, fillx, wrap 2", "[right]10[grow,fill]"));
        connectionPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        connectionPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "AWS Connection", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        connectionPanel.add(new JLabel("Queue URL:"));
        connectionPanel.add(queueUrlField);
        connectionPanel.add(new JLabel("Region (optional):"));
        connectionPanel.add(regionCombo, "width 250!");

        add(connectionPanel);

        // --- Authentication section ---
        JPanel authPanel = new JPanel(new MigLayout("insets 8, fillx, wrap 1"));
        authPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        authPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "Authentication", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        JPanel radioPanel = new JPanel(new MigLayout("insets 0, gap 15"));
        radioPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        radioPanel.add(authDefaultRadio);
        radioPanel.add(authStaticRadio);
        radioPanel.add(authRoleRadio);
        authPanel.add(radioPanel, "growx");
        authPanel.add(authCardsPanel, "growx");

        add(authPanel);

        // --- SQS Settings section ---
        JPanel sqsPanel = new JPanel(
                new MigLayout("insets 8, fillx, wrap 2", "[right]10[grow,fill]"));
        sqsPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        sqsPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "SQS Settings", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        sqsPanel.add(new JLabel("Long Poll Wait Time (seconds):"));
        sqsPanel.add(waitTimeField, "width 200!");
        sqsPanel.add(new JLabel("Max Messages Per Poll:"));
        sqsPanel.add(maxMessagesField, "width 200!");
        sqsPanel.add(new JLabel("Visibility Timeout (seconds):"));
        sqsPanel.add(visibilityTimeoutField, "width 200!");

        add(sqsPanel);

        // --- Message Handling section ---
        JPanel msgPanel = new JPanel(new MigLayout("insets 8, fillx, wrap 1"));
        msgPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        msgPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "Message Handling", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        msgPanel.add(includeAttributesCheck);
        msgPanel.add(messageGroupHandlingCheck);

        add(msgPanel);

        // --- S3 Event Notifications section ---
        JPanel s3Panel = new JPanel(new MigLayout("insets 8, fillx, wrap 1, hidemode 3"));
        s3Panel.setBackground(UIConstants.BACKGROUND_COLOR);
        s3Panel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "S3 Event Notifications", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        JPanel s3RadioPanel = new JPanel(new MigLayout("insets 0, gap 15"));
        s3RadioPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        s3RadioPanel.add(s3DisabledRadio);
        s3RadioPanel.add(s3ExtractRadio);
        s3RadioPanel.add(s3FetchRadio);
        s3Panel.add(s3RadioPanel, "growx");

        JPanel s3OptionsPanel = new JPanel(
                new MigLayout("insets 5, fillx, wrap 2, hidemode 3", "[right]10[grow,fill]"));
        s3OptionsPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        s3OptionsPanel.add(s3MaxSizeLabel);
        s3OptionsPanel.add(s3MaxSizeField, "width 200!");
        s3OptionsPanel.add(s3FileTypeLabel);
        s3OptionsPanel.add(s3FileTypeCombo, "width 200!");
        s3OptionsPanel.add(s3EncodingLabel);
        s3OptionsPanel.add(s3EncodingCombo, "width 200!");
        s3Panel.add(s3OptionsPanel, "growx");

        add(s3Panel);
    }
}
