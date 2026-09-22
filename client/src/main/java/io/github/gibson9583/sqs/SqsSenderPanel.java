/*
 * SPDX-License-Identifier: MPL-2.0
 */
package io.github.gibson9583.sqs;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JTable;
import javax.swing.DefaultCellEditor;
import javax.swing.table.DefaultTableModel;
import java.util.ArrayList;
import java.util.List;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.mirth.connect.client.ui.UIConstants;
import com.mirth.connect.client.ui.panels.connectors.ConnectorSettingsPanel;
import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsMessageAttribute;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;

import net.miginfocom.swing.MigLayout;

/**
 * Swing settings panel for the SQS destination connector.
 * <p>
 * All configurable fields are plain text fields so that users can enter OIE
 * replacement variables like {@code ${queueUrl}},
 * {@code ${message.encodedData}}, etc. The region dropdown is editable for
 * the same reason.
 * <p>
 * Displayed in the OIE Administrator when configuring a channel
 * with the SQS Sender destination connector.
 */
public class SqsSenderPanel extends ConnectorSettingsPanel {

    private final SqsUiContext ui;
    private SqsQueueInspectorPanel inspector;

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

    // --- Send Settings (text fields for Velocity substitution support) ---
    private JTextField delaySecondsField;
    private JTextField messageGroupIdField;
    private JTextField messageDeduplicationIdField;

    // --- Template ---
    private JTextArea templateTextArea;
    private JTable attributesTable;
    private DefaultTableModel attributesModel;
    private JButton addAttributeButton;

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

    public SqsSenderPanel() { this(SqsUiContext.HOST, SqsQueueInspectorPanel.HOST); }

    SqsSenderPanel(SqsUiContext ui, SqsQueueInspectorPanel.Service inspectionService) {
        this.ui = ui;
        initComponents();
        inspector = new SqsQueueInspectorPanel(this::getConnectionProperties, this::getChannelId, this::getChannelName, inspectionService, ui::canInspect);
        inspector.invalidateResult();
        initLayout();
    }

    private void changed() {
        ui.setSaveEnabled(true);
        if (inspector != null) inspector.invalidateResult();
    }

    // =========================================================================
    // ConnectorSettingsPanel overrides
    // =========================================================================

    @Override
    public String getConnectorName() {
        return new SqsDispatcherProperties().getName();
    }

    @Override
    public ConnectorProperties getProperties() {
        SqsDispatcherProperties props = getConnectionProperties();

        // Send Settings (stored as String for Velocity substitution)
        props.setDelaySeconds(delaySecondsField.getText().trim());
        props.setMessageGroupId(messageGroupIdField.getText().trim());
        props.setMessageDeduplicationId(messageDeduplicationIdField.getText().trim());

        // Commit the active cell before Save/Validate reads the table.
        if (attributesTable.isEditing()) attributesTable.getCellEditor().stopCellEditing();
        List<SqsMessageAttribute> attributes = new ArrayList<>();
        for (int row = 0; row < attributesModel.getRowCount(); row++) {
            attributes.add(new SqsMessageAttribute(cell(row, 0), cell(row, 1), cell(row, 2)));
        }
        props.setMessageAttributes(attributes);

        // Template
        props.setTemplate(templateTextArea.getText());

        return props;
    }

    private String cell(int row, int column) {
        Object value = attributesModel.getValueAt(row, column);
        return value == null ? "" : value.toString();
    }

    private SqsDispatcherProperties getConnectionProperties() {
        SqsDispatcherProperties props = new SqsDispatcherProperties();

        // AWS Connection
        props.setQueueUrl(queueUrlField.getText().trim());
        // Editable combo: user may have typed a variable like ${region}
        Object regionSelection = regionCombo.getEditor().getItem();
        props.setRegion(regionSelection != null ? regionSelection.toString().trim() : "");

        // Auth
        if (authStaticRadio.isSelected()) {
            props.setAuthType(SqsDispatcherProperties.AuthType.STATIC);
            props.setAccessKeyId(accessKeyIdField.getText().trim());
            props.setSecretAccessKey(new String(secretAccessKeyField.getPassword()));
        } else if (authRoleRadio.isSelected()) {
            props.setAuthType(SqsDispatcherProperties.AuthType.ROLE);
            props.setRoleArn(roleArnField.getText().trim());
            props.setExternalId(externalIdField.getText().trim());
        } else {
            props.setAuthType(SqsDispatcherProperties.AuthType.DEFAULT);
        }

        return props;
    }

    @Override
    public void setProperties(ConnectorProperties properties) {
        SqsDispatcherProperties props = (SqsDispatcherProperties) properties;

        // Preserve save state so that populating fields doesn't falsely trigger dirty
        boolean saveEnabled = ui.isSaveEnabled();

        // AWS Connection
        queueUrlField.setText(props.getQueueUrl());
        regionCombo.getEditor().setItem(props.getRegion());
        regionCombo.setSelectedItem(props.getRegion());

        // Auth
        // Panels are shared across connectors: overwrite every card on every load.
        accessKeyIdField.setText(props.getAccessKeyId());
        secretAccessKeyField.setText(props.getSecretAccessKey());
        roleArnField.setText(props.getRoleArn());
        externalIdField.setText(props.getExternalId());
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

        // Send Settings (plain text — may contain Velocity expressions)
        delaySecondsField.setText(props.getDelaySeconds());
        messageGroupIdField.setText(props.getMessageGroupId());
        messageDeduplicationIdField.setText(props.getMessageDeduplicationId());

        if (attributesTable.isEditing()) attributesTable.getCellEditor().cancelCellEditing();
        attributesModel.setRowCount(0);
        for (SqsMessageAttribute attribute : props.getMessageAttributes()) {
            attributesModel.addRow(attribute == null ? new Object[] { "", "String", "" }
                    : new Object[] { attribute.getName(), attribute.getDataType(), attribute.getValue() });
        }
        addAttributeButton.setEnabled(attributesModel.getRowCount() < 10);
        // Template
        templateTextArea.setText(props.getTemplate());

        if (inspector != null) inspector.invalidateResult();
        // Restore save state
        ui.setSaveEnabled(saveEnabled);
    }

    @Override
    public ConnectorProperties getDefaults() {
        return new SqsDispatcherProperties();
    }

    @Override
    public boolean checkProperties(ConnectorProperties properties, boolean highlight) {
        SqsDispatcherProperties props = (SqsDispatcherProperties) properties;
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

        // Template is required
        if (props.getTemplate() == null || props.getTemplate().isBlank()) {
            valid = false;
            if (highlight) {
                templateTextArea.setBackground(UIConstants.INVALID_COLOR);
            }
        } else {
            templateTextArea.setBackground(Color.WHITE);
        }

        // Static auth requires key and secret
        if (props.getAuthType() == SqsDispatcherProperties.AuthType.STATIC) {
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
        if (props.getAuthType() == SqsDispatcherProperties.AuthType.ROLE) {
            if (props.getRoleArn() == null || props.getRoleArn().isBlank()) {
                valid = false;
                if (highlight) {
                    roleArnField.setBackground(UIConstants.INVALID_COLOR);
                }
            } else {
                roleArnField.setBackground(null);
            }
        }

        boolean delayValid = SqsPanelValidation.integer(props.getDelaySeconds(), 0, 900, true);
        boolean groupValid = true;
        if (!SqsPanelValidation.expression(props.getQueueUrl()) && props.getQueueUrl() != null && props.getQueueUrl().endsWith(".fifo")) {
            groupValid = !SqsPanelValidation.blank(props.getMessageGroupId());
            delayValid &= SqsPanelValidation.blank(props.getDelaySeconds());
        }
        boolean dedupValid = SqsPanelValidation.expression(props.getQueueUrl()) || SqsPanelValidation.blank(props.getQueueUrl())
                || props.getQueueUrl().endsWith(".fifo") || SqsPanelValidation.blank(props.getMessageDeduplicationId());
        boolean attributesValid = SqsPanelValidation.attributes(props.getMessageAttributes());
        if (highlight) {
            delaySecondsField.setBackground(delayValid ? null : UIConstants.INVALID_COLOR);
            messageGroupIdField.setBackground(groupValid ? null : UIConstants.INVALID_COLOR);
            messageDeduplicationIdField.setBackground(dedupValid ? null : UIConstants.INVALID_COLOR);
            attributesTable.setBackground(attributesValid ? Color.WHITE : UIConstants.INVALID_COLOR);
        }
        return valid && delayValid && groupValid && dedupValid && attributesValid;
    }

    @Override
    public void resetInvalidProperties() {
        queueUrlField.setBackground(null);
        accessKeyIdField.setBackground(null);
        secretAccessKeyField.setBackground(null);
        roleArnField.setBackground(null);
        templateTextArea.setBackground(Color.WHITE);
        delaySecondsField.setBackground(null);
        messageGroupIdField.setBackground(null);
        attributesTable.setBackground(Color.WHITE);
        messageDeduplicationIdField.setBackground(null);
    }

    // =========================================================================
    // UI Component Initialization
    // =========================================================================

    private void initComponents() {
        String velocityHint = " — supports replacement variables e.g. ${key}";

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

        // Send settings — plain text fields for Velocity substitution support
        delaySecondsField = new JTextField("");
        delaySecondsField.setToolTipText(
                "Optional delivery delay in seconds (0-900). Leave blank to use the queue default. "
                        + "Not supported on FIFO queues." + velocityHint);

        messageGroupIdField = new JTextField("");
        messageGroupIdField.setToolTipText(
                "Required for FIFO queues; optional fair-queue tenant grouping for standard queues."
                        + velocityHint);

        messageDeduplicationIdField = new JTextField("");
        messageDeduplicationIdField.setToolTipText(
                "Optional message deduplication ID for FIFO queues. Leave blank if the queue "
                        + "uses content-based deduplication." + velocityHint);

        // Template
        templateTextArea = new JTextArea(8, 40);
        templateTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        templateTextArea.setLineWrap(true);
        // Default L&F gives the text area a grey background; other connectors
        // use a white content area, so set it explicitly.
        templateTextArea.setBackground(Color.WHITE);
        templateTextArea.setToolTipText(
                "SQS message body to send. Use ${message.encodedData} for the transformed "
                        + "channel message, or ${message.rawData} for the original inbound message."
                        + velocityHint);

        attributesModel = new DefaultTableModel(new Object[] { "Name", "Data Type", "Value" }, 0);
        attributesTable = new JTable(attributesModel);
        attributesTable.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);
        attributesTable.getColumnModel().getColumn(1).setCellEditor(new DefaultCellEditor(new JComboBox<>(new String[] { "String", "Number", "Binary" })));
        attributesTable.setToolTipText("Up to 10 attributes. Names and values support ${key}. Binary values use Base64.");
        addAttributeButton = new JButton("Add Attribute");
        addAttributeButton.addActionListener(e -> {
            if (attributesTable.isEditing()) attributesTable.getCellEditor().stopCellEditing();
            if (attributesModel.getRowCount() < 10) attributesModel.addRow(new Object[] { "", "String", "" });
        });
        attributesModel.addTableModelListener(e -> {
            addAttributeButton.setEnabled(attributesModel.getRowCount() < 10);
            changed();
        });

        // --- Change notification for all components ---
        // OIE requires explicit save notification; text fields use DocumentListener,
        // radio buttons use ActionListener.
        DocumentListener saveDocListener = new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) { changed(); }
            @Override
            public void removeUpdate(DocumentEvent e) { changed(); }
            @Override
            public void changedUpdate(DocumentEvent e) { changed(); }
        };
        ActionListener saveActionListener = (ActionEvent e) -> changed();

        // Text fields
        queueUrlField.getDocument().addDocumentListener(saveDocListener);
        accessKeyIdField.getDocument().addDocumentListener(saveDocListener);
        secretAccessKeyField.getDocument().addDocumentListener(saveDocListener);
        roleArnField.getDocument().addDocumentListener(saveDocListener);
        externalIdField.getDocument().addDocumentListener(saveDocListener);
        delaySecondsField.getDocument().addDocumentListener(saveDocListener);
        messageGroupIdField.getDocument().addDocumentListener(saveDocListener);
        messageDeduplicationIdField.getDocument().addDocumentListener(saveDocListener);
        templateTextArea.getDocument().addDocumentListener(saveDocListener);

        // Editable combo box
        JTextField regionEditor = (JTextField) regionCombo.getEditor().getEditorComponent();
        regionEditor.getDocument().addDocumentListener(saveDocListener);
        regionCombo.addActionListener(saveActionListener);

        // Radio buttons
        authDefaultRadio.addActionListener(saveActionListener);
        authStaticRadio.addActionListener(saveActionListener);
        authRoleRadio.addActionListener(saveActionListener);
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
        add(inspector);

        // --- Send Settings section ---
        JPanel sendPanel = new JPanel(
                new MigLayout("insets 8, fillx, wrap 2", "[right]10[grow,fill]"));
        sendPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        sendPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "Send Settings", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        sendPanel.add(new JLabel("Delay Seconds (optional):"));
        sendPanel.add(delaySecondsField, "width 200!");
        sendPanel.add(new JLabel("Message Group ID:"));
        sendPanel.add(messageGroupIdField, "width 300!");
        sendPanel.add(new JLabel("FIFO Deduplication ID (optional):"));
        sendPanel.add(messageDeduplicationIdField, "width 300!");

        add(sendPanel);

        JPanel attributesPanel = new JPanel(new MigLayout("insets 8, fill, wrap 1", "[grow,fill]"));
        attributesPanel.setBackground(UIConstants.BACKGROUND_COLOR);
        attributesPanel.setBorder(BorderFactory.createTitledBorder("Message Attributes"));
        attributesPanel.add(new JLabel("Up to 10 attributes. Names/values support ${key}; Binary values use Base64."));
        attributesPanel.add(new JScrollPane(attributesTable), "grow, height 100:140:");
        JPanel attributeButtons = new JPanel();
        attributeButtons.setOpaque(false);
        attributeButtons.add(addAttributeButton);
        JButton removeAttributeButton = new JButton("Remove Selected");
        removeAttributeButton.addActionListener(e -> {
            if (attributesTable.isEditing()) attributesTable.getCellEditor().stopCellEditing();
            int[] rows = attributesTable.getSelectedRows();
            for (int i = rows.length - 1; i >= 0; i--) attributesModel.removeRow(rows[i]);
        });
        attributeButtons.add(removeAttributeButton);
        attributesPanel.add(attributeButtons);
        add(attributesPanel);

        // --- Template section ---
        JPanel templatePanel = new JPanel(new MigLayout("insets 8, fill, wrap 1"));
        templatePanel.setBackground(UIConstants.BACKGROUND_COLOR);
        templatePanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createLineBorder(new Color(180, 180, 180)),
                "Message Body Template", TitledBorder.DEFAULT_JUSTIFICATION,
                TitledBorder.DEFAULT_POSITION));

        templatePanel.add(new JScrollPane(templateTextArea), "grow, height 120:160:");

        add(templatePanel);
    }
}
