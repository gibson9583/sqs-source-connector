/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import java.awt.BorderLayout;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.function.BooleanSupplier;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingWorker;
import com.mirth.connect.client.ui.PlatformUI;
import com.mirth.connect.connectors.sqs.SqsConnectorServletInterface;
import com.mirth.connect.connectors.sqs.SqsDispatcherProperties;
import com.mirth.connect.connectors.sqs.SqsReceiverProperties;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.util.ConnectionTestResponse;

/** Explicit read-only inspection. Every edit/load/unmount invalidates pending results. */
final class SqsQueueInspectorPanel extends JPanel {
    interface Service { ConnectionTestResponse inspect(String id, String name, ConnectorProperties properties) throws Exception; }
    static final Service HOST = (id, name, p) -> PlatformUI.MIRTH_FRAME.mirthClient
            .getServlet(SqsConnectorServletInterface.class).inspectQueue(id, name, p);
    final JButton button = new JButton("Inspect Queue");
    final JTextArea result = new JTextArea(4, 40);
    private final Supplier<ConnectorProperties> properties;
    private final Supplier<String> channelId;
    private final Supplier<String> channelName;
    private final Service service;
    private final BooleanSupplier allowed;
    private long generation;
    private SwingWorker<ConnectionTestResponse, Void> worker;

    SqsQueueInspectorPanel(Supplier<ConnectorProperties> properties, Supplier<String> channelId, Supplier<String> channelName, Service service) {
        this(properties, channelId, channelName, service, () -> true);
    }

    SqsQueueInspectorPanel(Supplier<ConnectorProperties> properties, Supplier<String> channelId, Supplier<String> channelName, Service service, BooleanSupplier allowed) {
        super(new BorderLayout(6, 6));
        this.properties = properties; this.channelId = channelId; this.channelName = channelName; this.service = service; this.allowed = allowed;
        setOpaque(false);
        button.setToolTipText("Reads GetQueueAttributes for this URL only. Does not receive, send, or delete messages.");
        result.setEditable(false); result.setLineWrap(true); result.setWrapStyleWord(true);
        result.getAccessibleContext().setAccessibleName("Queue inspection result");
        add(button, BorderLayout.NORTH); add(new JScrollPane(result), BorderLayout.CENTER);
        button.addActionListener(e -> inspect());
    }

    static boolean ready(ConnectorProperties p) {
        String url, auth, key, secret, role;
        if (p instanceof SqsReceiverProperties) {
            SqsReceiverProperties s = (SqsReceiverProperties)p;
            url=s.getQueueUrl(); auth=s.getAuthType().name(); key=s.getAccessKeyId(); secret=s.getSecretAccessKey(); role=s.getRoleArn();
        } else if (p instanceof SqsDispatcherProperties) {
            SqsDispatcherProperties s = (SqsDispatcherProperties)p;
            url=s.getQueueUrl(); auth=s.getAuthType().name(); key=s.getAccessKeyId(); secret=s.getSecretAccessKey(); role=s.getRoleArn();
        } else return false;
        return !SqsPanelValidation.blank(url) && (!"STATIC".equals(auth) || (!SqsPanelValidation.blank(key) && !SqsPanelValidation.blank(secret)))
                && (!"ROLE".equals(auth) || !SqsPanelValidation.blank(role));
    }

    void invalidateResult() {
        generation++;
        if (worker != null) { worker.cancel(true); worker = null; }
        result.setText(""); button.setText("Inspect Queue"); button.setEnabled(allowed.getAsBoolean() && ready(properties.get()));
    }

    private void inspect() {
        if (!allowed.getAsBoolean()) { invalidateResult(); return; }
        ConnectorProperties snapshot = properties.get();
        if (!ready(snapshot)) { result.setText("Provide the queue URL and the selected authentication fields."); return; }
        final long ticket = ++generation;
        final String id = channelId.get(), name = channelName.get();
        button.setEnabled(false); button.setText("Inspecting queue…"); result.setText("");
        worker = new SwingWorker<ConnectionTestResponse, Void>() {
            protected ConnectionTestResponse doInBackground() throws Exception { return service.inspect(id, name, snapshot); }
            protected void done() {
                if (ticket != generation) return;
                if (!allowed.getAsBoolean()) { invalidateResult(); return; }
                try {
                    ConnectionTestResponse response = get();
                    result.setText(response == null ? "No inspection result received." : response.getMessage());
                } catch (CancellationException ignored) {
                    result.setText("");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); result.setText("Queue inspection interrupted.");
                } catch (ExecutionException e) {
                    // Server response carries sanitized details; do not echo transport exception
                    // bodies, which can include the submitted credential properties.
                    result.setText("Queue inspection failed. Check the engine connection and server logs.");
                } finally {
                    worker = null; button.setText("Inspect Queue"); button.setEnabled(allowed.getAsBoolean() && ready(properties.get()));
                }
            }
        };
        worker.execute();
    }

    @Override public void removeNotify() { invalidateResult(); super.removeNotify(); }
}
