/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import static org.junit.jupiter.api.Assertions.*;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.swing.*;
import org.junit.jupiter.api.Test;
import com.mirth.connect.connectors.sqs.*;
import com.mirth.connect.util.ConnectionTestResponse;

class SqsPanelsTest {
    static final class Host implements SqsUiContext {
        boolean dirty;
        public boolean isSaveEnabled() { return dirty; }
        public void setSaveEnabled(boolean value) { dirty = value; }
        @SuppressWarnings({"rawtypes", "unchecked"}) public void setupEncoding(JComboBox<?> c) { ((JComboBox)c).addItem("DEFAULT_ENCODING"); ((JComboBox)c).addItem("UTF-8"); }
        public void setEncoding(JComboBox<?> c, String value) { c.setSelectedItem(value); }
        public String getEncoding(JComboBox<?> c) { return String.valueOf(c.getSelectedItem()); }
    }
    private static final SqsQueueInspectorPanel.Service NO_NETWORK = (id, name, p) -> { throw new AssertionError("Unexpected network call"); };
    static <T> T edt(Callable<T> task) throws Exception {
        CompletableFuture<T> future = new CompletableFuture<>();
        SwingUtilities.invokeAndWait(() -> { try { future.complete(task.call()); } catch (Throwable e) { future.completeExceptionally(e); } });
        return future.get(5, TimeUnit.SECONDS);
    }
    static <T> T field(Object target, String name, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(name); f.setAccessible(true); return type.cast(f.get(target));
    }

    @Test void receiverClearsEveryAuthenticationCardOnConnectorLoad() throws Exception {
        edt(() -> {
            Host host = new Host(); SqsReceiverPanel panel = new SqsReceiverPanel(host, NO_NETWORK);
            SqsReceiverProperties a = new SqsReceiverProperties(); a.setAuthType(SqsReceiverProperties.AuthType.STATIC); a.setAccessKeyId("a-key"); a.setSecretAccessKey("a-secret"); a.setRoleArn("a-role"); a.setExternalId("a-external");
            panel.setProperties(a); host.dirty = false;
            SqsReceiverProperties b = new SqsReceiverProperties(); b.setQueueUrl("https://example.invalid/b"); panel.setProperties(b);
            assertFalse(host.dirty);
            field(panel, "authStaticRadio", JRadioButton.class).doClick();
            SqsReceiverProperties after = (SqsReceiverProperties)panel.getProperties();
            assertEquals("", after.getAccessKeyId()); assertEquals("", after.getSecretAccessKey()); assertFalse(panel.checkProperties(after, true));
            field(panel, "authRoleRadio", JRadioButton.class).doClick(); after = (SqsReceiverProperties)panel.getProperties();
            assertEquals("", after.getRoleArn()); assertEquals("", after.getExternalId()); assertFalse(panel.checkProperties(after, true));
            assertTrue(host.dirty); return null;
        });
    }

    @Test void senderClearsAuthCardsAndRoundTripsTypedAttributesIncludingActiveCell() throws Exception {
        edt(() -> {
            Host host = new Host(); SqsSenderPanel panel = new SqsSenderPanel(host, NO_NETWORK);
            SqsDispatcherProperties a = new SqsDispatcherProperties(); a.setAuthType(SqsDispatcherProperties.AuthType.STATIC); a.setAccessKeyId("a-key"); a.setSecretAccessKey("a-secret"); a.setRoleArn("a-role"); a.setExternalId("a-external"); panel.setProperties(a);
            SqsDispatcherProperties b = new SqsDispatcherProperties(); b.setQueueUrl("https://example.invalid/b");
            b.setMessageAttributes(List.of(new SqsMessageAttribute("name", "String", "${tenant}"), new SqsMessageAttribute("count", "Number", "2"), new SqsMessageAttribute("bytes", "Binary", "YQ==")));
            host.dirty = false; panel.setProperties(b); assertFalse(host.dirty);
            field(panel, "authStaticRadio", JRadioButton.class).doClick();
            SqsDispatcherProperties after = (SqsDispatcherProperties)panel.getProperties();
            assertEquals("", after.getAccessKeyId()); assertEquals("", after.getSecretAccessKey());
            field(panel, "authRoleRadio", JRadioButton.class).doClick(); after = (SqsDispatcherProperties)panel.getProperties();
            assertEquals("", after.getRoleArn()); assertEquals("", after.getExternalId());
            field(panel, "authDefaultRadio", JRadioButton.class).doClick();
            JTable table = field(panel, "attributesTable", JTable.class); assertTrue(table.editCellAt(1, 2));
            ((JTextField)table.getEditorComponent()).setText("3");
            after = (SqsDispatcherProperties)panel.getProperties();
            assertEquals(3, after.getMessageAttributes().size()); assertEquals("3", after.getMessageAttributes().get(1).getValue());
            assertEquals("Number", after.getMessageAttributes().get(1).getDataType()); assertEquals("2", b.getMessageAttributes().get(1).getValue());
            assertTrue(panel.checkProperties(after, true));
            panel.setProperties(new SqsDispatcherProperties()); assertEquals(0, ((SqsDispatcherProperties)panel.getProperties()).getMessageAttributes().size());
            return null;
        });
    }

    @Test void panelsRejectLiteralRangesAndAcceptReplacementVariables() throws Exception {
        edt(() -> {
            SqsReceiverPanel reader = new SqsReceiverPanel(new Host(), NO_NETWORK);
            SqsReceiverProperties p = new SqsReceiverProperties(); p.setQueueUrl("${queueUrl}");
            assertTrue(reader.checkProperties(p, true));
            p.setWaitTimeSeconds("21"); assertFalse(reader.checkProperties(p, true));
            p.setWaitTimeSeconds("${wait}"); assertTrue(reader.checkProperties(p, true));
            p.setMaxMessages("bad"); assertFalse(reader.checkProperties(p, true));
            p.setMaxMessages("$maxMessages"); p.setVisibilityTimeout("43201"); assertFalse(reader.checkProperties(p, true));
            p.setVisibilityTimeout("$!{visibility}"); assertTrue(reader.checkProperties(p, true));
            p.setMessageGroupHandling(true); p.getSourceConnectorProperties().setRespondAfterProcessing(false); assertFalse(reader.checkProperties(p, true));
            p.getSourceConnectorProperties().setRespondAfterProcessing(true); p.getSourceConnectorProperties().setProcessingThreads(1); assertTrue(reader.checkProperties(p, true));
            p.setS3EventMode(SqsReceiverProperties.S3EventMode.FETCH_OBJECT);
            for (String size : List.of("", "0", "2147483648", "9007199254740991")) {
                p.setS3MaxObjectSizeKB(size); assertTrue(reader.checkProperties(p, true));
            }
            p.setS3MaxObjectSizeKB("9007199254740992"); assertFalse(reader.checkProperties(p, true));
            p.setS3MaxObjectSizeKB("10240");
            p.setS3EventMode(SqsReceiverProperties.S3EventMode.FETCH_OBJECT); p.setS3FileType("Binary"); p.getSourceConnectorProperties().setProcessBatch(true); assertFalse(reader.checkProperties(p, true));
            SqsSenderPanel sender = new SqsSenderPanel(new Host(), NO_NETWORK);
            SqsDispatcherProperties s = new SqsDispatcherProperties(); s.setQueueUrl("https://example.invalid/q"); s.setDelaySeconds("901"); assertFalse(sender.checkProperties(s, true));
            s.setDelaySeconds("${delay}"); assertTrue(sender.checkProperties(s, true));
            s.setQueueUrl("https://example.invalid/q.fifo"); s.setMessageGroupId("${group}"); assertFalse(sender.checkProperties(s, true));
            s.setDelaySeconds(""); assertTrue(sender.checkProperties(s, true));
            return null;
        });
    }

    @Test void attributeValidationRejectsDuplicatesMalformedNamesAndValues() {
        assertTrue(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("spaces", "String", " "))));
        assertFalse(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("key", null, "x"))));
        assertTrue(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("key", "Binary", "YQ"), new SqsMessageAttribute("count", "Number", "2.5e3"))));
        assertFalse(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("key", "String", "a"), new SqsMessageAttribute("key", "String", "b"))));
        assertFalse(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("AWS.test", "String", "x"))));
        assertFalse(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("key", "Binary", "!!!"))));
        assertFalse(SqsPanelValidation.attributes(List.of(new SqsMessageAttribute("key", "Number", "NaN"))));
    }

    @Test void inspectorValidatesConnectionOnlyAndSuppressesStaleSuccessAndError() throws Exception {
        for (boolean failure : List.of(false, true)) {
            CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1), finished = new CountDownLatch(1);
            AtomicInteger calls = new AtomicInteger(); SqsDispatcherProperties p = new SqsDispatcherProperties(); p.setQueueUrl("https://example.invalid/q"); p.setTemplate("");
            SqsQueueInspectorPanel inspector = edt(() -> {
                SqsQueueInspectorPanel i = new SqsQueueInspectorPanel(() -> p.clone(), () -> "channel", () -> "Channel", (id, name, snapshot) -> {
                    calls.incrementAndGet(); started.countDown();
                    // A service may complete even if cancellation cannot interrupt it.
                    while (release.getCount() > 0) try { release.await(); } catch (InterruptedException ignored) {}
                    finished.countDown();
                    if (failure) throw new IllegalStateException("stale transport error");
                    return new ConnectionTestResponse(ConnectionTestResponse.Type.SUCCESS, "stale success");
                });
                i.invalidateResult(); assertTrue(i.button.isEnabled()); assertEquals(0, calls.get()); i.button.doClick(); assertFalse(i.button.isEnabled()); return i;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            edt(() -> { p.setRegion("different"); inspector.invalidateResult(); assertEquals("", inspector.result.getText()); assertTrue(inspector.button.isEnabled()); return null; });
            release.countDown(); assertTrue(finished.await(5, TimeUnit.SECONDS));
            edt(() -> { assertEquals("", inspector.result.getText()); return null; });
        }
    }

    @Test void inspectorDisplaysCurrentSuccessAndClearsOnUnmount() throws Exception {
        CountDownLatch displayed = new CountDownLatch(1); SqsDispatcherProperties p = new SqsDispatcherProperties(); p.setQueueUrl("${queueUrl}");
        SqsQueueInspectorPanel inspector = edt(() -> {
            SqsQueueInspectorPanel i = new SqsQueueInspectorPanel(() -> p.clone(), () -> "a", () -> "A", (id, name, snapshot) -> new ConnectionTestResponse(ConnectionTestResponse.Type.SUCCESS, "GetQueueAttributes only"));
            i.result.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
                public void insertUpdate(javax.swing.event.DocumentEvent e) { displayed.countDown(); }
                public void removeUpdate(javax.swing.event.DocumentEvent e) {}
                public void changedUpdate(javax.swing.event.DocumentEvent e) {}
            });
            i.invalidateResult(); i.button.doClick(); return i;
        });
        assertTrue(displayed.await(5, TimeUnit.SECONDS));
        edt(() -> { assertEquals("GetQueueAttributes only", inspector.result.getText()); assertTrue(inspector.button.isEnabled()); inspector.removeNotify(); assertEquals("", inspector.result.getText()); return null; });
    }

    @Test void inspectorRechecksPermissionAtClickTime() throws Exception {
        edt(() -> {
            AtomicBoolean allowed = new AtomicBoolean(true);
            SqsDispatcherProperties p = new SqsDispatcherProperties(); p.setQueueUrl("https://example.invalid/q");
            SqsQueueInspectorPanel inspector = new SqsQueueInspectorPanel(() -> p, () -> "a", () -> "A", NO_NETWORK, allowed::get);
            inspector.invalidateResult(); assertTrue(inspector.button.isEnabled());
            allowed.set(false); inspector.button.doClick();
            assertFalse(inspector.button.isEnabled()); assertEquals("", inspector.result.getText());
            return null;
        });
    }
}
