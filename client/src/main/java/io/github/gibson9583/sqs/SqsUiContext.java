/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import javax.swing.JComboBox;
import com.mirth.connect.client.ui.PlatformUI;
import com.mirth.connect.client.ui.AuthorizationControllerFactory;

/** Small host boundary so the real panels can be tested headlessly. */
interface SqsUiContext {
    default boolean canInspect() { return true; }
    boolean isSaveEnabled();
    void setSaveEnabled(boolean enabled);
    void setupEncoding(JComboBox<?> combo);
    void setEncoding(JComboBox<?> combo, String encoding);
    String getEncoding(JComboBox<?> combo);

    SqsUiContext HOST = new SqsUiContext() {
        public boolean canInspect() { return AuthorizationControllerFactory.getAuthorizationController().checkTask("channel", "doInspectSqsQueue"); }
        public boolean isSaveEnabled() { return PlatformUI.MIRTH_FRAME.isSaveEnabled(); }
        public void setSaveEnabled(boolean value) { PlatformUI.MIRTH_FRAME.setSaveEnabled(value); }
        public void setupEncoding(JComboBox<?> combo) { PlatformUI.MIRTH_FRAME.setupCharsetEncodingForConnector(combo); }
        public void setEncoding(JComboBox<?> combo, String value) { PlatformUI.MIRTH_FRAME.setPreviousSelectedEncodingForConnector(combo, value); }
        public String getEncoding(JComboBox<?> combo) { return PlatformUI.MIRTH_FRAME.getSelectedEncodingForConnector(combo); }
    };
}
