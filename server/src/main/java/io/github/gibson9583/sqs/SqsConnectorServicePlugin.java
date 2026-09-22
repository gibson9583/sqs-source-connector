/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import java.util.Properties;
import com.mirth.connect.connectors.sqs.SqsConnectorServletInterface;
import com.mirth.connect.model.ExtensionPermission;
import com.mirth.connect.plugins.ServicePlugin;

/** Registers the inspection permission with the engine and authorization plugins. */
public class SqsConnectorServicePlugin implements ServicePlugin {
    @Override public String getPluginPointName() { return SqsConnectorServletInterface.PLUGIN_POINT; }
    @Override public void init(Properties properties) {}
    @Override public void start() {}
    @Override public void stop() {}
    @Override public void update(Properties properties) {}
    @Override public Properties getDefaultProperties() { return new Properties(); }

    @Override
    public ExtensionPermission[] getExtensionPermissions() {
        return new ExtensionPermission[] { new ExtensionPermission(getPluginPointName(),
                SqsConnectorServletInterface.PERMISSION_INSPECT,
                "Inspect an SQS queue using the connector's AWS credentials",
                new String[] { "inspectSqsQueue" }, new String[] { "doInspectSqsQueue" }) };
    }
}
