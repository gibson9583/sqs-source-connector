/* SPDX-License-Identifier: MPL-2.0 */
package io.github.gibson9583.sqs;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import com.mirth.connect.connectors.sqs.SqsConnectorServletInterface;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.server.api.MirthServlet;
import com.mirth.connect.util.ConnectionTestResponse;

public class SqsConnectorServlet extends MirthServlet implements SqsConnectorServletInterface {
    public SqsConnectorServlet(@Context HttpServletRequest request, @Context SecurityContext sc) {
        super(request, sc, PLUGIN_POINT);
    }

    @Override
    public ConnectionTestResponse inspectQueue(String channelId, String channelName,
            ConnectorProperties properties) {
        // Authorize before resolving channel-scoped variables or accessing AWS credentials.
        if (channelId == null || channelId.isBlank()) {
            checkUserAuthorized();
        } else {
            checkUserAuthorized(channelId);
        }
        return new SqsQueueInspector().inspect(channelId, channelName, properties);
    }
}
