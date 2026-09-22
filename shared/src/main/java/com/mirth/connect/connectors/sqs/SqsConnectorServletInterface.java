/* SPDX-License-Identifier: MPL-2.0 */
package com.mirth.connect.connectors.sqs;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.mirth.connect.client.core.ClientException;
import com.mirth.connect.client.core.Operation.ExecuteType;
import com.mirth.connect.client.core.api.BaseServletInterface;
import com.mirth.connect.client.core.api.MirthOperation;
import com.mirth.connect.client.core.api.Param;
import com.mirth.connect.donkey.model.channel.ConnectorProperties;
import com.mirth.connect.util.ConnectionTestResponse;

/** Read-only queue inspection using the current, possibly unsaved connector settings. */
@Path("/connectors/sqs")
@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
public interface SqsConnectorServletInterface extends BaseServletInterface {
    String PLUGIN_POINT = "SQS Connector Service";
    String PERMISSION_INSPECT = "Inspect SQS Queue";

    @POST
    @Path("/_inspectQueue")
    @MirthOperation(name = "inspectSqsQueue", display = "Inspect SQS Queue",
            permission = PERMISSION_INSPECT, type = ExecuteType.ASYNC,
            auditable = false, abortable = true)
    ConnectionTestResponse inspectQueue(
            @Param("channelId") @QueryParam("channelId") String channelId,
            @Param("channelName") @QueryParam("channelName") String channelName,
            @Param("properties") ConnectorProperties properties) throws ClientException;
}
