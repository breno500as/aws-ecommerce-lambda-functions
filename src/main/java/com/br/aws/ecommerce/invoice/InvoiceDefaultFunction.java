package com.br.aws.ecommerce.invoice;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.br.aws.ecommerce.layers.service.InvoiceWSService;
import com.br.aws.ecommerce.util.ClientsBean;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceDefaultFunction  implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

	private Logger logger = Logger.getLogger(InvoiceConnectionFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent input, Context context) {
		final APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
		
		final String connectionId = input.getRequestContext().getConnectionId();

		this.logger.log(Level.INFO, "InvoiceDefaultFunction start connectionId: {0}", connectionId);
		
		final InvoiceWSService service = new InvoiceWSService(ClientsBean.getApiGatewayClient());
		
		service.sendData(connectionId, "{\"message\": \"Velho vagabundo! Vai pra jaula\" }");

		response.setStatusCode(200);
	
		return response;
	}

}

