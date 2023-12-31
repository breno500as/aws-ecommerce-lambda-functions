package com.br.aws.ecommerce.invoice;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceDisconnectionFunction
		implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

	private Logger logger = Logger.getLogger(InvoiceDisconnectionFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent input, Context context) {

		final APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();

		this.logger.log(Level.INFO, "InvoiceDisconnectionFunction start connectionId: {0}",
				input.getRequestContext().getConnectionId());

		response.setStatusCode(200);

		return response;
	}

}
