package com.br.aws.ecommerce.product;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.CaptureMode;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductsFetchFunction
		implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

 

	@Tracing(captureMode = CaptureMode.DISABLED)
	@Metrics(captureColdStart = true)
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {

		LambdaLogger logger = context.getLogger();

		Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");

		// Identificador único associado a um cliente, independente do número de
		// requisições do cliente
		final String lamdaRequestId = context.getAwsRequestId();

		// Identificador de cada requisição HTTP
		final String apiRequestId = input.getRequestContext().getRequestId();
		logger.log(String.format("apiRequestId: %s lamdaRequestId: %s" , apiRequestId, lamdaRequestId));

		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);

		if ("/products".equals(input.getResource())) {

			String output = String.format("{ \"message\": \"velho vagabundo!\"}");

			return response.withStatusCode(200).withBody(output);

		} else if ("/products/{id}".equals(input.getResource())) {

			final String id = input.getPathParameters() != null ? input.getPathParameters().get("id") : null;

			String output = String.format("{ \"product_id\": \"%s\" }", id);

			return response.withStatusCode(200).withBody(output);
		}

		logger.log("Method not allowed");
		return response.withBody("{ \"error\": \"method not allowed\" }").withStatusCode(405);

	}

	 
 

}
