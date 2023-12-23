package com.br.aws.ecommerce.product;

import static software.amazon.lambda.powertools.tracing.CaptureMode.DISABLED;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductsAdminFunction
		implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	@Tracing(captureMode = DISABLED)
	@Metrics(captureColdStart = true)
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
		
		LambdaLogger logger = context.getLogger();
		
		Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");

		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);
		
		if ("/products".equals(input.getResource())) {
			
			  String output = String.format("{ \"message\": \"POST\"}");
			
			  return response.withStatusCode(201).withBody(output);
		} else if ("/products/{id}".equals(input.getResource())) {
			
			final String id = input.getPathParameters() != null ? input.getPathParameters().get("id") : null;
			
			if ("PUT".equals(input.getHttpMethod())) {
				   String output = String.format("{ \"message\": \"PUT\" }");
				
				 return response.withStatusCode(200).withBody(output);
				
			} else if ("DELETE".equals(input.getHttpMethod())) {
				   String output = String.format("{ \"message\": \"DELETE\" }");
				return response.withBody(output);
			}
			
		}
	 

		logger.log("Method not allowed");
		return response.withBody("{ \"error\": \"method not allowed\" }").withStatusCode(405);
		 
	}

	 

}
