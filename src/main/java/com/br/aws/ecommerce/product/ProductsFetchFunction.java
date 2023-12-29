package com.br.aws.ecommerce.product;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.model.ErrorMessageDTO;
import com.br.aws.ecommerce.layers.repository.ProductRepository;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

 

public class ProductsFetchFunction extends BaseLambdaFunction
		implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	
	private Logger logger = Logger.getLogger(ProductsFetchFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {

 
		final Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");

		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);

		try {

			// Identificador único associado a um cliente, independente do número de
			// requisições do cliente
			final String lamdaRequestId = context.getAwsRequestId();

			// Identificador de cada requisição HTTP
			final String apiRequestId = input.getRequestContext().getRequestId();

            this.logger.log(Level.INFO, String.format("apiRequestId: %s , lamdaRequestId: %s ", apiRequestId, lamdaRequestId));
			
			
		 	final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
		 	        .withRegion(Regions.US_EAST_1.getName())
		 	        .withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
		 	        .build();	
			
	 		final ProductRepository productRepository = new ProductRepository(client);
			
			
			if ("/products".equals(input.getResource())) {

				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(productRepository.findAll()));
				
				 

			} else if ("/products/{id}".equals(input.getResource())) {

				final String id = input.getPathParameters() != null ? input.getPathParameters().get("id") : null;

				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(productRepository.findById(id)));
			}

			this.logger.log(Level.WARNING, "Method not allowed");
			return response.withBody(super.getMapper().writeValueAsString(new ErrorMessageDTO("Method not allowed"))).withStatusCode(405);

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error %s: ",  e.getMessage()), e);
			return response.withBody(String.format("{ \"error\": \"%s\" }", e.getMessage())).withStatusCode(500);
		}

	}

}
