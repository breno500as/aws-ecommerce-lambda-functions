package com.br.aws.ecommerce.product;

import static software.amazon.lambda.powertools.tracing.CaptureMode.DISABLED;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.model.ErrorMessageDTO;
import com.br.aws.ecommerce.layers.model.ProductDTO;
import com.br.aws.ecommerce.layers.repository.ProductRepository;

import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductsAdminFunction extends BaseLambdaFunction
		implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	

	@Tracing(captureMode = DISABLED)
	@Metrics(captureColdStart = true)
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
		
		final LambdaLogger logger = context.getLogger();
		
		Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");
		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);
		
		try {

			final ProductRepository productRepository = new ProductRepository(AmazonDynamoDBClientBuilder.defaultClient());
			
			if ("/products".equals(input.getResource())) {
				
				  final ProductDTO productBody = getMapper().readValue(input.getBody(), ProductDTO.class);	
				
				  return response.withStatusCode(201).withBody(getMapper().writeValueAsString(productRepository.save(productBody)));
				  
			} else if ("/products/{id}".equals(input.getResource())) {
				
				final String id = input.getPathParameters() != null ? input.getPathParameters().get("id") : null;
				
				if ("PUT".equals(input.getHttpMethod())) {
					  
					  final ProductDTO productBody = getMapper().readValue(input.getBody(), ProductDTO.class);	
						
					  return response.withStatusCode(200).withBody(getMapper().writeValueAsString(productRepository.update(productBody,id)));
					
				} else if ("DELETE".equals(input.getHttpMethod())) {
					
					productRepository.delete(id);
					
					return response.withStatusCode(204);
				}
			}
		 

			logger.log("Method not allowed");
			return response.withBody(super.getMapper().writeValueAsString(new ErrorMessageDTO("Method not allowed"))).withStatusCode(405);
			
			
		} catch (Exception e) {
			logger.log("Error: " + e.getMessage());
			return response.withBody(String.format("{ \"error\": \"%s\" }",e.getMessage())).withStatusCode(500);
		}
		
		
		 
	}

	 

}
