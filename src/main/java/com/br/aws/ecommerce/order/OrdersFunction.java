package com.br.aws.ecommerce.order;

import java.util.HashMap;
import java.util.List;
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
import com.br.aws.ecommerce.layers.entity.OrderEntity;
import com.br.aws.ecommerce.layers.entity.ProductEntity;
import com.br.aws.ecommerce.layers.model.ErrorMessageDTO;
import com.br.aws.ecommerce.layers.repository.OrderRepository;
import com.br.aws.ecommerce.layers.repository.ProductRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrdersFunction extends BaseLambdaFunction implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	private Logger logger = Logger.getLogger(OrdersFunction.class.getName());
	
	private static final String EMAIL_KEY = "email";
	
	private static final String ORDER_ID_KEY = "orderId";

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

			final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.getName())
					.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).build();

			final OrderRepository orderRepository = new OrderRepository(client);
			final ProductRepository productRepository = new ProductRepository(client);
			
			if ("GET".equals(input.getHttpMethod())) {
				return this.handleRequestRead(orderRepository,productRepository, input, response);
			} else {
				return this.handleRequestWrite(orderRepository, productRepository,  input, response);
			}

		 
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s ", e.getMessage()), e);
			return response.withBody(String.format("{ \"error\": \"%s\" }", e.getMessage())).withStatusCode(500);
		}
		
	}
	
	private APIGatewayProxyResponseEvent handleRequestRead(final OrderRepository orderRepository,
			ProductRepository productRepository,
			final APIGatewayProxyRequestEvent input, final APIGatewayProxyResponseEvent response)
			throws JsonProcessingException {
		
		
		final Map<String, String> queryStringParams = input.getQueryStringParameters();
		
		if (queryStringParams != null && !queryStringParams.isEmpty()) {
			
			final String email = queryStringParams.get(EMAIL_KEY);
			
			final String orderId = queryStringParams.get(ORDER_ID_KEY);
			
			if (email != null) {
				
				if (orderId != null) {
					
					
					final OrderEntity order = orderRepository.findByEmailAndOrderId(email, orderId);
					
					if (order == null) {
						return this.notFound(response);
					}
					
			
					return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(order));
				}
				
				final List<OrderEntity> orders = orderRepository.findByEmail(email);
				
				if (orders == null || orders.isEmpty()) {
					return this.notFound(response);
				}
				
				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(orders));
			} 

		} else {
			return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(orderRepository.findAll()));
		}
		
		
		return this.notFound(response);
	}

	private APIGatewayProxyResponseEvent handleRequestWrite(final OrderRepository orderRepository,
			ProductRepository productRepository,
			final APIGatewayProxyRequestEvent input, final APIGatewayProxyResponseEvent response)
			throws JsonMappingException, JsonProcessingException {
		
		
		final Map<String, String> queryStringParams = input.getQueryStringParameters();

		if ("POST".equals(input.getHttpMethod())) {

			final OrderEntity orderBody = getMapper().readValue(input.getBody(), OrderEntity.class);

			final List<String> idsProductsRequest = orderBody.getIdsProducts();

			final List<ProductEntity> products = productRepository.getByIds(orderBody.getIdsProducts());

			if (products == null || products.size() != idsProductsRequest.size()) {
				this.logger.log(Level.WARNING, "Different products size");
				return this.notFound(response);
			}

			orderBody.setProducts(products);

			final OrderEntity order = orderRepository.save(orderBody);

			return response.withStatusCode(201).withBody(getMapper().writeValueAsString(order));

		} else if ("DELETE".equals(input.getHttpMethod())) {

			final String email = queryStringParams.get(EMAIL_KEY);

			final String orderId = queryStringParams.get(ORDER_ID_KEY);
			
			if (email == null || orderId == null) {
				this.logger.log(Level.WARNING, "Email or orderId null for delete");
				return this.notFound(response);
			}

			orderRepository.deleteByEmailAndOrderId(email, orderId);

			return response.withStatusCode(204);
		}
		
		return this.notFound(response);
		
	}
	
	private APIGatewayProxyResponseEvent notFound(APIGatewayProxyResponseEvent response) throws JsonProcessingException {
		this.logger.log(Level.WARNING, "Not Found");
		return response.withBody(super.getMapper().writeValueAsString(new ErrorMessageDTO("Not Found")))
				.withStatusCode(404);
	}


}
