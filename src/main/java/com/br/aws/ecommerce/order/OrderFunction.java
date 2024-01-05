package com.br.aws.ecommerce.order;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.OrderEntity;
import com.br.aws.ecommerce.layers.entity.ProductEntity;
import com.br.aws.ecommerce.layers.model.ErrorMessageDTO;
import com.br.aws.ecommerce.layers.model.OrderEnvelopeDTO;
import com.br.aws.ecommerce.layers.model.OrderEventDTO;
import com.br.aws.ecommerce.layers.model.OrderEventTypeEnum;
import com.br.aws.ecommerce.layers.repository.OrderRepository;
import com.br.aws.ecommerce.layers.repository.ProductRepository;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrderFunction extends BaseLambdaFunction<OrderEntity> implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	private Logger logger = Logger.getLogger(OrderFunction.class.getName());
	
	private static final String ORDER_EVENTS_TOPIC_ARN = "ORDER_EVENTS_TOPIC_ARN";
	
	private static final String EMAIL_KEY = "email";
	
	private static final String ORDER_ID_KEY = "orderId";
	
	private static final String PRODUCTS_KEY = "products";
	
	final AmazonDynamoDB dynamoDbClient = ClientsBean.getDynamoDbClient();
	
	final OrderRepository orderRepository = new OrderRepository(this.dynamoDbClient, System.getenv(Constants.ORDERS_DDB));
	
	final ProductRepository productRepository = new ProductRepository(this.dynamoDbClient, System.getenv(Constants.PRODUCTS_DDB));

	@Tracing
	@Logging
	@Metrics
	@Override
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
		
		final Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");
		
		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);
		
		final String lamdaRequestId = context.getAwsRequestId();
		
		try {

			
			if ("GET".equals(input.getHttpMethod())) {
				return this.handleRequestRead(input, response);
			} else {
				return this.handleRequestWrite(input, response, lamdaRequestId);
			}

		 
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s ", e.getMessage()), e);
			return response.withBody(String.format("{ \"error\": \"%s\" }", e.getMessage())).withStatusCode(500);
		}
		
	}
	
	private APIGatewayProxyResponseEvent handleRequestRead(
			final APIGatewayProxyRequestEvent input, final APIGatewayProxyResponseEvent response)
			throws JsonProcessingException {
		
		
		    final Map<String, String> queryStringParams = input.getQueryStringParameters();
		   	 
			
			final String email = queryStringParams != null ? queryStringParams.get(EMAIL_KEY) : null;
			
			final String orderId = queryStringParams != null ? queryStringParams.get(ORDER_ID_KEY) : null;
			
			final String products = queryStringParams != null ? queryStringParams.get(PRODUCTS_KEY) : null;
			
			if (email != null) {
				
				if (orderId != null) {
					
					final OrderEntity order = this.orderRepository.findByEmailAndOrderId(email, orderId);
					
					if (order == null) {
						return this.notFound(response);
					}
					
			
					return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(order));
				}
				
				final List<OrderEntity> orders = this.orderRepository.findByEmail(email);
				
				if (orders == null || orders.isEmpty()) {
					return this.notFound(response);
				}
				
				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(orders));
			} 

		 
			return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(this.orderRepository.findAll("S".equals(products) ? true : false)));
		 
	}

	private APIGatewayProxyResponseEvent handleRequestWrite(
			final APIGatewayProxyRequestEvent input, final APIGatewayProxyResponseEvent response, String lamdaRequestId)
			throws JsonMappingException, JsonProcessingException, InterruptedException, ExecutionException {
		
		
		final Map<String, String> queryStringParams = input.getQueryStringParameters();

		if ("POST".equals(input.getHttpMethod())) {

			final OrderEntity orderBody = getMapper().readValue(input.getBody(), OrderEntity.class);

			final List<String> idsProductsRequest = orderBody.getIdsProducts();

			final List<ProductEntity> products = this.productRepository.getByIds(orderBody.getIdsProducts());

			if (products == null || products.size() != idsProductsRequest.size()) {
				this.logger.log(Level.WARNING, "Different products size");
				return this.notFound(response);
			}

			orderBody.setProducts(products);
						
			final CompletableFuture<OrderEntity> dynamoTask = CompletableFuture.supplyAsync(() -> 
			       this.orderRepository.save(orderBody)
			);

			 
			final CompletableFuture<Void> snsTask = CompletableFuture.runAsync(() -> {
				try {
					this.publishSnsNotification(orderBody, OrderEventTypeEnum.CREATED, lamdaRequestId);
				} catch (JsonProcessingException e) {
					throw new RuntimeException(e);
				}
			});
			
			// https://stackoverflow.com/questions/34211080/how-do-you-access-completed-futures-passed-to-completablefuture-allof
		 
			final CompletableFuture<Void> allTasks = CompletableFuture.allOf(dynamoTask, snsTask);
			
			allTasks.get();  
			
		
			return response.withStatusCode(201).withBody(getMapper().writeValueAsString(dynamoTask.get()));

		} else if ("DELETE".equals(input.getHttpMethod())) {

			final String email = queryStringParams.get(EMAIL_KEY);

			final String orderId = queryStringParams.get(ORDER_ID_KEY);
			
			if (email == null || orderId == null) {
				this.logger.log(Level.WARNING, "Email or orderId null for delete");
				return this.notFound(response);
			}

			this.orderRepository.deleteByEmailAndOrderId(email, orderId);
			
			final OrderEntity order = new OrderEntity();
			order.setPk(email);
			order.setSk(orderId);
			
			this.publishSnsNotification(order, OrderEventTypeEnum.DELETED, lamdaRequestId);

			return response.withStatusCode(204);
		}
		
		return this.notFound(response);
		
	}
	
 
	
	private APIGatewayProxyResponseEvent notFound(APIGatewayProxyResponseEvent response) throws JsonProcessingException {
		this.logger.log(Level.WARNING, "Not Found");
		return response.withBody(super.getMapper().writeValueAsString(new ErrorMessageDTO("Not Found")))
				.withStatusCode(404);
	}
	
	private void publishSnsNotification(final OrderEntity order, final OrderEventTypeEnum orderEventType, String lambdaRequestId) throws JsonProcessingException {
		
		final OrderEventDTO orderEvent = new OrderEventDTO();
		orderEvent.setBilling(order.getBilling());
		orderEvent.setEmail(order.getPk());
		orderEvent.setOrderId(order.getSk());
		orderEvent.setRequestId(lambdaRequestId);
		orderEvent.setProducts(order.getProducts());
		
		final OrderEnvelopeDTO orderEnvelope = new OrderEnvelopeDTO();
		orderEnvelope.setEventType(orderEventType);
		orderEnvelope.setData( getMapper().writeValueAsString(orderEvent));
		
		final AmazonSNS snsClient = AmazonSNSClientBuilder
				                    .standard()
				                    .withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
				                    .withRegion(Regions.US_EAST_1.getName())
				                    .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
		
		final Map<String, MessageAttributeValue> attributes = new HashMap<>();
		attributes.put("eventType", new MessageAttributeValue()
				.withDataType("String")
				.withStringValue(orderEventType.getValue()));
		
	 
		final PublishRequest request = new PublishRequest()
		        .withTopicArn(System.getenv(ORDER_EVENTS_TOPIC_ARN))
		        .withMessage(getMapper().writeValueAsString(orderEnvelope))
		        .withMessageAttributes(attributes);	         
		
		final PublishResult publishResult = snsClient.publish(request);
		
		this.logger.log(Level.INFO, String.format("Message id: %s , Lambda request id: %s", publishResult.getMessageId(), lambdaRequestId));
	}


}
