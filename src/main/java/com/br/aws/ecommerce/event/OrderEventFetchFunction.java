package com.br.aws.ecommerce.event;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.OrderEntity;
import com.br.aws.ecommerce.layers.repository.EventRepository;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrderEventFetchFunction extends BaseLambdaFunction<OrderEntity> implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	
	
	private Logger logger = Logger.getLogger(OrderEventFetchFunction.class.getName());
	
	private static final String EMAIL_KEY = "email";
	
	private static final String EVENT_TYPE_KEY = "eventType";

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

			final EventRepository eventRepository = new EventRepository(ClientsBean.getDynamoDbClient(), System.getenv(Constants.ORDERS_DDB));
			
			final Map<String, String> queryStringParams = input.getQueryStringParameters();
			   	 	
			final String email = queryStringParams.get(EMAIL_KEY);
				
			final String eventType =  queryStringParams.get(EVENT_TYPE_KEY);
			
			if (eventType == null) {
				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(eventRepository.findByEmail(email)));
			} else {
				return response.withStatusCode(200).withBody(super.getMapper().writeValueAsString(eventRepository.findByEmailAndEventType(email, eventType)));
			}
			

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s ", e.getMessage()), e);
			return response.withBody(String.format("{ \"error\": \"%s\" }", e.getMessage())).withStatusCode(500);
		}
	}
	
	
	 

}
