package com.br.aws.ecommerce.order;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNS;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.model.OrderEnvelopeDTO;
import com.br.aws.ecommerce.layers.model.OrderEventDTO;
import com.br.aws.ecommerce.layers.repository.EventRepository;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrdersEventFunction extends BaseLambdaFunction implements RequestHandler<SNSEvent, String> {

	private Logger logger = Logger.getLogger(OrdersFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(SNSEvent input, Context context) {

		try {

		

			
			for (SNSRecord r : input.getRecords()) {
				
	             final SNS sns = r.getSNS();
	             
	         	this.logger.log(Level.INFO, "Incommig message id: {0}", sns.getMessageId());
				
				final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.getName())
						.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).build();

				final OrderEnvelopeDTO orderEnvelope = getMapper().readValue(sns.getMessage(), OrderEnvelopeDTO.class);
				
				final OrderEventDTO orderEvent = getMapper().readValue(orderEnvelope.getData(), OrderEventDTO.class);
				
				orderEvent.setMessageId(sns.getMessageId());

				final EventRepository eventRepository = new EventRepository(client);
				
				eventRepository.saveOrderEvent(orderEvent, orderEnvelope);
				
			}
			 
			
			return "OK";

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error %s: ", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

}
