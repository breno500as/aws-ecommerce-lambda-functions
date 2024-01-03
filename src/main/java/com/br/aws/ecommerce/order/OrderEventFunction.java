package com.br.aws.ecommerce.order;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNS;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.OrderEntity;
import com.br.aws.ecommerce.layers.model.OrderEnvelopeDTO;
import com.br.aws.ecommerce.layers.model.OrderEventDTO;
import com.br.aws.ecommerce.layers.repository.EventRepository;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrderEventFunction extends BaseLambdaFunction<OrderEntity> implements RequestHandler<SNSEvent, String> {

	private Logger logger = Logger.getLogger(OrderFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(SNSEvent input, Context context) {

		try {
 
			for (SNSRecord r : input.getRecords()) {
				
	             final SNS sns = r.getSNS();
	             
	         	this.logger.log(Level.INFO, "Incommig message id: {0}", sns.getMessageId());

				final OrderEnvelopeDTO orderEnvelope = getMapper().readValue(sns.getMessage(), OrderEnvelopeDTO.class);
				
				final OrderEventDTO orderEvent = getMapper().readValue(orderEnvelope.getData(), OrderEventDTO.class);
				
				this.logger.log(Level.INFO, "Order type: {0}", orderEnvelope.getEventType().getValue());
				
				orderEvent.setMessageId(sns.getMessageId());

				final EventRepository eventRepository = new EventRepository(ClientsBean.getDynamoDbClient(), System.getenv(Constants.EVENTS_DDB));
				
				eventRepository.saveOrderEvent(orderEvent, orderEnvelope);
				
			}
			 
			
			return "OK";

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

}
