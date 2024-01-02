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

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrderPaymentFunction extends BaseLambdaFunction<OrderEntity> implements RequestHandler<SNSEvent, String> {

	private Logger logger = Logger.getLogger(OrderPaymentFunction.class.getName());

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
				
				this.logger.log(Level.INFO, "Filtered SNS topic only ORDER_CREATED value: {0}", orderEnvelope.getEventType().getValue());
			
			}
			 
			
			return "OK";

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}

	}

}
