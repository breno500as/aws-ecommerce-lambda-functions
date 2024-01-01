package com.br.aws.ecommerce.order;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.model.OrderEnvelopeDTO;
import com.br.aws.ecommerce.layers.model.OrderEventDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class OrderEmailFunction extends BaseLambdaFunction implements RequestHandler<SQSEvent, String> {
	
	private Logger logger = Logger.getLogger(OrderEmailFunction.class.getName());

	@SuppressWarnings("unchecked")
	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(SQSEvent input, Context context) {

		try {

			for (SQSMessage m : input.getRecords()) {

				final String messageBody = m.getBody();
				
 
				final Map<String,String> mapSnsEvent = getMapper().readValue(messageBody, HashMap.class);
				
				final String snsMessage = mapSnsEvent.get("Message");

				this.logger.log(Level.INFO, "SNS Message: {0}", snsMessage);

			//	this.sendEmail(snsMessage);

			}

			return "OK";

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}

	}
	
	private void sendEmail(final String snsMessage) throws JsonMappingException, JsonProcessingException {
		
		final OrderEnvelopeDTO orderEnvelope = getMapper().readValue(snsMessage, OrderEnvelopeDTO.class);
		
		final OrderEventDTO orderEvent = getMapper().readValue(orderEnvelope.getData(), OrderEventDTO.class);
		
		final AmazonSimpleEmailService client = 
		          AmazonSimpleEmailServiceClientBuilder.standard()
		            .withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
		            .withRegion(Regions.US_EAST_1).build();
		
		final SendEmailRequest request = new SendEmailRequest()
		          .withDestination(
		              new Destination().withToAddresses(orderEvent.getEmail()))
		          .withMessage(new Message()
		              .withBody(new Body()
		                  .withHtml(new Content()
		                      .withCharset("UTF-8").withData(String.format("<p>Recebemos seu pedido: <strong>%s</strong></p>",orderEvent.getOrderId()))))
		              .withSubject(new Content()
		                  .withCharset("UTF-8").withData("Pedido Recebido!")))
		          .withSource("breno500as@gmail.com");
		  
		 client.sendEmail(request);
		
	}

}