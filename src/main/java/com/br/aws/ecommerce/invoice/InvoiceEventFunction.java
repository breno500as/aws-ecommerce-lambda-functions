package com.br.aws.ecommerce.invoice;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.InvoiceEntity;
import com.br.aws.ecommerce.layers.model.InvoiceEventDTO;
import com.br.aws.ecommerce.layers.model.InvoiceTranscationStatus;
import com.br.aws.ecommerce.layers.repository.EventRepository;
import com.br.aws.ecommerce.layers.service.InvoiceWSService;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceEventFunction extends BaseLambdaFunction<InvoiceEntity>
		implements RequestHandler<DynamodbEvent, String> {

	private Logger logger = Logger.getLogger(InvoiceEventFunction.class.getName());

	private EventRepository eventRepository = new EventRepository(ClientsBean.getDynamoDbClient(),
			System.getenv(Constants.EVENTS_DDB));

	private InvoiceWSService apiGatewayService = new InvoiceWSService(ClientsBean.getApiGatewayClient());

	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(DynamodbEvent input, Context context) {
		try {

			this.logger.log(Level.INFO, "InvoiceEventFunction start");

			for (DynamodbStreamRecord dynamoDbRecord : input.getRecords()) {
				this.processDynamoDbRecord(dynamoDbRecord);
			}

			return "OK";

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

	private void processDynamoDbRecord(DynamodbStreamRecord dynamoDbRecord) {
		
		if ("INSERT".equals(dynamoDbRecord.getEventName())) {
			
			this.logger.log(Level.INFO, "Dynamodb INSERT");

			if (dynamoDbRecord.getDynamodb().getNewImage() != null
					&& dynamoDbRecord.getDynamodb().getNewImage().get("pk") != null) {

				if (dynamoDbRecord.getDynamodb().getNewImage().get("pk").getS().startsWith("#transaction")) {
					this.logger.log(Level.INFO, "Transaction received");
				} else {

					final Map<String, AttributeValue> a = dynamoDbRecord.getDynamodb().getNewImage();
					this.eventRepository.saveInvoiceEvent(new InvoiceEventDTO(a.get("sk").getS(), a.get("pk").getS(),
							a.get("productId").getS(), a.get("transactionId").getS(), "INVOICE_CREATED"));
				}

			}

		} else if ("MODIFY".equals(dynamoDbRecord.getEventName())) {
			this.logger.log(Level.INFO, "Dynamodb MODIFY");
		} else if ("REMOVE".equals(dynamoDbRecord.getEventName())) {
			this.logger.log(Level.INFO, "Dynamodb REMOVE");
			
			
			if (dynamoDbRecord.getDynamodb().getOldImage() != null
					&& dynamoDbRecord.getDynamodb().getOldImage().get("pk") != null) {
				
				if (dynamoDbRecord.getDynamodb().getOldImage().get("pk").getS().startsWith("#transaction")) {
					 this.processExpiredTransaction(dynamoDbRecord.getDynamodb().getOldImage());
				}  
				
			}
			
		}

	}

	private void processExpiredTransaction(Map<String, AttributeValue> oldImage) {
		final String transactionId = oldImage.get("sk").getS();
		final String connectionId = oldImage.get("connectionId").getS();
		
		this.logger.log(Level.INFO, String.format("transactionId: %s , connectionId: %s", transactionId, connectionId));
		
		if (oldImage.get("invoiceTranscationStatus").getS().equals(InvoiceTranscationStatus.PROCESSED.getValue())) {
			this.logger.log(Level.INFO, "Invoice processed");
		} else {
			this.apiGatewayService.sendInvoiceStatus(connectionId, transactionId, InvoiceTranscationStatus.TIMEOUT);
			this.apiGatewayService.disconnectClient(connectionId);
		}
	}
	
	 

}
