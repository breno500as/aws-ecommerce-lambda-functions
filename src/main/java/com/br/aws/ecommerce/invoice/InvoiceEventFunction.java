package com.br.aws.ecommerce.invoice;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.model.PutEventsRequest;
import com.amazonaws.services.eventbridge.model.PutEventsRequestEntry;
import com.amazonaws.services.eventbridge.model.PutEventsResult;
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
		implements RequestHandler<DynamodbEvent, Boolean> {

	private Logger logger = Logger.getLogger(InvoiceEventFunction.class.getName());

	private EventRepository eventRepository = new EventRepository(ClientsBean.getDynamoDbClient(),
			System.getenv(Constants.EVENTS_DDB_KEY));

	private InvoiceWSService apiGatewayService = new InvoiceWSService(ClientsBean.getApiGatewayClient());
	
	private AmazonEventBridge eventBridgeClient = ClientsBean.getEventBridgeClient();

	@Tracing
	@Logging
	@Metrics
	@Override
	public Boolean handleRequest(DynamodbEvent input, Context context) {
		try {

			this.logger.log(Level.INFO, "InvoiceEventFunction start");

			for (DynamodbStreamRecord dynamoDbRecord : input.getRecords()) {
				this.processDynamoDbRecord(dynamoDbRecord);
			}

			return true;

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

	private void processDynamoDbRecord(DynamodbStreamRecord dynamoDbRecord) throws InterruptedException, ExecutionException {
		
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

	private void processExpiredTransaction(Map<String, AttributeValue> oldImage) throws InterruptedException, ExecutionException {
		final String transactionId = oldImage.get("sk").getS();
		final String connectionId = oldImage.get("connectionId").getS();
		
		this.logger.log(Level.INFO, String.format("transactionId: %s , connectionId: %s", transactionId, connectionId));
		
		if (oldImage.get("invoiceTranscationStatus").getS().equals(InvoiceTranscationStatus.PROCESSED.getValue())) {
			this.logger.log(Level.INFO, "Invoice processed");
		} else {
			
			final CompletableFuture<Void> sendTimoutStatusTask = CompletableFuture.runAsync(() -> this.apiGatewayService.sendInvoiceStatus(connectionId, transactionId, InvoiceTranscationStatus.TIMEOUT));

			final CompletableFuture<Void> sendTimeoutMessageTask = CompletableFuture.runAsync(() -> this.sendErrorToEventBridge(("{\"reason\": \"" + System.getenv(Constants.INVOICE_TIMEOUT_KEY) + "\"}")));
				
			final CompletableFuture<Void> timeoutInvoiceTasks = CompletableFuture.allOf(sendTimoutStatusTask, sendTimeoutMessageTask);

			timeoutInvoiceTasks.get();
				
			this.apiGatewayService.disconnectClient(connectionId);
		}
	}
	
	private void sendErrorToEventBridge(String jsonMessage) {
   	 
    	this.logger.log(Level.INFO, "EventBridge putEvents: {0}", jsonMessage);
    		
 		final PutEventsResult putEventsResult = this.eventBridgeClient.putEvents(new PutEventsRequest().withEntries(
 				new PutEventsRequestEntry()
 				.withSource(System.getenv(Constants.INVOICE_SOURCE_EVENT_BRIDGE_KEY))
 				.withEventBusName(System.getenv(Constants.AUDIT_EVENT_BRIDGE_KEY))
 				.withDetailType("invoice")
 				.withDetail(jsonMessage)
 				.withTime(new Date())));
 		
 		this.logger.log(Level.INFO, "EventBridge putEventsResult status code: {0}", putEventsResult.getSdkHttpMetadata().getHttpStatusCode());
 		
 	}
	
	 

}
