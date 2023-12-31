package com.br.aws.ecommerce.invoice;

import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.model.PutEventsRequest;
import com.amazonaws.services.eventbridge.model.PutEventsRequestEntry;
import com.amazonaws.services.eventbridge.model.PutEventsResult;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.util.StringUtils;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.InvoiceEntity;
import com.br.aws.ecommerce.layers.entity.InvoiceTransactionEntity;
import com.br.aws.ecommerce.layers.model.InvoiceObjectDTO;
import com.br.aws.ecommerce.layers.model.InvoiceTranscationStatus;
import com.br.aws.ecommerce.layers.repository.InvoiceRepository;
import com.br.aws.ecommerce.layers.repository.InvoiceTranscationRepository;
import com.br.aws.ecommerce.layers.service.InvoiceWSService;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceImportFunction extends BaseLambdaFunction<InvoiceEntity> implements RequestHandler<S3Event, Boolean> {

	private Logger logger = Logger.getLogger(InvoiceImportFunction.class.getName());
	
	private AmazonS3 s3Client = ClientsBean.getS3Client();
	
	private AmazonDynamoDB dynamoDbClient = ClientsBean.getDynamoDbClient();

	private InvoiceTranscationRepository itRepository  = new InvoiceTranscationRepository(
			this.dynamoDbClient, System.getenv(Constants.INVOICE_DDB_KEY));
	
	private InvoiceRepository iRepository = new InvoiceRepository(this.dynamoDbClient,
			System.getenv(Constants.INVOICE_DDB_KEY));
	
	private InvoiceWSService apiGatewayService = new InvoiceWSService(ClientsBean.getApiGatewayClient());
	
	private AmazonEventBridge eventBridgeClient = ClientsBean.getEventBridgeClient();

	@Metrics
	@Logging
	@Tracing
	@Override
	public Boolean handleRequest(S3Event input, Context context) {
		
		try {
			
			this.logger.log(Level.INFO, "InvoiceImportFunction start");

			for (S3EventNotificationRecord s3Event : input.getRecords()) {
	            this.processS3Record(s3Event);
			}

			return true;
			
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}


	}
	
	private void processS3Record(S3EventNotificationRecord s3Event) throws InterruptedException, ExecutionException, 
	JsonMappingException, JsonProcessingException {
		
		final String key = s3Event.getS3().getObject().getKey();
		
		final String bucketName = s3Event.getS3().getBucket().getName();

		final InvoiceTransactionEntity i = this.itRepository.getByKey(key);

		if (i != null && InvoiceTranscationStatus.GENERATED.equals(i.getInvoiceTranscationStatus())) {

			this.sendMessageReceived(key, bucketName, i);
			
			final boolean objectProcessed = this.processInvoiceObject(key, bucketName, i);
			
			if (objectProcessed) {
				this.sendMessageProcessed(key, bucketName, i);
			}
		

		} else {
			apiGatewayService.sendInvoiceStatus(i.getConnectionId(), key , i.getInvoiceTranscationStatus());
			this.logger.log(Level.WARNING, "Non valid transaction status");
			return;
		}
	}
	
	private boolean processInvoiceObject(final String key, final String bucketName, InvoiceTransactionEntity i) throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
		this.logger.log(Level.INFO, "Process S3 invoice object");
		
		final String stringObjectS3 = this.s3Client.getObjectAsString(bucketName, key);
		
		if (StringUtils.isNullOrEmpty(stringObjectS3)) {
			this.logger.log(Level.WARNING, "Empty String Object");
			this.disconectClientInvalidInvoice(key, i);
			return false;
		}
		
		final InvoiceObjectDTO invoiceObject =  super.getMapper().readValue(stringObjectS3, InvoiceObjectDTO.class);
		
		if (invoiceObject.getInvoiceNumber() == null || invoiceObject.getInvoiceNumber().length() < 5) {
			this.logger.log(Level.WARNING, "Invalid invoice number");
			this.disconectClientInvalidInvoice(key, i);
			return false;
		}
		
		this.logger.log(Level.INFO, "Invoice Object: {0}", invoiceObject);
		
		final CompletableFuture<Void> createInvoiceTask = CompletableFuture.runAsync(() -> this.saveInvoice(invoiceObject, iRepository, key));

		final CompletableFuture<Void> deleteS3ObjectTask = CompletableFuture.runAsync(() -> this.s3Client.deleteObject(bucketName, key));
			
		final CompletableFuture<Void> invoiceFileTasks = CompletableFuture.allOf(createInvoiceTask, deleteS3ObjectTask);

		invoiceFileTasks.get();
		
		return true;

	}
	
	private void disconectClientInvalidInvoice(String key, InvoiceTransactionEntity i) throws InterruptedException, ExecutionException {
		
		final CompletableFuture<Void> eventBridgeTask = CompletableFuture.runAsync(() -> this.sendErrorToEventBridge("{\"reason\": \"" + System.getenv(Constants.FAIL_CHECK_INVOICE_KEY) + "\"}"));

		final CompletableFuture<Void> updateTransactionTask = CompletableFuture.runAsync(() -> this.itRepository.update(key, InvoiceTranscationStatus.NON_VALID_INVOICE_NUMBER));
		
		final CompletableFuture<Void> invalidInvoiceNumberTasks = CompletableFuture.allOf(eventBridgeTask, updateTransactionTask);

		invalidInvoiceNumberTasks.get();
		
		this.apiGatewayService.disconnectClient(i.getConnectionId());
	}
	
    private void sendMessageReceived(final String key, final String bucketName, final InvoiceTransactionEntity i) throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
    	this.logger.log(Level.INFO, "Send RECEIVED message to client: {0}", i.getConnectionId());
    	
    	final CompletableFuture<Boolean> sendMessageInvoiceReceivedTask = CompletableFuture.supplyAsync(() -> this.apiGatewayService
				.sendInvoiceStatus(i.getConnectionId(), key , InvoiceTranscationStatus.RECEIVED));

		final CompletableFuture<Void> receivedTransactionTask = CompletableFuture.runAsync(() -> {
			this.itRepository.update(key, InvoiceTranscationStatus.RECEIVED);
		});

		final CompletableFuture<Void> invoiceTransactionTasks = CompletableFuture.allOf(sendMessageInvoiceReceivedTask, receivedTransactionTask);

		invoiceTransactionTasks.get();

	}
	
     private void sendMessageProcessed(final String key, final String bucketName, final InvoiceTransactionEntity i) throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
    	this.logger.log(Level.INFO, "Send PROCESSED message to client: {0}", i.getConnectionId());
    	 
    	final CompletableFuture<Void> processedTransactionTask = CompletableFuture.runAsync(() -> this.itRepository.update(key, InvoiceTranscationStatus.PROCESSED));
			
		final CompletableFuture<Boolean> sendMessageInvoiceProcessedTask = CompletableFuture.supplyAsync(() -> this.apiGatewayService
					.sendInvoiceStatus(i.getConnectionId(), key , InvoiceTranscationStatus.PROCESSED));
			
		final CompletableFuture<Void> invoiceProcessedTasks = CompletableFuture.allOf(processedTransactionTask, sendMessageInvoiceProcessedTask);

		invoiceProcessedTasks.get();

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
	
	private void saveInvoice(InvoiceObjectDTO invoiceFile, InvoiceRepository iRepository, String key) {
		
		final InvoiceEntity i = new InvoiceEntity();
		i.setPk("#invoice_" + invoiceFile.getCustumerName());
		i.setSk(invoiceFile.getInvoiceNumber());
		i.setTtl(0);
		i.setTotalValue(invoiceFile.getTotalValue());
		i.setProductId(invoiceFile.getProductId());
		i.setQuantity(invoiceFile.getQuantity());
		i.setTransactionId(key);
		i.setCreatedAt(Instant.now().toEpochMilli());
		
		this.iRepository.save(i);
		
	}

}