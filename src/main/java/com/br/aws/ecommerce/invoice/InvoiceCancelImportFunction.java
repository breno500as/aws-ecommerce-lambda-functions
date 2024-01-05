package com.br.aws.ecommerce.invoice;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.InvoiceEntity;
import com.br.aws.ecommerce.layers.entity.InvoiceTransactionEntity;
import com.br.aws.ecommerce.layers.model.InvoiceTranscationStatus;
import com.br.aws.ecommerce.layers.repository.InvoiceTranscationRepository;
import com.br.aws.ecommerce.layers.service.InvoiceWSService;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceCancelImportFunction extends BaseLambdaFunction<InvoiceEntity>
		implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

	private Logger logger = Logger.getLogger(InvoiceCancelImportFunction.class.getName());
	
	private InvoiceWSService apiGatewayService = new InvoiceWSService(ClientsBean.getApiGatewayClient());
	

	private InvoiceTranscationRepository itRepository = new InvoiceTranscationRepository(
			ClientsBean.getDynamoDbClient(), System.getenv(Constants.INVOICE_DDB_KEY));
	
 
	@Metrics
	@Logging
	@Tracing
	@Override
	public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent input, Context context) {
		
		try {
			
			this.logger.log(Level.INFO, "InvoiceCancelImportFunction start");
			
			final APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
			
			this.logger.log(Level.INFO, "input: {0}" , input);
			
			this.logger.log(Level.INFO, "body: {0}" , input.getBody());

			final String transactionId = getMapper().readTree(input.getBody()).get("transactionId").asText();
			
			final String connectionId = input.getRequestContext().getConnectionId();
			
			final InvoiceTransactionEntity i = this.itRepository.getByKey(transactionId);
			
			if (i == null) {
				this.logger.log(Level.SEVERE, String.format("InvoiceTransactionEntity not found: %s", transactionId));
			    apiGatewayService.sendData(connectionId, "{\"message\": \"transactionId "+transactionId+" not found! \"}");
			    
			    this.apiGatewayService.disconnectClient(connectionId);
			    
			    response.setStatusCode(417);
			    return response;
			}
			
			if (InvoiceTranscationStatus.GENERATED.equals(i.getInvoiceTranscationStatus())) {
				
				
				final CompletableFuture<Void> updateTransactionTask = CompletableFuture.runAsync(() -> this.itRepository.update(transactionId, InvoiceTranscationStatus.CANCELLED));

				final CompletableFuture<Void> successMessageTask = CompletableFuture.runAsync(() -> 	apiGatewayService.sendData(connectionId,
						"{\"message\": \"Success, transactionId " + transactionId + " "
						+ InvoiceTranscationStatus.CANCELLED.getValue() +"! \"}"));
				
				final CompletableFuture<Void> invoiceCancelledTasks = CompletableFuture.allOf(updateTransactionTask, successMessageTask);

				invoiceCancelledTasks.get();
				
				
				
			} else {
				this.logger.log(Level.SEVERE, String.format("InvoiceTransactionEntity not found: %s", transactionId));
				apiGatewayService.sendData(connectionId,
						"{\"message\": \"I can't cancell your transaction \" " + transactionId
								+ " because the status is not " + InvoiceTranscationStatus.GENERATED.getValue() + "! \"}");
				
				  this.apiGatewayService.disconnectClient(connectionId);
				
				 response.setStatusCode(417);
				 return response;
			}
		
			 
			
			response.setStatusCode(200);

			return response;
			
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
		
		
	}

}
