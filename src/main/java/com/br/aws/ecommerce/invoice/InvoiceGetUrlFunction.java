package com.br.aws.ecommerce.invoice;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.InvoiceTransactionEntity;
import com.br.aws.ecommerce.layers.model.InvoiceTranscationStatus;
import com.br.aws.ecommerce.layers.repository.InvoiceTranscationRepository;
import com.br.aws.ecommerce.layers.service.InvoiceWSService;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceGetUrlFunction extends BaseLambdaFunction<InvoiceTransactionEntity> implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

	private Logger logger = Logger.getLogger(InvoiceGetUrlFunction.class.getName());

	private static final String S3_BUCKET_KEY = "S3_BUCKET_KEY";
	
	@Metrics
	@Logging
	@Tracing
	@Override
	public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent input, Context context) {
		final APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();

		final String connectionId = input.getRequestContext().getConnectionId();

		this.logger.log(Level.INFO, "InvoiceGetUrlFunction start with connectionId: {0}", connectionId);
		
		final String fileName = UUID.randomUUID().toString();

		final String urlPresigned = this.generatePresignedUrl(fileName, HttpMethod.PUT);
		
		final InvoiceTransactionEntity i = this.createInvoiceTransaction(connectionId, context.getAwsRequestId(), fileName);
		
		final InvoiceWSService invoiceWSService = new InvoiceWSService(ClientsBean.getApiGatewayClient());
		
		invoiceWSService.sendData(connectionId, "{\"url\": \"" + urlPresigned + "\""
					+ ",\"trascationId\": \"" + i.getSk() + "\""
			        + ",\"expiresIn\": \"" + i.getExpiresIn() + "\"}");
		
 
		response.setStatusCode(200);

		return response;
	}
	
	private InvoiceTransactionEntity createInvoiceTransaction(String connectionId, String awsRequestId, String fileName) {
		
		final InvoiceTranscationRepository invoiceTranscationRepository = new InvoiceTranscationRepository(ClientsBean.getDynamoDbClient(), System.getenv(Constants.INVOICE_DDB));
		
		final InvoiceTransactionEntity i = new InvoiceTransactionEntity();
		i.setTimestamp(Instant.now().toEpochMilli());
		i.setTtl(Instant.now().plus(Duration.ofMinutes(2)).getEpochSecond());
		i.setPk(InvoiceTranscationRepository.PK_TRANSACTION);
		i.setSk(fileName);
		i.setRequestId(awsRequestId);
		i.setInvoiceTranscationStatus(InvoiceTranscationStatus.GENERATED);
		i.setConnectionId(connectionId);
		
		invoiceTranscationRepository.save(i);
		
		return i;
		
	}

	public String generatePresignedUrl(String fileName, HttpMethod mehtod) {

		Instant instant = Instant.now().plus(Duration.ofMinutes(5));

		final String s3BucketName = System.getenv(S3_BUCKET_KEY);

	 
		final GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(s3BucketName,
				fileName).withMethod(mehtod).withExpiration(Date.from(instant));

		return ClientsBean.getS3Client().generatePresignedUrl(generatePresignedUrlRequest).toString();

	}

	
	

}
