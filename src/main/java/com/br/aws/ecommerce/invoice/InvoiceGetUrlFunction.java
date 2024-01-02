package com.br.aws.ecommerce.invoice;

import java.net.URL;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApi;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApiClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceGetUrlFunction
		implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

	private Logger logger = Logger.getLogger(InvoiceGetUrlFunction.class.getName());

	private static final String S3_BUCKET_KEY = "S3_BUCKET_KEY";

	private static final String WEB_SOCKET_API_GATEWAY_KEY = "WEB_SOCKET_API_GATEWAY_KEY";

	@Metrics
	@Logging
	@Tracing
	@Override
	public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent input, Context context) {
		final APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();

		final String connectionId = input.getRequestContext().getConnectionId();

		this.logger.log(Level.INFO, "InvoiceGetUrlFunction start with connectionId: {0}", connectionId);

		final AmazonApiGatewayManagementApi apiGatewayClient = this.getApiGatewayClient();

		final String urlPresigned = this.generatePresignedUrl(UUID.randomUUID().toString(), HttpMethod.POST);

		return response;
	}

	public String generatePresignedUrl(String fileName, HttpMethod mehtod) {

		final String s3BucketName = System.getenv(S3_BUCKET_KEY);

		// Set the pre-signed URL to expire after 10 mins.
		java.util.Date expiration = new java.util.Date();
		long expTimeMillis = expiration.getTime();
		expTimeMillis += 1000 * 60 * 10;
		expiration.setTime(expTimeMillis);

		// Generate the pre-signed URL
		final GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(s3BucketName,
				fileName).withMethod(mehtod).withExpiration(expiration);
		final URL url = this.getS3Client().generatePresignedUrl(generatePresignedUrlRequest);

		return url.toString();

	}

	private AmazonS3 getS3Client() {

		return AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain())
				.withRegion(Regions.US_EAST_1.getName()).build();

	}

	private AmazonApiGatewayManagementApi getApiGatewayClient() {

		final String webSocketApiGatewayUrl = System.getenv(WEB_SOCKET_API_GATEWAY_KEY);

		return AmazonApiGatewayManagementApiClientBuilder.standard()
				.withCredentials(new DefaultAWSCredentialsProviderChain())
				.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).withEndpointConfiguration(
						new EndpointConfiguration(webSocketApiGatewayUrl, Regions.US_EAST_1.getName()))
				.build();
	}

}
