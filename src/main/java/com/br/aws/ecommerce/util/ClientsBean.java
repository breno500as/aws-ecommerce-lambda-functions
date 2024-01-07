package com.br.aws.ecommerce.util;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApi;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApiClientBuilder;
import com.amazonaws.services.cognitoidentity.AmazonCognitoIdentity;
import com.amazonaws.services.cognitoidentity.AmazonCognitoIdentityClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.AmazonEventBridgeClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;


public class ClientsBean {
	

	public static final String WEB_SOCKET_API_GATEWAY_KEY = "WEB_SOCKET_API_GATEWAY_KEY";
	

	public static AmazonApiGatewayManagementApi getApiGatewayClient() {

		String webSocketApiGatewayUrl = System.getenv(WEB_SOCKET_API_GATEWAY_KEY);
		
		if (webSocketApiGatewayUrl == null) {
			webSocketApiGatewayUrl = "wss://09meb2nkod.execute-api.us-east-1.amazonaws.com/prod";
		}
		

		return AmazonApiGatewayManagementApiClientBuilder.standard()
				.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
				.withEndpointConfiguration(new EndpointConfiguration(webSocketApiGatewayUrl.replaceAll("wss://", ""), Regions.US_EAST_1.getName()))
				.build();
	}

	public static AmazonS3 getS3Client() {
		return AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain())
				.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
				.withRegion(Regions.US_EAST_1.getName()).build();

	}

	public static AmazonDynamoDB getDynamoDbClient() {
		return AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.getName())
				.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).build();
	}
	
	public static AmazonSimpleEmailService getSimpleEmailClient() {
		return  AmazonSimpleEmailServiceClientBuilder.standard()
		            .withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
		            .withRegion(Regions.US_EAST_1.getName()).build();
	}
	
	
	public static AmazonEventBridge getEventBridgeClient() {
		return AmazonEventBridgeClient.builder().withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).build();
	}
	
	public static AmazonCognitoIdentity getCognitoClient() {
		return AmazonCognitoIdentityClientBuilder
	                .standard()
	                .withRegion(Regions.US_EAST_1.getName())
	                .withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
	                .build();
	}
	
 
	
 

}
