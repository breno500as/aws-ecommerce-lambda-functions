package com.br.aws.ecommerce.event;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.model.ProductEventDTO;
import com.br.aws.ecommerce.layers.repository.EventRepository;
import com.br.aws.ecommerce.product.ProductsAdminFunction;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductEventFunction extends BaseLambdaFunction implements RequestHandler<ProductEventDTO, String> {

	private Logger logger = Logger.getLogger(ProductsAdminFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(ProductEventDTO productEvent, Context context) {

		try {

			this.logger.log(Level.INFO, "Get Input: {0}", productEvent);
			this.logger.log(Level.INFO, "Lambda requestId: {0}", context.getAwsRequestId());

			productEvent.setRequestId(context.getAwsRequestId());

			final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1.getName())
					.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())).build();

			final EventRepository productEventRepository = new EventRepository(client);

			productEventRepository.saveProductEvent(productEvent);

			return "OK " + productEvent.getProductEventType().getValue();

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error %s: ", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

}
