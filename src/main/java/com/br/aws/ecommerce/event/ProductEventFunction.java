package com.br.aws.ecommerce.event;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.ProductEntity;
import com.br.aws.ecommerce.layers.model.ProductEventDTO;
import com.br.aws.ecommerce.layers.repository.EventRepository;
import com.br.aws.ecommerce.product.ProductAdminFunction;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductEventFunction extends BaseLambdaFunction<ProductEntity> implements RequestHandler<ProductEventDTO, String> {

	private Logger logger = Logger.getLogger(ProductAdminFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public String handleRequest(ProductEventDTO productEvent, Context context) {

		try {

			this.logger.log(Level.INFO, "Get Input: {0}", productEvent);
			this.logger.log(Level.INFO, "Lambda requestId: {0}", context.getAwsRequestId());

			productEvent.setRequestId(context.getAwsRequestId());

			final EventRepository eventRepository = new EventRepository(ClientsBean.getDynamoDbClient(), System.getenv(Constants.EVENTS_DDB));

			eventRepository.saveProductEvent(productEvent);

			return "OK " + productEvent.getProductEventType().getValue();

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error %s: ", e.getMessage()), e);
			throw new ServiceException(e.getMessage());
		}
	}

}
