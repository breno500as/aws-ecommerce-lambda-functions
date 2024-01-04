package com.br.aws.ecommerce.product;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.br.aws.ecommerce.layers.base.BaseLambdaFunction;
import com.br.aws.ecommerce.layers.entity.ProductEntity;
import com.br.aws.ecommerce.layers.model.ErrorMessageDTO;
import com.br.aws.ecommerce.layers.model.ProductEventDTO;
import com.br.aws.ecommerce.layers.model.ProductEventTypeEnum;
import com.br.aws.ecommerce.layers.repository.ProductRepository;
import com.br.aws.ecommerce.util.ClientsBean;
import com.br.aws.ecommerce.util.Constants;
import com.fasterxml.jackson.core.JsonProcessingException;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class ProductAdminFunction extends BaseLambdaFunction<ProductEntity>
		implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	private Logger logger = Logger.getLogger(ProductAdminFunction.class.getName());

	@Tracing
	@Logging
	@Metrics
	@Override
	public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {

		final Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Custom-Header", "application/json");
		
		final APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);

		try {

			final ProductRepository productRepository = new ProductRepository(ClientsBean.getDynamoDbClient(), System.getenv(Constants.PRODUCTS_DDB));

			if ("/products".equals(input.getResource())) {

				final ProductEntity productBody = getMapper().readValue(input.getBody(), ProductEntity.class);

				final ProductEntity product = productRepository.save(productBody);

				this.createEvent(product, ProductEventTypeEnum.CREATED, Boolean.TRUE, "insert_user@gmail.com");

				return response.withStatusCode(201).withBody(getMapper().writeValueAsString(product));

			} else if ("/products/{id}".equals(input.getResource())) {

				final String id = input.getPathParameters() != null ? input.getPathParameters().get("id") : null;

				if ("PUT".equals(input.getHttpMethod())) {

					final ProductEntity productBody = getMapper().readValue(input.getBody(), ProductEntity.class);

					final ProductEntity product = productRepository.update(productBody, id);

					this.createEvent(product, ProductEventTypeEnum.UPDATED, Boolean.TRUE, "update_user@gmail.com");

					return response.withStatusCode(200).withBody(getMapper().writeValueAsString(product));

				} else if ("DELETE".equals(input.getHttpMethod())) {

					productRepository.delete(id);

					this.createEvent(new ProductEntity(id), ProductEventTypeEnum.DELETED, Boolean.TRUE, "delete_user@gmail.com");

					return response.withStatusCode(204);
				}
			}

			this.logger.log(Level.WARNING, "Method not allowed");
			return response.withBody(super.getMapper().writeValueAsString(new ErrorMessageDTO("Method not allowed")))
					.withStatusCode(405);

		} catch (Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error: %s ", e.getMessage()), e);
			return response.withBody(String.format("{ \"error\": \"%s\" }", e.getMessage())).withStatusCode(500);
		}

	}

	

	private void createEvent(ProductEntity product, ProductEventTypeEnum productEventType, boolean async, String email)
			throws JsonProcessingException {

		final ProductEventDTO productEvent = new ProductEventDTO();
		productEvent.setProductEventType(productEventType);
		productEvent.setEmail(email);
		productEvent.setProductCode(product.getCode());
		productEvent.setProductId(product.getId());
		productEvent.setProductPrice(product.getPrice());

		final String functionProductEvents = System.getenv(Constants.PRODUCT_EVENT_FUNCTION_KEY);

		InvokeResult invokeResult = null;

		final InvokeRequest invokeRequest = new InvokeRequest().withFunctionName(functionProductEvents)
				.withPayload(getMapper().writeValueAsString(productEvent));

		try {

			if (async) {

				this.logger.log(Level.INFO, String.format("Asynchronous: %s , Event:  %s", functionProductEvents,
						productEvent.getProductEventType().getValue()));

				AWSLambdaAsyncClientBuilder.defaultClient().invokeAsync(invokeRequest, new AsyncLambdaHandler());

			} else {

				this.logger.log(Level.INFO, String.format("Synchronous: %s , Event:  %s", functionProductEvents,
						productEvent.getProductEventType().getValue()));

				invokeResult = AWSLambdaClientBuilder.defaultClient().invoke(invokeRequest);

				String ans = new String(invokeResult.getPayload().array(), StandardCharsets.UTF_8);

				this.logger.log(Level.INFO, String.format("Return %s: %s ", functionProductEvents, ans));
				this.logger.log(Level.INFO, "Status code return: {0}", invokeResult.getStatusCode());

			}

		} catch (ServiceException e) {
			this.logger.log(Level.SEVERE, String.format("Error invoking %s: ", functionProductEvents, e.getMessage()), e);
		}

	}

	private class AsyncLambdaHandler implements AsyncHandler<InvokeRequest, InvokeResult> {

		private Logger logger = Logger.getLogger(AsyncLambdaHandler.class.getName());

		public void onSuccess(InvokeRequest req, InvokeResult res) {
			ByteBuffer responsePayload = res.getPayload();
			this.logger.log(Level.INFO, "Return Async: {0} ", new String(responsePayload.array()));
		}

		public void onError(Exception e) {
			this.logger.log(Level.SEVERE, String.format("Error Async: %s ", e.getMessage()), e);
		}
	}

}
