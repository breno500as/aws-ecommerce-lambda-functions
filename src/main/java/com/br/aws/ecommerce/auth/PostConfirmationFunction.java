package com.br.aws.ecommerce.auth;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.CognitoUserPoolPostConfirmationEvent;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class PostConfirmationFunction
		implements RequestHandler<CognitoUserPoolPostConfirmationEvent, CognitoUserPoolPostConfirmationEvent> {

	private Logger logger = Logger.getLogger(PostConfirmationFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public CognitoUserPoolPostConfirmationEvent handleRequest(CognitoUserPoolPostConfirmationEvent input,
			Context context) {

		this.logger.log(Level.INFO, "PostConfirmationFunction start: {0}", input);

		final Map<String, String> userAttributes = input.getRequest().getUserAttributes();

		logger.log(Level.INFO, "User Attributes: {0} ", userAttributes.entrySet());
	 

		return input;
	}
}
