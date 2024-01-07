package com.br.aws.ecommerce.auth;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.CognitoUserPoolPreAuthenticationEvent;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class PreAuthenticationFunction implements RequestHandler<CognitoUserPoolPreAuthenticationEvent, CognitoUserPoolPreAuthenticationEvent> {

	private Logger logger = Logger.getLogger(PreAuthenticationFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public CognitoUserPoolPreAuthenticationEvent handleRequest(CognitoUserPoolPreAuthenticationEvent input, Context context) {

		this.logger.log(Level.INFO, "PreAuthenticationFunction start: {0}", input);
		
		final Map<String, String> userAttributes = input.getRequest().getUserAttributes();

		logger.log(Level.INFO, "User Attributes: {0} ", userAttributes.entrySet());
		logger.log(Level.INFO, "Client Meta Data: {0}" + input.getRequest().getUserAttributes().entrySet());
 
		
		return input;
	}
}
