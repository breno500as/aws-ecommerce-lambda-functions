package com.br.aws.ecommerce.audit.errors;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoicesErrorsFunction implements RequestHandler<ScheduledEvent, Boolean> {

	private Logger logger = Logger.getLogger(InvoicesErrorsFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public Boolean handleRequest(ScheduledEvent input, Context context) {

		this.logger.log(Level.INFO, "InvoicesErrorsFunction start: {0}", input);

		return true;
	}
}
