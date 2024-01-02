package com.br.aws.ecommerce.invoice;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.tracing.Tracing;

public class InvoiceImportFunction implements RequestHandler<S3Event, String> {

	private Logger logger = Logger.getLogger(InvoiceImportFunction.class.getName());

	@Metrics
	@Logging
	@Tracing
	@Override
	public String handleRequest(S3Event input, Context context) {

		this.logger.log(Level.INFO, "InvoiceImportFunction start");

		return "OK";
	}

}