package io.camunda.zeebebenchmark.worker;


import io.camunda.zeebebenchmark.GeneralProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("benchmark.worker")
record WorkerProperties(
		String jobType,
		Duration completionDelay,
		String payloadPath,
		boolean streamEnabled,
		boolean sendMessage,
		String messageName,
		String correlationKeyVariableName) implements GeneralProperties { }
