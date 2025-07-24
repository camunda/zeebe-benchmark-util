package io.camunda.zeebebenchmark.starter;

import io.camunda.zeebebenchmark.GeneralProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties("benchmark.starter")
record StarterProperties(
		String processId,
		long rate,
		String bpmnXmlPath,
		List<String> extraBpmnModels, 
		String businessKey,
		String payloadPath, 
		boolean withResults, 
		Duration withResultsTimeout, 
		Duration durationLimit, 
		String msgName, 
		boolean startViaMessage, 
		boolean ignoreResourceExhausted, 
		boolean failFast, 
		boolean stallDetection,
		Duration rateTime, 
		int rateIncrease) implements GeneralProperties {
}
