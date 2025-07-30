package io.camunda.zeebebenchmark.starter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Service
@Slf4j
@Profile("starter")
class FixedInstanceStarter extends AbstractInstanceStarter {
	
	public FixedInstanceStarter(
			CamundaClient camundaClient,
			StarterProperties starterProperties,
			ObjectMapper objectMapper, 
			PrometheusMeterRegistry meterRegistry) {
		super(camundaClient, starterProperties, objectMapper, meterRegistry);
	}

	@Override
	protected Disposable doStartInstances() {
		Duration interval = Duration.ofSeconds(1).dividedBy(properties.rate());
		log.atInfo().arg(interval.toNanos()).log("Creating an instance every {}ns");

		return Flux.interval(interval)
				.doOnNext(_ -> pushInFlight(startSingleInstance(getVariables())))
				.subscribe();
	}
}
