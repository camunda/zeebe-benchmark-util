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
@Profile("increasingstarter")
class RateIncreasingInstanceStarter extends AbstractInstanceStarter {
	
	public RateIncreasingInstanceStarter(
			CamundaClient camundaClient,
			StarterProperties starterProperties,
			ObjectMapper objectMapper, 
			PrometheusMeterRegistry meterRegistry) {
		super(camundaClient, starterProperties, objectMapper, meterRegistry);
	}

	@Override
	protected Disposable doStartInstances() {
		return Flux.<Long, Long>generate(() -> 0L, (o, sink) -> {
					sink.next(o++);
					return o;
				})
				.concatMap(multiplier -> {
					long rate = properties.rate() + (properties.rateIncrease() * multiplier);
					Duration interval = Duration.ofSeconds(1).dividedBy(rate);
					log.atInfo().arg(interval.toNanos()).arg(rate).log("Creating an instance every {}ns for rate {}/sec");
					currentRate.set(rate);

					return Flux.interval(interval)
							.take(properties.rateTime())
							.onBackpressureDrop()
							.doOnNext(_ -> pushInFlight(startSingleInstance(getVariables())));
				}, 1)
				.subscribe();
	}
}
