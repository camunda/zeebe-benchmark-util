package io.camunda.zeebebenchmark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.BrokerInfo;
import io.camunda.client.api.response.Topology;
import io.micrometer.core.instrument.MeterRegistry;
import io.vavr.control.Try;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractBenchmarkingRole<T extends GeneralProperties> implements ApplicationListener<ApplicationReadyEvent> {

	protected static final RetryBackoffSpec RETRY_FOREVER = RetryBackoffSpec
			.backoff(Long.MAX_VALUE, Duration.ofMillis(300))
			.maxBackoff(Duration.ofSeconds(5));

	protected final CamundaClient camundaClient;
	protected final T properties;
	protected final ResourceLoader resourceLoader = new DefaultResourceLoader();
	protected final ObjectMapper objectMapper;
	protected final MeterRegistry meterRegistry;

	private final Sinks.Many<Mono<?>> inFlightSink = Sinks.many().unicast().onBackpressureBuffer();

	@Override
	public void onApplicationEvent(ApplicationReadyEvent ignored) {
		printTopology();
		startCheckingResponses();
		doInit();
	}

	protected abstract void doInit();

	private void printTopology() {
		Mono.defer(() -> Mono.fromCompletionStage(camundaClient.newTopologyRequest().send())
				.flatMapIterable(Topology::getBrokers)
				.doOnNext(broker -> log.atInfo()
						.arg(broker.getNodeId())
						.arg(broker.getAddress())
						.log("Broker {} - {}"))
				.flatMapIterable(BrokerInfo::getPartitions)
				.doOnNext(p -> log.atInfo().arg(p.getPartitionId()).arg(p.getRole()).log("{} - {}"))
				.then()
				.doOnError(thrown -> log.atError()
						.addArgument(thrown.getMessage())
						.log("Topology request failed, will retry ({})")))
				.retryWhen(RETRY_FOREVER)
				.block();
	}

	protected Map<String, Object> getVariables() {
		return Try.withResources(() -> resourceLoader.getResource(properties.payloadPath()).getInputStream())
				.of(in -> objectMapper.readValue(in, new TypeReference<Map<String, Object>>() {}))
				.get();
	}

	protected void pushInFlight(Mono<?> mono) {
		inFlightSink.tryEmitNext(mono);
	}

	protected void pushInFlight(CompletionStage<?> completionStage) {
		inFlightSink.tryEmitNext(Mono.fromCompletionStage(completionStage));
	}

	private void startCheckingResponses() {
		inFlightSink.asFlux()
				.flatMap(it -> it, 8192)
				.onErrorContinue((_, _) -> {})
				.subscribe();
	}

}
