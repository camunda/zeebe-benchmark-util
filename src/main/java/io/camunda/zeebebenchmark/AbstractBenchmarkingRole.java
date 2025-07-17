package io.camunda.zeebebenchmark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.BrokerInfo;
import io.camunda.client.api.response.Topology;
import io.vavr.control.Try;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ResourceLoader;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.CompletionStage;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractBenchmarkingRole<T extends GeneralProperties> {

	protected final CamundaClient camundaClient;
	protected final T properties;
	protected final ResourceLoader resourceLoader;
	protected final ObjectMapper objectMapper;

	private final Sinks.Many<Mono<?>> inFlightSink = Sinks.many().unicast().onBackpressureBuffer();

	@PostConstruct
	public final void init() {
		printTopology();
		startCheckingResponses();
		doInit();
	}
	
	protected abstract void doInit();

	private void printTopology() {
		Mono.fromCompletionStage(camundaClient.newTopologyRequest().send())
				.flatMapIterable(Topology::getBrokers)
				.doOnNext(broker -> log.atInfo()
						.arg(broker.getNodeId())
						.arg(broker.getAddress())
						.log("Broker {} - {}"))
				.flatMapIterable(BrokerInfo::getPartitions)
				.doOnNext(p -> log.atInfo().arg(p.getPartitionId()).arg(p.getRole()).log("{} - {}"))
				.then()
				.doOnError(thrown -> log.atError().setCause(thrown).log("Topology request failed"))
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
				.flatMap(it -> it)
				.subscribe();
	}
}
