package io.camunda.zeebebenchmark.starter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.CreateProcessInstanceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.response.BrokerInfo;
import io.camunda.client.api.response.Topology;
import io.camunda.zeebebenchmark.ZeebeBenchmarkUtilApplication;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.vavr.control.Try;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
class InstanceStarter {
	
	private final StarterProperties starterProperties;
	private final CamundaClient camundaClient;
	private final ResourceLoader resourceLoader;
	private final ObjectMapper objectMapper;
	
	private final AtomicLong businessKeyCounter = new AtomicLong();
	private final AtomicLong successInstances = new AtomicLong();
	private final AtomicLong failedInstances = new AtomicLong();
	private final AtomicLong inFlightInstances = new AtomicLong();
	
	private record Stats(long success, long failed, long inFlight) {}

	@PostConstruct
	public void init() {
		printTopology();
		deployProcess();
		startInstances();
	}

	private void deployProcess() {

		DeployResourceCommandStep1.DeployResourceCommandStep2 deployCommand = camundaClient
				.newDeployResourceCommand()
				.addResourceFromClasspath(starterProperties.bpmnXmlPath());

		starterProperties.extraBpmnModels().forEach(deployCommand::addResourceFromClasspath);

		Mono.fromCompletionStage(deployCommand.send())
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
						.doBeforeRetry(_ -> log.atError().log("Failed to deploy process, will retry")))
				.doOnError(thrown -> log.atError().setCause(thrown).log("Failed to deploy process"))
				.doOnSuccess(d -> log.atInfo().arg(d.getProcesses().size()).log("Deployed {} resources"))
				.block();
	}

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

	private void startInstances() {
		Duration interval = Duration.ofSeconds(1).dividedBy(starterProperties.rate());
		log.atInfo().arg(interval.toNanos()).log("Creating an instance every {}ns");
		Map<String, Object> variables = Try.withResources(() -> resourceLoader.getResource(starterProperties.payloadPath()).getInputStream())
				.of(in -> objectMapper.readValue(in, new TypeReference<Map<String, Object>>() {}))
				.get();

		Sinks.Many<Mono<?>> inFlightSink = Sinks.many().unicast().onBackpressureBuffer();

		Disposable startProcessInstancesSubscription = Flux.interval(interval)
				.onBackpressureDrop()
				.doOnNext(_ -> inFlightSink.tryEmitNext(startSingleInstance(variables)))
				.subscribe();

		inFlightSink.asFlux()
				.flatMap(it -> it)
				.subscribe();

		Flux.interval(Duration.ofSeconds(5))
				.map(_ -> new Stats(successInstances.get(),
						failedInstances.get(),
						inFlightInstances.get()))
				.doOnNext(stats -> log.atInfo().arg(stats).log("{}"))
				.buffer(3)
				.handle((statsList, sink) -> {
					if (startProcessInstancesSubscription.isDisposed() && statsList.getLast().inFlight() != 0)
						return;
					if (startProcessInstancesSubscription.isDisposed() && statsList.getLast().inFlight() == 0)
						sink.complete();
					else if (statsList.size() == 3 && Set.copyOf(statsList).size() == 1)
						sink.error(new IllegalStateException("Zeebe has stalled"));
				})
				.doOnError(_ -> startProcessInstancesSubscription.dispose())
				.doFinally(_ -> ZeebeBenchmarkUtilApplication.allowShutdown())
				.subscribe();
		
		if (starterProperties.durationLimit() != null)
			Mono.delay(starterProperties.durationLimit())
					.doOnNext(_ -> log.atInfo().log("Duration limit reached, stopping"))
					.doOnNext(_ -> startProcessInstancesSubscription.dispose())
					.subscribe();
	}

	private Mono<?> startSingleInstance(Map<String, Object> variables) {
		return doStartSingleInstance(variables)
				.doOnSubscribe(_ -> inFlightInstances.incrementAndGet())
				
				.doOnError(_ -> failedInstances.incrementAndGet())
				
				.onErrorComplete(thrown -> thrown instanceof StatusRuntimeException srex
						&& srex.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED)

				.doOnNext(_ -> successInstances.incrementAndGet())
				.onErrorComplete()
				.doFinally(_ -> inFlightInstances.decrementAndGet());
	}

	private Mono<?> doStartSingleInstance(Map<String, Object> variables) {
		
		Map<String, Object> instanceVariables = new HashMap<>(variables);
		variables.put(starterProperties.businessKey(), businessKeyCounter.incrementAndGet());
		
		if(starterProperties.startViaMessage())
			return Mono.fromCompletionStage(camundaClient
					.newPublishMessageCommand()
					.messageName(starterProperties.msgName())
					.correlationKey(UUID.randomUUID().toString())
					.variables(instanceVariables)
					.timeToLive(Duration.ZERO)
					.send());
		
		CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 commandBuilder = camundaClient
				.newCreateInstanceCommand()
				.bpmnProcessId(starterProperties.processId())
				.latestVersion()
				.variables(instanceVariables);
		
		
		if(starterProperties.withResults())
			return Mono.fromCompletionStage(commandBuilder
							.withResult()
							.requestTimeout(starterProperties.withResultsTimeout())
							.send());
		
		return Mono.fromCompletionStage(commandBuilder.send());
	}
}
