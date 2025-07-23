package io.camunda.zeebebenchmark.starter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.CreateProcessInstanceCommandStep1;
import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.zeebebenchmark.AbstractBenchmarkingRole;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@Profile("starter")
class InstanceStarter extends AbstractBenchmarkingRole<StarterProperties> {
	
	private final AtomicLong businessKeyCounter = new AtomicLong();
	private final AtomicLong successInstances = new AtomicLong();
	private final AtomicLong failedInstances = new AtomicLong();
	private final AtomicLong inFlightInstances = new AtomicLong();
	
	private record Stats(long success, long failed, long inFlight) {}

	public InstanceStarter(
			CamundaClient camundaClient,
			StarterProperties starterProperties,
			ObjectMapper objectMapper) {
		super(camundaClient, starterProperties, objectMapper);
	}

	@Override
	protected void doInit() {
		deployProcess();
		startInstances();
	}

	private void deployProcess() {

		DeployResourceCommandStep1.DeployResourceCommandStep2 deployCommand = camundaClient
				.newDeployResourceCommand()
				.addResourceFromClasspath(properties.bpmnXmlPath());

		properties.extraBpmnModels().forEach(deployCommand::addResourceFromClasspath);

		Mono.fromCompletionStage(deployCommand.send())
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
						.doBeforeRetry(_ -> log.atError().log("Failed to deploy process, will retry")))
				.doOnError(thrown -> log.atError().setCause(thrown).log("Failed to deploy process"))
				.doOnSuccess(d -> log.atInfo().arg(d.getProcesses().size()).log("Deployed {} resources"))
				.block();
	}

	private void startInstances() {
		Duration interval = Duration.ofSeconds(1).dividedBy(properties.rate());
		log.atInfo().arg(interval.toNanos()).log("Creating an instance every {}ns");

		Disposable startProcessInstancesSubscription = Flux.interval(interval)
				.onBackpressureDrop()
				.doOnNext(_ -> pushInFlight(startSingleInstance(getVariables())))
				.subscribe();

		if (properties.durationLimit() != null)
			Mono.delay(properties.durationLimit())
					.doOnNext(_ -> log.atInfo().log("Duration limit reached, stopping"))
					.doOnNext(_ -> startProcessInstancesSubscription.dispose())
					.subscribe();

		startStats(startProcessInstancesSubscription);
	}
	
	private Mono<?> startSingleInstance(Map<String, Object> variables) {
		return doStartSingleInstance(variables)
				.doOnSubscribe(_ -> inFlightInstances.incrementAndGet())
				
				.doOnError(_ -> failedInstances.incrementAndGet())
				
				.onErrorComplete(thrown -> thrown instanceof StatusRuntimeException srex
						&& srex.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED)

				.doOnNext(_ -> successInstances.incrementAndGet())
				
				.transform(m -> ! properties.failFast() 
						? m.onErrorComplete() 
						: m.onErrorResume(thrown -> Mono.error(new RuntimeException("Failed to create process instance", thrown))))
				
				.doFinally(_ -> inFlightInstances.decrementAndGet());
	}

	private Mono<?> doStartSingleInstance(Map<String, Object> variables) {
		
		Map<String, Object> instanceVariables = new HashMap<>(variables);
		variables.put(properties.businessKey(), businessKeyCounter.incrementAndGet());
		
		if(properties.startViaMessage())
			return Mono.fromCompletionStage(camundaClient
					.newPublishMessageCommand()
					.messageName(properties.msgName())
					.correlationKey(UUID.randomUUID().toString())
					.variables(instanceVariables)
					.timeToLive(Duration.ZERO)
					.send());
		
		CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 commandBuilder = camundaClient
				.newCreateInstanceCommand()
				.bpmnProcessId(properties.processId())
				.latestVersion()
				.variables(instanceVariables);
		
		
		if(properties.withResults())
			return Mono.fromCompletionStage(commandBuilder
							.withResult()
							.requestTimeout(properties.withResultsTimeout())
							.send());
		
		return Mono.fromCompletionStage(commandBuilder.send());
	}

	private void detectStall(List<Stats> statsList, SynchronousSink<Object> sink, Disposable startProcessInstancesSubscription) {
		if ( ! properties.stallDetection())
			return;
		
		if (startProcessInstancesSubscription.isDisposed() && statsList.getLast().inFlight() != 0)
			return;
		
		if (startProcessInstancesSubscription.isDisposed() && statsList.getLast().inFlight() == 0)
			sink.complete();
		else if (statsList.size() == 3 && Set.copyOf(statsList).size() == 1)
			sink.error(new IllegalStateException("Zeebe has stalled"));
	}
	
	private void startStats(Disposable startProcessInstancesSubscription) {
		Flux.interval(Duration.ofSeconds(5))
				.map(_ -> new Stats(successInstances.get(),
						failedInstances.get(),
						inFlightInstances.get()))
				.doOnNext(stats -> log.atInfo().arg(stats).log("{}"))
				.buffer(3)
				.handle((statsList, sink) ->
						detectStall(statsList, sink, startProcessInstancesSubscription))
				.doOnError(_ -> startProcessInstancesSubscription.dispose())
				.subscribe();
	}
}
