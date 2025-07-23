package io.camunda.zeebebenchmark.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobHandler;
import io.camunda.client.api.worker.JobWorker;
import io.camunda.client.api.worker.JobWorkerMetrics;
import io.camunda.zeebebenchmark.AbstractBenchmarkingRole;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingClientInterceptor;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Timed;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
@Profile("worker")
class WorkerStarter extends AbstractBenchmarkingRole<WorkerProperties> {
	
	private final PrometheusMeterRegistry meterRegistry;

	private final AtomicLong jobsReceived = new AtomicLong();
	private final AtomicLong successfulCompletions = new AtomicLong();
	private final AtomicLong failedCompletions = new AtomicLong();

	private record Stats(long jobsReceived, long successfulCompletions, long failedCompletions) { }

	WorkerStarter(
			CamundaClient camundaClient,
			WorkerProperties workerProperties,
			ObjectMapper objectMapper, 
			PrometheusMeterRegistry meterRegistry) {
		super(camundaClient, workerProperties, objectMapper);
		this.meterRegistry = meterRegistry;
	}

	@Override
	protected void doInit() {
		camundaClient.getConfiguration().getInterceptors().add(new MetricCollectingClientInterceptor(meterRegistry));
		startWorkers();
		startStats();
	}

	private void startWorkers() {
		JobWorkerMetrics metrics = JobWorkerMetrics
				.micrometer()
				.withMeterRegistry(meterRegistry)
				.withTags(Tags.of(
						"workerName", camundaClient.getConfiguration().getDefaultJobWorkerName(),
						"jobType", properties.jobType()))
				.build();

		Map<String, Object> variables = getVariables();

		JobWorker worker = camundaClient
				.newWorker()
				.jobType(properties.jobType())
				.handler(handleJob(variables))
				.streamEnabled(properties.streamEnabled())
				.metrics(metrics)
				.open();
	}

	private JobHandler handleJob(Map<String, Object> variables) {
		return (client, job) -> shouldSendCompleteCommand(job)
				.doOnSubscribe(_ -> jobsReceived.incrementAndGet())
				.timed()
				.filter(Timed::get)
				.flatMap(timed -> Mono.delay(properties.completionDelay().minus(timed.elapsedSinceSubscription()))
						.then(Mono.fromSupplier(() -> client
										.newCompleteCommand(job)
										.variables(variables)
										.send())
								.doOnNext(completionStage -> pushInFlight(
										Mono.fromCompletionStage(completionStage)
												.doOnSuccess(_ -> 
														successfulCompletions.incrementAndGet())
												.doOnError(_ -> 
														failedCompletions.incrementAndGet())))))
				.subscribe();
	}

	private Mono<Boolean> shouldSendCompleteCommand(ActivatedJob job) {
		if ( ! properties.sendMessage()) 
			return Mono.just(true);
		
		String correlationKey = job.getVariable(properties.correlationKeyVariableName()).toString();
		return Mono.fromCompletionStage(camundaClient
						.newPublishMessageCommand()
						.messageName(properties.messageName())
						.correlationKey(correlationKey)
						.send())
				.timeout(Duration.ofSeconds(10))
				.thenReturn(true)
				.doOnError(thrown -> log.atError()
						.setCause(thrown)
						.arg(properties.messageName())
						.arg(correlationKey)
						.log("Exception on publishing a message with name {} and correlationKey {}"))
				.onErrorReturn(false);
	}

	private void startStats() {
		Flux.interval(Duration.ofSeconds(5))
				.map(_ -> new Stats(jobsReceived.get(),
						successfulCompletions.get(),
						failedCompletions.get()))
				.doOnNext(stats -> log.atInfo().arg(stats).log("{}"))
				.subscribe();
	}
}
