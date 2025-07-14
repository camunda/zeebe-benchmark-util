package io.camunda.zeebebenchmark;

import io.vavr.control.Try;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ZeebeBenchmarkUtilApplication {

	private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

	public static void main(String[] args) {
		SpringApplication springApplication = new SpringApplication(ZeebeBenchmarkUtilApplication.class);
		springApplication.setWebApplicationType(WebApplicationType.NONE);
		springApplication.run(args);
		Try.run(shutdownLatch::await).get();
	}
	
	public static void allowShutdown() {
		shutdownLatch.countDown();
	}
}
