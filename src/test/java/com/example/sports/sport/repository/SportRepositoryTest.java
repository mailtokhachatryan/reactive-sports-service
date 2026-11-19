package com.example.sports.sport.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class SportRepositoryTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	@Container
	@ServiceConnection
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

	@Autowired
	private SportRepository repository;

	@Test
	void concurrentImportsLeaveOneRow() {
		StepVerifier.create(Flux.merge(repository.insertIfMissing("Rowing"), repository.insertIfMissing("rowing"))
				.collectList())
				.assertNext(results -> assertThat(results).containsExactlyInAnyOrder(true, false))
				.expectComplete().verify(TIMEOUT);
		StepVerifier.create(repository.count()).expectNext(1L).expectComplete().verify(TIMEOUT);
	}
}
