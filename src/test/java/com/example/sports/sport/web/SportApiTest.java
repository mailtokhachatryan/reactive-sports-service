package com.example.sports.sport.web;

import com.example.sports.sport.repository.SportRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SportApiTest {

	private static final String SPORT_PATH = "/api/v1/sport";
	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	@Container
	@ServiceConnection
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

	@Autowired
	private SportRepository repository;

	@LocalServerPort
	private int port;

	private WebTestClient client;

	@BeforeEach
	void resetSports() {
		repository.deleteAll().block(TIMEOUT);
		client = WebTestClient.bindToServer().baseUrl("http://127.0.0.1:" + port).responseTimeout(TIMEOUT).build();
	}

	@Test
	void createsSportAndSearchesByLiteralSubstring() {
		client.post().uri(SPORT_PATH + "/{name}", "  100%_Ball  ").exchange()
				.expectStatus().isCreated()
				.expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
				.expectBody().jsonPath("$.id").isNumber().jsonPath("$.name").isEqualTo("100%_Ball");
		createSport("100XXBall");
		createSport("A Ball");

		client.get().uri(builder -> builder.path(SPORT_PATH).queryParam("q", "{q}").build("%_bAl"))
				.exchange().expectStatus().isOk().expectBody()
				.jsonPath("$.length()").isEqualTo(1)
				.jsonPath("$[0].name").isEqualTo("100%_Ball");
		client.get().uri(SPORT_PATH + "?q=BALL").exchange().expectStatus().isOk().expectBody()
				.jsonPath("$[*].name").isEqualTo(List.of("100%_Ball", "100XXBall", "A Ball"));
		client.get().uri(SPORT_PATH + "?q=rowing").exchange().expectStatus().isOk().expectBody().json("[]");
	}

	@Test
	void concurrentCreatesReturnOneCreatedAndOneNameConflict() throws Exception {
		CompletableFuture<Integer> first = CompletableFuture.supplyAsync(() -> createSport("Tennis"));
		CompletableFuture<Integer> second = CompletableFuture.supplyAsync(() -> createSport("  tEnNiS  "));
		assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
				.containsExactlyInAnyOrder(201, 409);

		client.post().uri(SPORT_PATH + "/{name}", "TENNIS").exchange()
				.expectStatus().isEqualTo(409)
				.expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
				.expectBody().jsonPath("$.code").isEqualTo("SPORT_NAME_EXISTS");
		StepVerifier.create(repository.count()).expectNext(1L).expectComplete().verify(TIMEOUT);
	}

	@Test
	void rejectsInvalidInputs() {
		client.post().uri(SPORT_PATH + "/{name}", "X".repeat(101)).exchange()
				.expectStatus().isBadRequest().expectBody().jsonPath("$.code").isEqualTo("INVALID_SPORT_INPUT");
		client.get().uri(SPORT_PATH).exchange().expectStatus().isBadRequest()
				.expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
				.expectBody().jsonPath("$.code").isEqualTo("INVALID_SPORT_INPUT");
		client.get().uri(builder -> builder.path(SPORT_PATH).queryParam("q", "{q}").build("   "))
				.exchange().expectStatus().isBadRequest();
	}

	private int createSport(String name) {
		return client.post().uri(SPORT_PATH + "/{name}", name)
				.exchange().expectBody().returnResult().getStatus().value();
	}
}
