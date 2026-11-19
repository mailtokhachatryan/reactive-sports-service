package com.example.sports.ingestion;

import com.example.sports.SportsApplication;
import com.example.sports.ingestion.rakuten.RakutenStub;
import com.example.sports.sport.Sport;
import com.example.sports.sport.repository.SportRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class RakutenImportTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	@Container
	@ServiceConnection
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

	@Autowired
	private SportRepository repository;

	@Test
	void importsMultiplePagesAndCanRepeatTheImportWithoutDuplicates() throws Exception {
		try (RakutenStub stub = new RakutenStub(request -> {
			if (request.query().contains("genreId=208012")) {
				return items(2, 20, 208012);
			}
			if (request.query().contains("genreId=508949")) {
				return items(1, 5, 508949);
			}
			return new RakutenStub.Response(501, "{}");
		})) {
			runImport(stub);
			assertThat(repository.findAll().map(Sport::name).collectList().block(TIMEOUT))
					.containsExactlyInAnyOrder("Tennis", "Table Tennis");
			runImport(stub);
			assertThat(stub.requests()).hasSize(6);
			StepVerifier.create(repository.count()).expectNext(2L).expectComplete().verify(TIMEOUT);
		}
	}

	private static RakutenStub.Response items(int pages, int count, int genreId) {
		String item = "{\"genreId\":" + genreId + "}";
		return new RakutenStub.Response(200, "{\"pageCount\":" + pages + ",\"items\":["
				+ String.join(",", Collections.nCopies(count, item)) + "]}");
	}

	private static void runImport(RakutenStub stub) {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(SportsApplication.class)
				.profiles("import").run(
						"--DB_HOST=" + POSTGRES.getHost(),
						"--DB_PORT=" + POSTGRES.getMappedPort(5432),
						"--DB_NAME=" + POSTGRES.getDatabaseName(),
						"--DB_USERNAME=" + POSTGRES.getUsername(),
						"--DB_PASSWORD=" + POSTGRES.getPassword(),
						"--RAKUTEN_ENDPOINT=" + stub.endpoint(),
						"--RAKUTEN_APPLICATION_ID=stub-app", "--RAKUTEN_ACCESS_KEY=stub-key",
						"--logging.level.com.example.sports.ingestion.ItemBatches=DEBUG")) {
			assertThat(context.isActive()).isTrue();
		}
	}
}
