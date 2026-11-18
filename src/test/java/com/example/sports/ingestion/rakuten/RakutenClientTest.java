package com.example.sports.ingestion.rakuten;

import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RakutenClientTest {
	private static final Duration TIMEOUT = Duration.ofSeconds(5);
	private static final String EMPTY = """
			{"pageCount": 0, "items": []}
			""";

	@Test
	void sendsVersionedSearchAndStopsAtTheConfiguredPageLimit() throws Exception {
		try (RakutenStub stub = new RakutenStub(request -> new RakutenStub.Response(200, """
				{"pageCount": 12, "items": [{"genreId": 101, "itemName": "racket"}]}
				"""))) {
			StepVerifier.create(client(stub, 2).items())
					.expectNext(new RakutenItem(101L), new RakutenItem(101L))
					.expectComplete().verify(TIMEOUT);
			assertThat(stub.requests()).hasSize(2).allSatisfy(request -> {
				assertThat(request.method()).isEqualTo("GET");
				assertThat(request.accessKey()).isEqualTo("stub-key");
				assertThat(request.query()).contains("applicationId=stub%2Bapp%26x", "genreId=101", "formatVersion=2", "hits=20");
			});
			assertThat(stub.requests().get(0).query()).endsWith("page=1");
			assertThat(stub.requests().get(1).query()).endsWith("page=2");
		}
	}

	@Test
	void retriesRateLimitsAndServerErrorsBeforeCompletingAnEmptyPage() throws Exception {
		AtomicInteger attempts = new AtomicInteger();
		try (RakutenStub stub = new RakutenStub(request -> switch (attempts.incrementAndGet()) {
			case 1 -> new RakutenStub.Response(429, "{}", "0");
			case 2 -> new RakutenStub.Response(503, "{}");
			default -> new RakutenStub.Response(200, EMPTY);
		})) {
			StepVerifier.create(client(stub, 3).items()).expectComplete().verify(TIMEOUT);
			assertThat(stub.requests()).hasSize(3);
		}
	}

	@Test
	void failsWithoutRetryWhenCredentialsAreRejectedOrTheBodyIsMalformed() throws Exception {
		try (RakutenStub stub = new RakutenStub(request -> new RakutenStub.Response(401, "stub-key"))) {
			StepVerifier.create(client(stub, 3).items()).expectErrorMessage("Rakuten returned HTTP 401.").verify(TIMEOUT);
			assertThat(stub.requests()).hasSize(1);
		}
		try (RakutenStub stub = new RakutenStub(request -> new RakutenStub.Response(200, "{invalid"))) {
			StepVerifier.create(client(stub, 3).items()).expectErrorMessage("Rakuten returned an invalid item page.").verify(TIMEOUT);
			assertThat(stub.requests()).hasSize(1);
		}
	}

	private static RakutenClient client(RakutenStub stub, int maxPages) {
		RakutenProperties properties = new RakutenProperties(stub.endpoint(), "stub+app&x", "stub-key", maxPages,
				TIMEOUT, TIMEOUT, Map.of(101L, "Tennis"));
		return new RakutenClient(WebClient.builder(), properties);
	}
}
