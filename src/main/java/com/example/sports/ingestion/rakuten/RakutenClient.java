package com.example.sports.ingestion.rakuten;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeoutException;

@Profile("import")
@Component
public class RakutenClient {

	private static final int PAGE_SIZE = 20;
	private static final long RETRIES = 2;
	private final WebClient client;
	private final RakutenProperties properties;

	public RakutenClient(WebClient.Builder builder, RakutenProperties properties) {
		this.client = builder.build();
		this.properties = properties;
	}

	public Flux<RakutenItem> items() {
		return Flux.fromIterable(properties.genreNames().keySet().stream().sorted().toList())
				.concatMap(genreId -> itemsOnPage(genreId, 1), 0);
	}

	private Flux<RakutenItem> itemsOnPage(long genreId, int number) {
		return fetchPage(genreId, number).flatMapMany(page -> {
			Flux<RakutenItem> items = Flux.fromIterable(page.items());
			int lastPage = Math.min(page.pageCount(), properties.maxPagesPerGenre());
			return number < lastPage ? items.concatWith(Flux.defer(() -> itemsOnPage(genreId, number + 1))) : items;
		});
	}

	private Mono<RakutenPage> fetchPage(long genreId, int number) {
		return Mono.defer(() -> client.get()
				.uri(UriComponentsBuilder.fromUri(properties.endpoint())
						.queryParam("applicationId", "{applicationId}")
						.queryParam("genreId", genreId)
						.queryParam("format", "json")
						.queryParam("formatVersion", 2)
						.queryParam("elements", "pageCount,items,genreId")
						.queryParam("hits", PAGE_SIZE)
						.queryParam("page", number)
						.encode().buildAndExpand(properties.applicationId()).toUri())
				.header("accessKey", properties.accessKey())
				.accept(MediaType.APPLICATION_JSON)
				.exchangeToMono(RakutenClient::decode))
				.timeout(properties.requestTimeout())
				.onErrorMap(WebClientRequestException.class, error -> retryable("Rakuten connection failed."))
				.onErrorMap(TimeoutException.class, error -> retryable("Rakuten request timed out."))
				.retryWhen(Retry.from(signals -> signals.concatMap(this::retryDelay)));
	}

	private static Mono<RakutenPage> decode(ClientResponse response) {
		if (!response.statusCode().is2xxSuccessful()) {
			int status = response.statusCode().value();
			boolean retryable = status == 429 || response.statusCode().is5xxServerError();
			Duration retryAfter = parseRetryAfter(response.headers().asHttpHeaders().getFirst(HttpHeaders.RETRY_AFTER));
			return response.releaseBody()
					.then(Mono.error(new RakutenRequestException("Rakuten returned HTTP " + status + ".", retryable, retryAfter)));
		}
		return response.bodyToMono(RakutenPage.class)
				.onErrorMap(error -> error instanceof IOException
						? retryable("Rakuten connection closed before the response completed.") : invalidResponse())
				.filter(page -> page.pageCount() != null && page.pageCount() >= 0 && page.items() != null
						&& page.items().size() <= PAGE_SIZE && (page.pageCount() > 0 || page.items().isEmpty())
						&& page.items().stream().allMatch(item -> item != null && item.genreId() != null && item.genreId() > 0))
				.switchIfEmpty(Mono.error(RakutenClient::invalidResponse));
	}

	private Mono<Long> retryDelay(Retry.RetrySignal signal) {
		if (!(signal.failure() instanceof RakutenRequestException error) || !error.retryable()
				|| signal.totalRetries() >= RETRIES) {
			return Mono.error(signal.failure());
		}
		Duration backoff = Duration.ofMillis(250L << signal.totalRetries());
		Duration delay = error.retryAfter().compareTo(backoff) > 0 ? error.retryAfter() : backoff;
		return delay.compareTo(properties.importTimeout()) >= 0 ? Mono.error(error) : Mono.delay(delay);
	}

	private static Duration parseRetryAfter(String value) {
		if (value == null || value.isBlank()) {
			return Duration.ZERO;
		}
		try {
			return Duration.ofSeconds(Math.max(0, Long.parseLong(value)));
		} catch (NumberFormatException invalidSeconds) {
			try {
				Duration delay = Duration.between(Instant.now(), ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME));
				return delay.isNegative() ? Duration.ZERO : delay;
			} catch (DateTimeParseException invalidDate) {
				return Duration.ZERO;
			}
		}
	}

	private static RakutenRequestException invalidResponse() {
		return new RakutenRequestException("Rakuten returned an invalid item page.", false, Duration.ZERO);
	}

	private static RakutenRequestException retryable(String message) {
		return new RakutenRequestException(message, true, Duration.ZERO);
	}
}
