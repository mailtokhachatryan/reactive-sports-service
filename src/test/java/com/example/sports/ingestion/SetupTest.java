package com.example.sports.ingestion;

import com.example.sports.ingestion.rakuten.RakutenItem;
import com.example.sports.ingestion.rakuten.RakutenProperties;
import com.example.sports.sport.service.SportService;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SetupTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(3);
	private static final RakutenItem KNOWN_ITEM = new RakutenItem(101L);
	private static final RakutenItem UNMAPPED_ITEM = new RakutenItem(999L);
	private final SportService sports = mock(SportService.class);
	private final Setup setup = new Setup(null, new RakutenProperties(
			URI.create("http://127.0.0.1/items"), "stub-app", "stub-key", 1, TIMEOUT, TIMEOUT, Map.of(101L, "Tennis")), sports);

	@Test
	void requestsTwentyAtATimeOnlyAfterCompletedWritesAndDrainsFinalBatch() {
		Sinks.One<Boolean> firstWrite = Sinks.one();
		when(sports.importSport("Tennis")).thenReturn(firstWrite.asMono());
		List<String> signals = new ArrayList<>();
		Flux<RakutenItem> source = Flux.range(1, 45)
				.doOnRequest(demand -> signals.add("request(" + demand + ")"))
				.doOnNext(item -> signals.add("onNext"))
				.map(index -> index <= 20 ? KNOWN_ITEM : UNMAPPED_ITEM);

		StepVerifier.create(setup.importItems(source))
				.then(() -> assertThat(signals).containsExactlyElementsOf(window(20)))
				.then(() -> assertThat(firstWrite.tryEmitValue(true)).isEqualTo(Sinks.EmitResult.OK))
				.expectNext(new ImportSummary(20, 0, 25))
				.expectComplete().verify(TIMEOUT);

		List<String> expected = new ArrayList<>(window(20));
		expected.addAll(window(20));
		expected.addAll(window(5));
		assertThat(signals).containsExactlyElementsOf(expected);
	}

	@Test
	void completesForEmptySourceAndForAnExactFinalBatch() {
		StepVerifier.create(setup.importItems(Flux.empty()))
				.expectNext(new ImportSummary(0, 0, 0)).expectComplete().verify(TIMEOUT);
		StepVerifier.create(setup.importItems(Flux.range(1, 20).map(i -> UNMAPPED_ITEM)))
				.expectNext(new ImportSummary(0, 0, 20)).expectComplete().verify(TIMEOUT);
	}

	@Test
	void cancellationStopsUpstreamAndInFlightWrite() {
		TestPublisher<RakutenItem> source = TestPublisher.create();
		TestPublisher<Boolean> write = TestPublisher.create();
		when(sports.importSport("Tennis")).thenReturn(write.mono());

		StepVerifier.create(setup.importItems(source.flux()))
				.then(() -> emitBatch(source))
				.then(write::assertSubscribers)
				.thenCancel().verify(TIMEOUT);

		source.assertCancelled();
		write.assertCancelled();
	}

	@Test
	void aWriteFailureCancelsUpstream() {
		TestPublisher<RakutenItem> source = TestPublisher.create();
		IllegalStateException failure = new IllegalStateException("write failed");
		when(sports.importSport("Tennis")).thenReturn(Mono.error(failure));

		StepVerifier.create(setup.importItems(source.flux()))
				.then(() -> emitBatch(source))
				.expectErrorMatches(error -> error == failure).verify(TIMEOUT);
		source.assertCancelled();
	}

	@Test
	void anUpstreamFailureCancelsTheInFlightWrite() {
		TestPublisher<RakutenItem> source = TestPublisher.create();
		TestPublisher<Boolean> write = TestPublisher.create();
		IllegalStateException failure = new IllegalStateException("provider failed");
		when(sports.importSport("Tennis")).thenReturn(write.mono());

		StepVerifier.create(setup.importItems(source.flux()))
				.then(() -> emitBatch(source))
				.then(() -> source.error(failure))
				.expectErrorMatches(error -> error == failure).verify(TIMEOUT);
		write.assertCancelled();
	}

	private static List<String> window(int items) {
		List<String> signals = new ArrayList<>();
		signals.add("request(20)");
		signals.addAll(Collections.nCopies(items, "onNext"));
		return signals;
	}

	private static void emitBatch(TestPublisher<RakutenItem> source) {
		source.next(KNOWN_ITEM, Collections.nCopies(19, UNMAPPED_ITEM).toArray(RakutenItem[]::new));
	}
}
