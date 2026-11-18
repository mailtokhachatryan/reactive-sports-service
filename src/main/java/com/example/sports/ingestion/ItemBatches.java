package com.example.sports.ingestion;

import com.example.sports.ingestion.rakuten.RakutenItem;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.List;

final class ItemBatches extends BaseSubscriber<RakutenItem> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ItemBatches.class);
	private static final int BATCH_SIZE = 20;
	private final FluxSink<List<RakutenItem>> batches;
	private final List<RakutenItem> pending = new ArrayList<>(BATCH_SIZE);
	private long itemCount;

	private ItemBatches(FluxSink<List<RakutenItem>> batches) {
		this.batches = batches;
	}

	static Flux<List<RakutenItem>> from(Flux<RakutenItem> items) {
		return Flux.create(batches -> items.subscribe(new ItemBatches(batches)), FluxSink.OverflowStrategy.ERROR);
	}

	@Override
	public Context currentContext() {
		return Context.of(batches.contextView());
	}

	@Override
	protected void hookOnSubscribe(Subscription subscription) {
		batches.onCancel(this::cancel);
		batches.onRequest(batchCount -> {
			if (!isDisposed()) {
				long itemDemand = Operators.multiplyCap(batchCount, BATCH_SIZE);
				LOGGER.debug("request({})", itemDemand);
				request(itemDemand);
			}
		});
	}

	@Override
	protected void hookOnNext(RakutenItem item) {
		LOGGER.debug("onNext() item={}", ++itemCount);
		pending.add(item);
		if (pending.size() == BATCH_SIZE) {
			emitBatch();
		}
	}

	@Override
	protected void hookOnComplete() {
		if (!pending.isEmpty()) {
			emitBatch();
		}
		batches.complete();
	}

	@Override
	protected void hookOnError(Throwable error) {
		batches.error(error);
	}

	private void emitBatch() {
		List<RakutenItem> batch = List.copyOf(pending);
		pending.clear();
		batches.next(batch);
	}
}
