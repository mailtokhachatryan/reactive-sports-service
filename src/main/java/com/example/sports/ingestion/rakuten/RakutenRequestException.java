package com.example.sports.ingestion.rakuten;

import java.time.Duration;

final class RakutenRequestException extends RuntimeException {

	private final boolean retryable;
	private final Duration retryAfter;

	RakutenRequestException(String message, boolean retryable, Duration retryAfter) {
		super(message);
		this.retryable = retryable;
		this.retryAfter = retryAfter;
	}
	boolean retryable() {
		return retryable;
	}

	Duration retryAfter() {
		return retryAfter;
	}
}
