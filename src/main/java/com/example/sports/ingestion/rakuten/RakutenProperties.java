package com.example.sports.ingestion.rakuten;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

@ConfigurationProperties("sports.rakuten")
public record RakutenProperties(
		URI endpoint,
		String applicationId,
		String accessKey,
		int maxPagesPerGenre,
		Duration requestTimeout,
		Duration importTimeout,
		Map<Long, String> genreNames
) {
	public RakutenProperties {
		if (endpoint == null || endpoint.getHost() == null || endpoint.getRawUserInfo() != null
				|| endpoint.getRawQuery() != null || endpoint.getRawFragment() != null
				|| !("https".equalsIgnoreCase(endpoint.getScheme()) || isLocalHttp(endpoint))) {
			throw new IllegalArgumentException("Rakuten requires an HTTPS endpoint or a loopback HTTP stub.");
		}
		if (applicationId == null || applicationId.isBlank() || accessKey == null || accessKey.isBlank()) {
			throw new IllegalArgumentException("Set RAKUTEN_APPLICATION_ID and RAKUTEN_ACCESS_KEY to run the import.");
		}
		if (maxPagesPerGenre < 1 || maxPagesPerGenre > 100) {
			throw new IllegalArgumentException("Rakuten page limit must be between 1 and 100.");
		}
		if (requestTimeout == null || !requestTimeout.isPositive() || importTimeout == null || !importTimeout.isPositive()) {
			throw new IllegalArgumentException("Rakuten request and import timeouts must be positive.");
		}
		if (genreNames == null || genreNames.isEmpty()
				|| genreNames.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey() <= 0
						|| entry.getValue() == null || entry.getValue().isBlank())) {
			throw new IllegalArgumentException("Configure positive Rakuten genre IDs with nonblank sport names.");
		}
		genreNames = Map.copyOf(genreNames);
	}

	private static boolean isLocalHttp(URI endpoint) {
		return "http".equalsIgnoreCase(endpoint.getScheme())
				&& ("127.0.0.1".equals(endpoint.getHost()) || "localhost".equalsIgnoreCase(endpoint.getHost()));
	}

	@Override
	public String toString() {
		return "RakutenProperties[maxPagesPerGenre=" + maxPagesPerGenre + ", requestTimeout=" + requestTimeout
				+ ", importTimeout=" + importTimeout + ", genreCount=" + genreNames.size() + "]";
	}
}
