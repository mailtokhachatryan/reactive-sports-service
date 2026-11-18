package com.example.sports.ingestion.rakuten;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

public final class RakutenStub implements AutoCloseable {
	private static final String SEARCH_PATH = "/ichibams/api/IchibaItem/Search/20260701";
	private final HttpServer server;
	private final List<Request> requests = new CopyOnWriteArrayList<>();

	public RakutenStub(Function<Request, Response> responder) throws IOException {
		this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext(SEARCH_PATH, exchange -> {
			try (exchange) {
				Request request = new Request(exchange.getRequestMethod(), exchange.getRequestURI().getRawQuery(),
						exchange.getRequestHeaders().getFirst("accessKey"));
				requests.add(request);
				Response response = responder.apply(request);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				if (response.retryAfter() != null) {
					exchange.getResponseHeaders().set("Retry-After", response.retryAfter());
				}
				byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
				exchange.sendResponseHeaders(response.status(), body.length);
				exchange.getResponseBody().write(body);
			}
		});
		server.start();
	}

	public URI endpoint() {
		return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + SEARCH_PATH);
	}

	public List<Request> requests() {
		return List.copyOf(requests);
	}

	@Override
	public void close() {
		server.stop(0);
	}

	public record Request(String method, String query, String accessKey) {
	}

	public record Response(int status, String body, String retryAfter) {
		public Response(int status, String body) {
			this(status, body, null);
		}
	}
}
