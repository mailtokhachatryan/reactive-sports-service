package com.example.sports.sport.web;

import com.example.sports.sport.service.DuplicateSportException;
import com.example.sports.sport.service.InvalidSportInputException;
import com.example.sports.sport.service.SportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Component
class SportHandler {

	static final String SPORT_NAME_VARIABLE = "sportname";
	private final SportService service;

	SportHandler(SportService service) {
		this.service = service;
	}

	Mono<ServerResponse> create(ServerRequest request) {
		return service.create(request.pathVariable(SPORT_NAME_VARIABLE))
				.map(SportResponse::from)
				.flatMap(sport -> ServerResponse.status(HttpStatus.CREATED)
						.contentType(MediaType.APPLICATION_JSON)
						.bodyValue(sport))
				.onErrorResume(DuplicateSportException.class, error ->
						problem(HttpStatus.CONFLICT, "SPORT_NAME_EXISTS", error.getMessage()))
				.onErrorResume(InvalidSportInputException.class, SportHandler::badInput);
	}

	Mono<ServerResponse> search(ServerRequest request) {
		return service.search(request.queryParam("q").orElse(null))
				.map(SportResponse::from)
				.collectList()
				.flatMap(sports -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(sports))
				.onErrorResume(InvalidSportInputException.class, SportHandler::badInput);
	}

	private static Mono<ServerResponse> badInput(InvalidSportInputException error) {
		return problem(HttpStatus.BAD_REQUEST, "INVALID_SPORT_INPUT", error.getMessage());
	}

	private static Mono<ServerResponse> problem(HttpStatus status, String code, String detail) {
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
		problem.setProperty("code", code);
		return ServerResponse.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).bodyValue(problem);
	}
}
