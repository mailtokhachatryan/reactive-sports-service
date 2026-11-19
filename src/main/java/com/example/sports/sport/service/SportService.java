package com.example.sports.sport.service;

import com.example.sports.sport.Sport;
import com.example.sports.sport.repository.SportRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class SportService {

	private static final int MAX_INPUT_LENGTH = 100;
	private final SportRepository repository;

	public SportService(SportRepository repository) {
		this.repository = repository;
	}

	public Mono<Sport> create(String name) {
		return Mono.defer(() -> repository.save(new Sport(null, validate(name, "Sport name"))))
				.onErrorMap(SportRepository::isNameConflict, DuplicateSportException::new);
	}

	public Flux<Sport> search(String query) {
		return Flux.defer(() -> repository.searchByName(validate(query, "Query parameter q")));
	}

	public Mono<Boolean> importSport(String name) {
		return Mono.defer(() -> repository.insertIfMissing(validate(name, "Sport name")));
	}

	private static String validate(String input, String field) {
		if (input == null || input.isBlank()) {
			throw new InvalidSportInputException(field + " must not be blank.");
		}
		String value = input.strip();
		if (value.codePointCount(0, value.length()) > MAX_INPUT_LENGTH) {
			throw new InvalidSportInputException(field + " must have at most " + MAX_INPUT_LENGTH + " characters.");
		}
		if (value.codePoints().anyMatch(Character::isISOControl)) {
			throw new InvalidSportInputException(field + " must not contain control characters.");
		}
		return value;
	}
}
