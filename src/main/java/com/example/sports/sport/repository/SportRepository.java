package com.example.sports.sport.repository;

import com.example.sports.sport.Sport;
import io.r2dbc.postgresql.api.PostgresqlException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface SportRepository extends ReactiveCrudRepository<Sport, Integer> {

	@Modifying
	@Query("""
			INSERT INTO sport (name) VALUES (:name)
			ON CONFLICT (lower(name)) DO NOTHING
			""")
	Mono<Boolean> insertIfMissing(@Param("name") String name);

	@Query("""
			SELECT id, name FROM sport
			WHERE strpos(lower(name), lower(:query)) > 0
			ORDER BY lower(name), id
			""")
	Flux<Sport> searchByName(@Param("query") String query);

	static boolean isNameConflict(Throwable error) {
		return error instanceof DuplicateKeyException
				&& error.getCause() instanceof PostgresqlException postgres
				&& postgres.getErrorDetails().getConstraintName().filter("sport_name_ci_unique"::equals).isPresent();
	}
}
