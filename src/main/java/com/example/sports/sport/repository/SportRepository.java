package com.example.sports.sport.repository;

import com.example.sports.sport.Sport;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface SportRepository extends ReactiveCrudRepository<Sport, Integer> {

	@Modifying
	@Query("""
			INSERT INTO sport (name) VALUES (:name)
			ON CONFLICT (lower(name)) DO NOTHING
			""")
	Mono<Boolean> insertIfMissing(@Param("name") String name);
}
