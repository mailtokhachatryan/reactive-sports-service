package com.example.sports.sport.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration(proxyBeanMethods = false)
class SportRoutes {

	private static final String SPORT_PATH = "/api/v1/sport";

	@Bean
	RouterFunction<ServerResponse> sportsRouter(SportHandler handler) {
		return RouterFunctions.route()
				.POST(SPORT_PATH + "/{" + SportHandler.SPORT_NAME_VARIABLE + "}", handler::create)
				.GET(SPORT_PATH, handler::search)
				.build();
	}
}
