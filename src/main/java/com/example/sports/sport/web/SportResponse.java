package com.example.sports.sport.web;

import com.example.sports.sport.Sport;

record SportResponse(int id, String name) {

	static SportResponse from(Sport sport) {
		return new SportResponse(sport.id(), sport.name());
	}
}
