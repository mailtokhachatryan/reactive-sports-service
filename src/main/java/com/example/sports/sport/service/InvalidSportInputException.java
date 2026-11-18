package com.example.sports.sport.service;

public class InvalidSportInputException extends RuntimeException {

	public InvalidSportInputException(String message) {
		super(message);
	}
}
