package com.example.sports.sport.service;

public class DuplicateSportException extends RuntimeException {

	public DuplicateSportException(Throwable cause) {
		super("A sport with this name already exists.", cause);
	}
}
