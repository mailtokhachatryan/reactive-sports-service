package com.example.sports.ingestion;

record ImportSummary(long inserted, long existing, long unmapped) {

	ImportSummary add(ImportOutcome outcome) {
		return switch (outcome) {
			case INSERTED -> new ImportSummary(inserted + 1, existing, unmapped);
			case EXISTING -> new ImportSummary(inserted, existing + 1, unmapped);
			case UNMAPPED -> new ImportSummary(inserted, existing, unmapped + 1);
		};
	}
}
