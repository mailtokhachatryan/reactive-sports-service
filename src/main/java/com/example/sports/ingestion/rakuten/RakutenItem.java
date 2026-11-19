package com.example.sports.ingestion.rakuten;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RakutenItem(Long genreId) {
}
