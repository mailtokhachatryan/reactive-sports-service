package com.example.sports.ingestion.rakuten;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
record RakutenPage(Integer pageCount, @JsonAlias("Items") List<RakutenItem> items) {
}
