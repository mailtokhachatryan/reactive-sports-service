package com.example.sports.sport;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("sport")
public record Sport(@Id Integer id, String name) {
}
