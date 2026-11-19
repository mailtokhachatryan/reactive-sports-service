package com.example.sports.ingestion;

import com.example.sports.ingestion.rakuten.RakutenClient;
import com.example.sports.ingestion.rakuten.RakutenItem;
import com.example.sports.ingestion.rakuten.RakutenProperties;
import com.example.sports.sport.service.SportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Profile("import")
@EnableConfigurationProperties(RakutenProperties.class)
@Component
class Setup implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(Setup.class);
	private final RakutenClient client;
	private final RakutenProperties properties;
	private final SportService sportService;

	Setup(RakutenClient client, RakutenProperties properties, SportService sportService) {
		this.client = client;
		this.properties = properties;
		this.sportService = sportService;
	}

	@Override
	public void run(ApplicationArguments args) {
		importItems(client.items())
				.doOnNext(summary -> LOGGER.info("Rakuten import completed. Inserted={}, existing={}, unmapped={}",
						summary.inserted(), summary.existing(), summary.unmapped()))
				.block(properties.importTimeout());
	}

	Mono<ImportSummary> importItems(Flux<RakutenItem> items) {
		return items.concatMap(this::persist, 0)
				.reduce(new ImportSummary(0, 0, 0), ImportSummary::add);
	}

	private Mono<ImportOutcome> persist(RakutenItem item) {
		String name = properties.genreNames().get(item.genreId());
		if (name == null) {
			return Mono.just(ImportOutcome.UNMAPPED);
		}
		return sportService.importSport(name)
				.map(inserted -> inserted ? ImportOutcome.INSERTED : ImportOutcome.EXISTING);
	}
}
