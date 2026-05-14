package org.example.gtfsynq.ingest.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.ingest.config.GtfsProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Service responsible for processing GTFS feeds at regular intervals
 */
@Service
@Slf4j
@AllArgsConstructor
public class GtfsIngestionService {

	private final GtfsProperties gtfsConfig;
	private final GtfsIngestionAsyncService ingestionAsyncService;
	private final AtomicBoolean isRunning = new AtomicBoolean(false);

	/**
	 * Scheduled task to process GTFS feeds at regular intervals
	 */
	@Scheduled(fixedRateString = "${gtfsynq.polling.interval-ms}")
	public void process() {
		// Prevent concurrent executions
		if (!isRunning.compareAndSet(false, true)) {
			log.info("Previous polling is still running, skipping this iteration");
			return;
		}

		var startTime = System.currentTimeMillis();

		try {
			log.debug("Starting GTFS ingestion for {} sources", gtfsConfig.sources().size());

			gtfsConfig.sources().forEach(this::processFeedGroup);

			log.info("Total processing time: {}ms", System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			log.error("Critical error during ingestion process", e);
		} finally {
			isRunning.set(false);
		}
	}

	/**
	 * Processes a group of GTFS feed URLs for a given feed ID
	 *
	 * @param feedId The ID of the feed to process
	 * @param source The feed source configuration
	 */
	private void processFeedGroup(String feedId, GtfsProperties.FeedSource source) {
		var urls = source.realtimeConfig().urls();

		// Map URLs to futures and wait for completion
		var futures = urls.stream()
				.map(url -> ingestionAsyncService.processFeedUrlAsync(feedId, url))
				.toArray(CompletableFuture[]::new);

		CompletableFuture.allOf(futures).join();
	}
}
