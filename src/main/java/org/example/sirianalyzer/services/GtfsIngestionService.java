package org.example.sirianalyzer.services;

import com.google.common.hash.Hashing;
import com.google.transit.realtime.GtfsRealtime;
import org.example.sirianalyzer.util.SizeFormat;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Service for ingesting GTFS real-time data and storing it in LMDB
 */
@Service
public class GtfsIngestionService {

    private static final Logger log = LoggerFactory.getLogger(GtfsIngestionService.class);

    private final RestClient restClient;
    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final String gtfsFeedUrl;

    private long lastFeedHash;

    /**
     * Create a new GTFS ingestion service with the provided dependencies
     *
     * @param builder RestClient builder
     * @param env LMDB environment
     * @param db LMDB database
     * @param gtfsFeedUrl URL of the GTFS feed to fetch
     */
    public GtfsIngestionService(RestClient.Builder builder,
                                Env<ByteBuffer> env,
                                Dbi<ByteBuffer> db,
                                @Value("${gtfs.feed.url}") String gtfsFeedUrl) {
        this.restClient = builder.build();
        this.env = env;
        this.db = db;
        this.gtfsFeedUrl = gtfsFeedUrl;
    }

    /**
     * Fetch and process GTFS feed data from the configured URL
     */
    @Scheduled(fixedRateString = "${gtfs.fetch.interval-ms}")
    public void fetchAndProcess() {
        log.info("Starting GTFS fetch from {}", gtfsFeedUrl);

        long startFetch = System.currentTimeMillis();

        byte[] feedBytes;
        try {
            feedBytes = restClient.get()
                    .uri(gtfsFeedUrl)
                    .retrieve()
                    .body(byte[].class);
        } catch (Exception e) {
            log.error("Failed to fetch GTFS feed", e);
            return;
        }

        if (feedBytes == null || feedBytes.length == 0) {
            log.warn("Empty GTFS feed received");
            return;
        }

        var fetchDuration = System.currentTimeMillis() - startFetch;
        log.info("Fetched GTFS feed ({}) in {} ms", SizeFormat.humanBytes(feedBytes.length), fetchDuration);

        var currentFeedHash = Hashing.murmur3_128().hashBytes(feedBytes).asLong();
        if (currentFeedHash == lastFeedHash) {
            log.info("GTFS feed unchanged, skipping");
            return;
        }
        lastFeedHash = currentFeedHash;

        long startParse = System.currentTimeMillis();
        GtfsRealtime.FeedMessage feed;

        try (InputStream is = new ByteArrayInputStream(feedBytes)) {
            feed = GtfsRealtime.FeedMessage.parseFrom(is);
        } catch (IOException e) {
            log.error("Failed to parse GTFS feed", e);
            return;
        }
        var parseDuration = System.currentTimeMillis() - startParse;
        log.info("Parsed feed with {} entities in {} ms", feed.getEntityCount(), parseDuration);

        var startDedup = System.currentTimeMillis();
        var newOrUpdatedCount = processStreamParallel(feed);
        var dedupDuration = System.currentTimeMillis() - startDedup;

        log.info("De-duplication & LMDB write complete: {} new/updated entities in {} ms", newOrUpdatedCount, dedupDuration);
    }

    /**
     * Process a GTFS feed message in parallel, computing hashes and keys for each entity
     *
     * @param feed The GTFS feed message to process
     * @return The number of new or updated entities in the feed
     */
    private int processStreamParallel(GtfsRealtime.FeedMessage feed) {
        // 1. Compute hashes and keys in parallel
        var changedEntities = feed.getEntityList().parallelStream()
                .map(entity -> {
                    try {
                        // Compute hash of the entity and prepare LMDB key
                        var hash = Hashing.murmur3_128().hashBytes(entity.toByteArray()).asLong();
                        var idBytes = entity.getId().getBytes();

                        var key = ByteBuffer.allocateDirect(idBytes.length).put(idBytes).flip();

                        return new EntityHash(entity, key, hash);
                    } catch (Exception e) {
                        log.error("Failed to hash entity: {}", entity.getId(), e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        int count = 0;

        // 2. Single LMDB write transaction for all changes
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (EntityHash eh : changedEntities) {
                // Check if entity already exists in LMDB
                var val = db.get(txn, eh.key);
                var previousHash = val != null ? val.getLong(0) : -1;

                // Only write if hash has changed
                if (previousHash != eh.hash) {
                    var value = ByteBuffer.allocateDirect(8).putLong(eh.hash).flip();
                    db.put(txn, eh.key, value);
                    count++;
                }
            }
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to process feed batch", e);
        }

        return count;
    }

    /**
     * Record to hold precomputed key/hash for parallel processing
     *
     * @param entity The GTFS entity
     * @param key The LMDB key for the entity
     * @param hash The hash of the entity
     */
    private record EntityHash(GtfsRealtime.FeedEntity entity, ByteBuffer key, long hash) {}
}