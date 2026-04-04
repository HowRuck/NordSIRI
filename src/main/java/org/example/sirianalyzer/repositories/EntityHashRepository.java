package org.example.sirianalyzer.repositories;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for storing and retrieving entity hashes from Redis
 */
@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private final RedisTemplate<String, Long> redisTemplate;
    private final MeterRegistry registry;

    /**
     * Retrieves the hash values for the given storage keys
     *
     * @param keys The storage keys to retrieve the hashes for
     * @return A list of {@link EntityHash} objects, one for each key, with null values for keys not found
     */
    public List<Long> getHashBatch(List<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }

        // Perform multi-get to retrieve values for all keys
        var values = redisTemplate.opsForValue().multiGet(keys);

        // If values are null, return a list of EntityHash with null values for each key
        // otherwise, map the values to EntityHash objects
        return values;
    }

    /**
     * Upserts (inserts or updates) a batch of entity hashes in Redis
     *
     * @param source The source of the batch (e.g. feed ID)
     * @param batch  A map of storage keys to entity hashes to upsert
     */
    public void upsertBatch(String source, Map<String, Long> batch) {
        if (batch.isEmpty()) {
            return;
        }

        // Record the start time for the batch upsert and increment the write counter
        var sample = Timer.start(registry);
        registry
            .counter("gtfs.redis.writes", "source", source)
            .increment(batch.size());

        // Perform multi-set to upsert all values in the batch
        redisTemplate.opsForValue().multiSet(batch);

        sample.stop(
            registry.timer("gtfs.redis.commit.latency", "source", source)
        );
    }
}
