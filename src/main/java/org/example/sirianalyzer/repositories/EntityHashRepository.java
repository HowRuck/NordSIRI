package org.example.sirianalyzer.repositories;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.example.sirianalyzer.model.EntityHash;
import org.example.sirianalyzer.model.StorageKey;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository for storing and retrieving entity hashes from Redis
 */
@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private final RedisTemplate<String, byte[]> redisTemplate;
    private final MeterRegistry registry;

    /**
     * Retrieves the hash value for the given storage key
     *
     * @param key The storage key to retrieve the hash for
     * @return The hash value, or null if not found
     */
    public byte[] getHash(StorageKey key) {
        return redisTemplate.opsForValue().get(key.value());
    }

    /**
     * Retrieves the hash values for the given storage keys
     *
     * @param keys The storage keys to retrieve the hashes for
     * @return A list of {@link EntityHash} objects, one for each key, with null values for keys not found
     */
    public List<EntityHash> getHashBatch(List<StorageKey> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }

        // Convert StorageKey to String keys for Redis multi-get
        var stringKeys = keys.stream().map(StorageKey::value).toList();
        // Perform multi-get to retrieve values for all keys
        var values = redisTemplate.opsForValue().multiGet(stringKeys);

        // If values are null, return a list of EntityHash with null values for each key
        // otherwise, map the values to EntityHash objects
        if (values == null) {
            return keys
                .stream()
                .map(_ -> new EntityHash(null))
                .toList();
        }
        return values.stream().map(EntityHash::new).toList();
    }

    /**
     * Upserts (inserts or updates) a batch of entity hashes in Redis
     *
     * @param source The source of the batch (e.g. feed ID)
     * @param batch  A map of storage keys to entity hashes to upsert
     */
    public void upsertBatch(String source, Map<StorageKey, EntityHash> batch) {
        if (batch.isEmpty()) {
            return;
        }

        // Record the start time for the batch upsert and increment the write counter
        var sample = Timer.start(registry);
        registry
            .counter("gtfs.redis.writes", "source", source)
            .increment(batch.size());

        // Convert the batch to a map of String keys to byte[] values for Redis multi-set
        var rawBatch = batch
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().value(),
                    entry -> entry.getValue().value()
                )
            );

        // Perform multi-set to upsert all values in the batch
        redisTemplate.opsForValue().multiSet(rawBatch);

        sample.stop(
            registry.timer("gtfs.redis.commit.latency", "source", source)
        );
    }
}
