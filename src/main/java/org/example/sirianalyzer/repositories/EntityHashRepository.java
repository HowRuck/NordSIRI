package org.example.sirianalyzer.repositories;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Repository;

/**
 * Repository for storing and retrieving entity hashes from Redis.
 *
 * <p>This implementation uses Redis pipelining to reduce round trips for batched reads and writes.
 */
@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private static final StringRedisSerializer STRING_SERIALIZER =
        new StringRedisSerializer();
    private static final GenericToStringSerializer<Long> LONG_SERIALIZER =
        new GenericToStringSerializer<>(Long.class);

    private final RedisTemplate<String, Long> redisTemplate;
    private final MeterRegistry registry;

    /**
     * Retrieves the hash values for the given storage keys using a pipelined batch read.
     *
     * @param keys the storage keys to retrieve the hashes for
     * @return a list of hash values aligned with the input keys; missing keys are null
     */
    public List<Long> getHashBatch(List<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }

        var values = redisTemplate.executePipelined(
            (RedisCallback<Object>) connection -> {
                for (var key : keys) {
                    var rawKey = STRING_SERIALIZER.serialize(key);
                    if (rawKey != null) {
                        connection.stringCommands().get(rawKey);
                    }
                }
                return null;
            },
            LONG_SERIALIZER
        );

        var hashes = new ArrayList<Long>(values.size());

        for (var value : values) {
            if (value instanceof Long longValue) {
                hashes.add(longValue);
            } else {
                hashes.add(null);
            }
        }

        return hashes;
    }

    /**
     * Upserts a batch of entity hashes in Redis using pipelined writes.
     *
     * @param source the source of the batch, used for metrics
     * @param batch the map of storage keys to entity hashes to upsert
     */
    public void upsertBatch(String source, Map<String, Long> batch) {
        if (batch.isEmpty()) {
            return;
        }

        var sample = Timer.start(registry);
        registry
            .counter("gtfs.redis.writes", "source", source)
            .increment(batch.size());

        // Use pipelined writes for bulk upsert
        redisTemplate.executePipelined(
            (RedisCallback<Object>) connection -> {
                for (var entry : batch.entrySet()) {
                    // Serialize key and value using the configured serializers
                    var rawKey = STRING_SERIALIZER.serialize(entry.getKey());
                    var rawValue = LONG_SERIALIZER.serialize(entry.getValue());

                    // Only set if both key and value are non-null
                    if (rawKey != null && rawValue != null) {
                        connection.stringCommands().set(rawKey, rawValue);
                    }
                }
                return null;
            }
        );

        sample.stop(
            registry.timer("gtfs.redis.commit.latency", "source", source)
        );
    }
}
