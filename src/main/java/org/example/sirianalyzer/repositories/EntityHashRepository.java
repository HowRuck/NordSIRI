package org.example.sirianalyzer.repositories;

import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private final RedisTemplate<String, String> redisTemplate;

    @SuppressWarnings("unchecked")
    private static final RedisScript<List<String>> SYNC_SCRIPT = RedisScript.of(
        """
        local changed = {}
        for i, k in ipairs(KEYS) do
            if redis.call('GET', k) ~= ARGV[i] then
                redis.call('SET', k, ARGV[i])
                table.insert(changed, k)
            end
        end
        return changed
        """,
        (Class<List<String>>) (Class<?>) List.class
    );

    public Set<String> syncHashes(List<String> keys, List<Long> hashes) {
        if (keys.isEmpty()) return Set.of();

        var args = hashes.stream().map(String::valueOf).toArray();

        var result = redisTemplate.execute(SYNC_SCRIPT, keys, args);

        // Copy the result to a Set to speed up lookups
        return result == null ? Set.of() : Set.copyOf(result);
    }
}
