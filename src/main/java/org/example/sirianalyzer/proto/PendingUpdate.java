package org.example.sirianalyzer.proto;

import com.google.protobuf.ByteString;
import java.util.List;

/**
 * An entity whose content has changed since the last parse run.
 *
 * <p>{@code lmdbWrites} contains one entry for coarse dedup (full-entity hash)
 * or N entries for fine-grained dedup (e.g. one per changed {@code StopTimeUpdate}).
 * {@code entityBytes} is always the full payload forwarded downstream.</p>
 */
public record PendingUpdate(
    GtfsEntityType type,
    ByteString entityBytes,
    List<LmdbEntry> lmdbWrites
) {
    /** Convenience factory for the common single-write case. */
    public static PendingUpdate ofSingleEntry(
        GtfsEntityType type,
        ByteString entityBytes,
        ByteString key,
        long hash
    ) {
        return new PendingUpdate(
            type,
            entityBytes,
            List.of(new LmdbEntry(key, hash))
        );
    }
}
