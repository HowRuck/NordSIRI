package org.example.gtfsynq.util;

import lombok.NoArgsConstructor;
import net.openhft.hashing.LongHashFunction;

/**
 * Hashing utilities for GTFS FeedEntity objects using Murmur3 128-bit
 */
@NoArgsConstructor
public final class FeedHashing {

    private static final LongHashFunction STATE_HASHER = LongHashFunction.xx();

    /**
     * Hash the raw bytes of a GTFS entity
     *
     * @param rawBytes Raw bytes of the GTFS entity
     * @return Hash of the raw bytes
     */
    public static long hashBytes(byte[] rawBytes) {
        return STATE_HASHER.hashBytes(rawBytes);
    }
}
