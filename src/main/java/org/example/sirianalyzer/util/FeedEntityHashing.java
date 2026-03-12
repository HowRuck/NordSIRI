package org.example.sirianalyzer.util;

import com.google.common.hash.Hashing;
import com.google.transit.realtime.GtfsRealtime;
import lombok.NoArgsConstructor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Hashing utilities for GTFS FeedEntity objects using Murmur3 128-bit
 */
@NoArgsConstructor
public final class FeedEntityHashing {

    // Murmur3 uses little-endian byte order
    private static final VarHandle LONG_VIEW =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * Compute the state of a GTFS entity
     *
     * @param entity GTFS entity to compute state for
     * @return State of the entity
     */
    public static GtfsEntityState computeState(GtfsRealtime.FeedEntity entity) {
        // Construct key and hash values
        var keyBytes = entity.getIdBytes().toByteArray();

        var rawBytes = entity.toByteArray();
        var hashResult = Hashing.murmur3_128()
                .hashBytes(rawBytes)
                .asBytes();

        // Extract longs using VarHandle to skip object overhead
        var h1 = (long) LONG_VIEW.get(hashResult, 0);
        var h2 = (long) LONG_VIEW.get(hashResult, 8);

        return new GtfsEntityState(entity, keyBytes, h1, h2);
    }

    /**
     * Immutable record for GTFS entity state
     *
     * @param original   Original GTFS entity
     * @param keyBytes   Key bytes for the entity
     * @param h1         1st hash value
     * @param h2         2nd hash value
     */
    public record GtfsEntityState(
            GtfsRealtime.FeedEntity original,
            byte[] keyBytes,
            long h1,
            long h2
    ) {
    }
}