package org.example.sirianalyzer.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import lombok.NoArgsConstructor;
import org.example.sirianalyzer.model.EntityHash;

/**
 * Hashing utilities for GTFS FeedEntity objects using Murmur3 128-bit
 */
@NoArgsConstructor
public final class FeedHashing {

    private static final HashFunction STATE_HASHER = Hashing.murmur3_128();

    /**
     * Hash the raw bytes of a GTFS entity
     *
     * @param rawBytes Raw bytes of the GTFS entity
     * @return Hash of the raw bytes
     */
    public static EntityHash hashBytes(ByteString rawBytes) {
        var roBuffer = rawBytes.asReadOnlyByteBuffer();

        return new EntityHash(STATE_HASHER.hashBytes(roBuffer).asBytes());
    }
}
