package org.example.sirianalyzer.model;

import java.util.Arrays;

/**
 * Represents a hash of a GTFS-RT entity, used for comparing entity equality
 */
public record EntityHash(byte[] value) {
    @Override
    public boolean equals(Object obj) {
        return (
            this == obj ||
            (obj instanceof EntityHash other &&
                Arrays.equals(this.value, other.value))
        );
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
