package org.example.sirianalyzer.model;

/**
 * Represents a storage key for a GTFS-RT entity, consisting of a feed ID and a stable ID
 */
public record StorageKey(String value) {
    public static StorageKey of(String feedId, String stableId) {
        return new StorageKey(feedId + ":" + stableId);
    }
}
