package org.example.sirianalyzer.model;

import java.util.ArrayList;

import com.google.protobuf.ByteString;

/**
 * Wrapper for a GTFS feed in byte format
 */
public record GtfsByteFeedWrapper(
        /**
         * Trip updates
         */
        ArrayList<ByteString> tripUpdates,
        ArrayList<ByteString> vehiclePositions,
        ArrayList<ByteString> alerts) {
}
