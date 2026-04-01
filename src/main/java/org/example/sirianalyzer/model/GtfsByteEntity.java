package org.example.sirianalyzer.model;

import com.google.protobuf.ByteString;
import org.example.sirianalyzer.proto.GtfsEntityType;

public record GtfsByteEntity(
    String stableId,
    GtfsEntityType type,
    ByteString data
) {
    public static GtfsByteEntity of(
        String stableId,
        int type,
        ByteString data
    ) {
        return new GtfsByteEntity(
            stableId,
            GtfsEntityType.values()[type - 3],
            data
        );
    }
}
