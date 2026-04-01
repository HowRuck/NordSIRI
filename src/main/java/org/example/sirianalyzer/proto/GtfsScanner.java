package org.example.sirianalyzer.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;

public class GtfsScanner {

    // Protobuf Field Tags for GTFS-RT
    private static final int TAG_TRIP_UPDATE = 3; // FeedEntity.trip_update
    private static final int TAG_VEHICLE_POS = 4; // FeedEntity.vehicle
    private static final int TAG_ALERT = 5; // FeedEntity.alert

    private static final int TAG_TRIP_DESC = 1; // TripUpdate.trip or VehiclePosition.trip
    private static final int TAG_TRIP_ID = 1; // TripDescriptor.trip_id
    private static final int TAG_START_DATE = 3; // TripDescriptor.start_date

    public record ScanResult(String stableId, int type) {}

    public static ScanResult scanEntity(CodedInputStream cis)
        throws IOException {
        String stableId = null;
        int entityType = -1;

        while (!cis.isAtEnd()) {
            var tag = cis.readTag();
            var field = WireFormat.getTagFieldNumber(tag);

            switch (field) {
                case TAG_TRIP_UPDATE:
                case TAG_VEHICLE_POS:
                    entityType = field;
                    stableId = parseNestedTripDescriptor(cis);
                    break;
                case TAG_ALERT:
                    entityType = field;
                    cis.skipField(tag);
                    break;
                default:
                    cis.skipField(tag);
                    break;
            }
        }

        return new ScanResult(stableId, entityType);
    }

    public static String parseNestedTripDescriptor(CodedInputStream cis)
        throws IOException {
        var length = cis.readRawVarint32();
        var oldLimit = cis.pushLimit(length);
        String tid = "",
            sd = "";

        try {
            while (!cis.isAtEnd()) {
                var tag = cis.readTag();

                if (WireFormat.getTagFieldNumber(tag) == TAG_TRIP_DESC) {
                    var innerLen = cis.readRawVarint32();
                    var innerLimit = cis.pushLimit(innerLen);

                    while (!cis.isAtEnd()) {
                        var innerTag = cis.readTag();

                        switch (WireFormat.getTagFieldNumber(innerTag)) {
                            case TAG_TRIP_ID:
                                tid = cis.readString();
                                break;
                            case TAG_START_DATE:
                                sd = cis.readString();
                                break;
                            default:
                                cis.skipField(innerTag);
                                break;
                        }
                    }
                    cis.popLimit(innerLimit);
                } else {
                    cis.skipField(tag);
                }
            }
        } finally {
            cis.popLimit(oldLimit);
        }

        return tid.isEmpty() ? null : tid + ":" + sd;
    }
}
