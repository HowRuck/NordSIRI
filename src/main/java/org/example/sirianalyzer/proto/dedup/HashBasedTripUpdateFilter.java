package org.example.sirianalyzer.proto.dedup;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.RequiredArgsConstructor;
import org.example.sirianalyzer.proto.GtfsEntityType;
import org.example.sirianalyzer.proto.PendingUpdate;
import org.example.sirianalyzer.proto.ProcessingAccumulator;
import org.example.sirianalyzer.repositories.GtfsStateRepository;
import org.example.sirianalyzer.util.FeedHashing;
import org.example.sirianalyzer.util.ProtoUtils;
import org.lmdbjava.Txn;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * Filter for TripUpdate entities based on hash comparison
 */
@Primary
@Component
@RequiredArgsConstructor
public class HashBasedTripUpdateFilter implements EntityUpdateFilter {

    private static final int TRIP_DESCRIPTOR_FIELD = 1;
    private static final int TRIP_ID_FIELD = 1;

    private final GtfsStateRepository stateRepo;

    /**
     * Extract the trip ID from a TripUpdate entity and store/update the hash in the state repository.
     *
     * <p>
     * If the hash matches the stored value, the entity is skipped.
     * Otherwise, it is added to the pending updates using the provided {@link ProcessingAccumulator}
     * </p>
     *
     * @param entityBytes ByteString containing the TripUpdate entity
     * @param acc         ProcessingAccumulator for storing pending updates
     * @throws IOException If an error occurs while reading the entity
     */
    @Override
    public PendingUpdate getPendingUpdateIfChanged(
        ByteString entityBytes,
        Txn<ByteBuffer> txn
    ) throws IOException {
        final var tripId = extractTripId(entityBytes);
        if (tripId == null) return null;

        final var hash = FeedHashing.hashBytes(entityBytes);
        if (!stateRepo.hasChanged(txn, tripId, hash)) {
            return null;
        }

        return PendingUpdate.ofSingleEntry(
            GtfsEntityType.TRIP_UPDATE,
            entityBytes,
            tripId,
            hash
        );
    }

    /**
     * Extract the trip ID from a TripUpdate entity
     *
     * @param tuBytes ByteString containing the TripUpdate entity
     * @return ByteString containing the trip ID, or null if not found
     * @throws IOException If an error occurs while reading the entity
     */
    private ByteString extractTripId(ByteString tuBytes) throws IOException {
        final var tuInput = tuBytes.newCodedInput();
        final var tripDescriptorBytes =
            ProtoUtils.findFirstLengthDelimitedField(
                tuInput,
                TRIP_DESCRIPTOR_FIELD
            );

        if (tripDescriptorBytes == null) {
            return null;
        }

        return ProtoUtils.findFirstLengthDelimitedField(
            tripDescriptorBytes.newCodedInput(),
            TRIP_ID_FIELD
        );
    }
}
