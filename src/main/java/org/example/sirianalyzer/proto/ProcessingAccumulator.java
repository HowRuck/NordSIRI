package org.example.sirianalyzer.proto;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.lmdbjava.Txn;

/**
 * Mutable state accumulated during a single GTFS parse run.
 * Sized using the previous run's changed count to avoid ArrayList resizing in steady state.
 */
public final class ProcessingAccumulator {

    public final Txn<ByteBuffer> readTxn;
    private final ArrayList<PendingUpdate> pendingUpdates;
    public int feedEntityCount;

    public ProcessingAccumulator(Txn<ByteBuffer> readTxn, int initialCapacity) {
        this.readTxn = readTxn;
        this.pendingUpdates = new ArrayList<>(Math.max(initialCapacity, 16));
    }

    public void addUpdate(PendingUpdate update) {
        pendingUpdates.add(update);
    }

    public List<PendingUpdate> getPendingUpdates() {
        return pendingUpdates;
    }

    public int pendingUpdateCount() {
        return pendingUpdates.size();
    }

    public ArrayList<ByteString> collectChangedBytes(GtfsEntityType type) {
        var result = new ArrayList<ByteString>();
        for (var update : pendingUpdates) {
            if (update.type() == type) {
                result.add(update.entityBytes());
            }
        }
        return result;
    }
}
