package org.example.sirianalyzer.proto.dedup;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.example.sirianalyzer.proto.PendingUpdate;
import org.lmdbjava.Txn;

/**
 * A functional interface for filtering entities based on update requirements
 * <br/>
 * Implementations dictate whether an entity should be updated by examining its
 * current state and previous processing results.
 */
@FunctionalInterface
public interface EntityUpdateFilter {
    PendingUpdate getPendingUpdateIfChanged(
        ByteString entityBytes,
        Txn<ByteBuffer> txn
    ) throws IOException;
}
