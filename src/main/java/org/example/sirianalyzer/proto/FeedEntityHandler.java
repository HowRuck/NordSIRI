package org.example.sirianalyzer.proto;

import com.google.protobuf.ByteString;
import java.io.IOException;

/**
 * Strategy for processing one type of {@code FeedEntity} payload.
 *
 * <p>Implementations are Spring {@code @Component}s auto-collected into
 * {@code GtfsParserService}'s dispatch table by {@link #fieldNumber()}.
 * Adding a new entity type requires no changes to the orchestrator.</p>
 */
public interface FeedEntityHandler {
    /** Proto field number inside {@code FeedEntity} (3=TripUpdate, 4=Vehicle, 5=Alert). */
    int fieldNumber();

    GtfsEntityType entityType();

    /** Reset per-run counters before processing starts. */
    void beforeRun();

    /** Process one raw payload extracted from a {@code FeedEntity}. */
    void processPayload(ByteString payload, ProcessingAccumulator acc)
        throws IOException;

    /** Publish Micrometer metrics for the completed run. */
    void afterRun(long runDurationNs);
}
