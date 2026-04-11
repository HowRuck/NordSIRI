package org.example.sirianalyzer.proto;

import com.google.protobuf.CodedInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.hashing.LongHashFunction;
import org.apache.kafka.shaded.com.google.protobuf.WireFormat;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
/**
 * A filter that parses native GTFS feeds from input streams
 */
public class GtfsNativeFilter {

    private final OffHeapHashStore stateStore;

    private final LongHashFunction hashFunction = LongHashFunction.xx3();
    private int lastUpdateCount = 10_000;

    public record TypedEntity(byte[] bytes, long hash) {}

    @Value("${gtfs.readBufferSize:8}")
    private int readBufferSizeMb;

    /**
     * Check if the feed header has changed and update the state store if it has
     *
     * @param feedId The feed ID to use for entity IDs
     * @param feedUrl The feed URL to use for entity IDs
     * @param buffer The buffer containing the feed header
     *
     * @return true if the header has changed, false otherwise
     *
     * @throws IOException If an error occurs while reading the buffer
     */
    private boolean checkHeaderChanged(
        String feedId,
        String feedUrl,
        byte[] buffer
    ) throws IOException {
        var headerKey = hashFunction.hashChars(feedId + feedUrl);
        var headerHash = hashFunction.hashBytes(buffer);

        var existingHeaderHash = stateStore.get(headerKey);

        if (headerHash == existingHeaderHash) {
            return false;
        }

        stateStore.put(headerKey, headerHash);
        return true;
    }

    /**
     * Process a single feed entity and return a TypedEntity with the entity's bytes and hash
     *
     * @param entityBytes The entity's bytes
     * @param feedIdChars The feed ID as a char array
     * @param feedIdWithPadding The feed ID with padding as a char array
     *
     * @return A TypedEntity with the entity's bytes and hash
     *
     * @throws IOException If an error occurs while reading the entity
     */
    private TypedEntity processFeedEntity(byte[] entityBytes, String feedId)
        throws IOException {
        var entityCis = CodedInputStream.newInstance(entityBytes);
        var scanResult = GtfsScanner.scanEntity(entityCis);

        var entityId = feedId + ":" + scanResult.id();

        var hashedId = hashFunction.hashBytes(entityId.getBytes());
        var hashedBytes = hashFunction.hashBytes(entityBytes);

        var existingHash = stateStore.get(hashedId);

        if (existingHash == OffHeapHashStore.EMPTY_VALUE) {
            stateStore.put(hashedId, hashedBytes);
            return new TypedEntity(entityBytes, hashedBytes);
        }

        if (hashedBytes == existingHash) {
            return null;
        }

        stateStore.put(hashedId, hashedBytes);

        return new TypedEntity(entityBytes, hashedBytes);
    }

    /**
     * Parse a native GTFS feed from an input stream
     *
     * @param feedId The feed ID to use for entity IDs
     * @param feedUrl The feed URL to use for entity IDs
     * @param is The input stream to read from
     * @return A list of parsed entities
     *
     * @throws IOException If an error occurs while reading the stream
     */
    public List<TypedEntity> parseNative(
        String feedId,
        String feedUrl,
        InputStream is
    ) throws IOException {
        var bufferSize = readBufferSizeMb * 1024 * 1024;
        var cis = CodedInputStream.newInstance(
            new BufferedInputStream(is, bufferSize)
        );

        var changedEntities = new ArrayList<TypedEntity>(lastUpdateCount);

        var numEntities = 0;
        var numChanged = 0;

        while (!cis.isAtEnd()) {
            var tag = cis.readTag();
            var fieldNumber = WireFormat.getTagFieldNumber(tag);

            if (fieldNumber == 0) {
                break;
            } else if (fieldNumber == 1) {
                var headerBytes = cis.readByteArray();

                if (!checkHeaderChanged(feedId, feedUrl, headerBytes)) {
                    log.info("Header hash matches existing hash, breaking");

                    break;
                }
            } else if (fieldNumber == 2) {
                var entityBytes = cis.readByteArray();
                var typedEntity = processFeedEntity(entityBytes, feedId);
                numEntities++;

                if (typedEntity != null) {
                    changedEntities.add(typedEntity);
                    numChanged++;
                }
            }
        }

        lastUpdateCount = changedEntities.size();

        log.info(
            "Parsed {} entities, {} changed",
            SizeFormat.formatNumber(numEntities),
            SizeFormat.formatNumber(numChanged)
        );

        return changedEntities;
    }
}
