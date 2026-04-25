package org.example.gtfsynq.infrastructure.protobuf;

import com.google.protobuf.CodedInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.openhft.hashing.LongHashFunction;
import org.apache.kafka.shaded.com.google.protobuf.WireFormat;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapHashStore;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapLongTable;
import org.example.gtfsynq.util.SizeFormat;
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

    /**
     * A typed entity with the entity's bytes and type
     */
    public record TypedEntity(
        byte[] bytes,
        long type,
        boolean isNew,
        int[] changedFields
    ) {
        /**
         * Encodes this TypedEntity into a byte array
         */
        public byte[] encode() {
            var bytesLen = (bytes == null) ? 0 : bytes.length;
            var fieldsLen = (changedFields == null) ? 0 : changedFields.length;

            // Calculate total size and allocate buffer
            // 8 (type) + 1 (isNew) + 4 (bytesLen) + bytesLen + 4 (fieldsLen) + fieldsLen * 4
            var totalSize = 8 + 1 + 4 + bytesLen + 4 + (fieldsLen * 4);
            var buffer = ByteBuffer.allocate(totalSize);

            // Write metadata
            buffer.putLong(type);
            buffer.put((byte) (isNew ? 1 : 0));

            // Write main payload (bytes)
            buffer.putInt(bytesLen);
            if (bytesLen > 0) {
                buffer.put(bytes);
            }

            // Write changed fields metadata
            buffer.putInt(fieldsLen);
            if (fieldsLen > 0) {
                for (var field : changedFields) {
                    buffer.putInt(field);
                }
            }

            return buffer.array();
        }

        /**
         * Decodes a byte array back into a TypedEntity record
         */
        public static TypedEntity decode(byte[] data) {
            var buffer = ByteBuffer.wrap(data);

            var type = buffer.getLong();
            var isNew = buffer.get() == 1;

            // Extract bytes
            var bytesLen = buffer.getInt();
            var bytes = new byte[bytesLen];
            if (bytesLen > 0) {
                buffer.get(bytes);
            }

            // Extract changedFields
            var fieldsLen = buffer.getInt();
            var changedFields = new int[fieldsLen];
            for (int i = 0; i < fieldsLen; i++) {
                changedFields[i] = buffer.getInt();
            }

            return new TypedEntity(bytes, type, isNew, changedFields);
        }
    }

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
     * Process a single feed entity and return a TypedEntity with the entity's bytes and type
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

        if (existingHash == OffHeapLongTable.EMPTY_VALUE) {
            stateStore.put(hashedId, hashedBytes);
            return new TypedEntity(
                entityBytes,
                scanResult.type(),
                true,
                new int[0]
            );
        }

        if (hashedBytes == existingHash) {
            return null;
        }

        stateStore.put(hashedId, hashedBytes);

        return new TypedEntity(
            entityBytes,
            scanResult.type(),
            false,
            new int[0]
        );
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
                    log.info(
                        "Header hash matches existing hash for feed {}[{}]",
                        feedId,
                        feedUrl
                    );

                    return changedEntities;
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
            "Finished parsing feed {}[{}] {} changed and {} entities total",
            feedId,
            feedUrl,
            SizeFormat.formatNumber(numChanged),
            SizeFormat.formatNumber(numEntities)
        );

        return changedEntities;
    }
}
