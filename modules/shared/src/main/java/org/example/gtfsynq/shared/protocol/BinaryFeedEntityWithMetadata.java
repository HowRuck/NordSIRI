package org.example.gtfsynq.shared.protocol;

import java.nio.ByteBuffer;

/**
 * Typed wrapper for a raw GTFS-RT entity payload plus metadata.
 *
 * <p>Layout used by {@link #encode()} and {@link #decode(byte[])}:
 * [int payloadLength][payload bytes][long type][long ts]
 */
public record BinaryFeedEntityWithMetadata(byte[] bytes, long type, long ts) {

    /**
     * Encodes this record into a byte array.
     *
     * @return encoded payload
     */
    public byte[] encode() {
        var payloadLength = bytes != null ? bytes.length : 0;

        var buffer = ByteBuffer.allocate(
            Integer.BYTES + payloadLength + Long.BYTES + Long.BYTES
        );

        buffer.putInt(payloadLength);

        if (payloadLength > 0) {
            buffer.put(bytes);
        }

        buffer.putLong(type);
        buffer.putLong(ts);

        return buffer.array();
    }

    /**
     * Decodes a byte array back into a typed payload wrapper.
     *
     * @param data encoded payload
     * @return decoded record
     */
    public static BinaryFeedEntityWithMetadata decode(byte[] data) {
        if (data == null || data.length < Integer.BYTES + Long.BYTES + Long.BYTES) {
            throw new IllegalArgumentException(
                "Data too short. Expected at least 20 bytes, got " +
                (data == null ? 0 : data.length)
            );
        }

        var buffer = ByteBuffer.wrap(data);
        var payloadLength = buffer.getInt();

        var remainingNeeded = payloadLength + Long.BYTES + Long.BYTES;
        if (buffer.remaining() < remainingNeeded) {
            throw new IllegalArgumentException(
                String.format(
                    "Payload too short. Need %d more bytes, have %d. payloadLength=%d, totalSize=%d",
                    remainingNeeded,
                    buffer.remaining(),
                    payloadLength,
                    data.length
                )
            );
        }

        var bytes = new byte[payloadLength];
        if (payloadLength > 0) {
            buffer.get(bytes);
        }

        var type = buffer.getLong();
        var ts = buffer.getLong();

        return new BinaryFeedEntityWithMetadata(bytes, type, ts);
    }
}
