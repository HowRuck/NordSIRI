package org.example.sirianalyzer.proto;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * An off-heap hash store for {@code long -> long} entries with millisecond TTL support.
 */
@Component
@Slf4j
public class OffHeapHashStore {

    // Must be a power of 2 for bitwise modulus
    private static final long SLOTS = 262_144L;
    private static final long MASK = SLOTS - 1L;

    private static final long SLOT_SIZE = 24L;

    private static final int DUMP_MAGIC = 0x4F484853; // OHHS
    private static final int DUMP_VERSION = 1;

    // 60 minute TTL
    private static final long TTL_MILLIS = 60 * 60 * 1000L;

    public static final long EMPTY_VALUE = -1L;

    private final Arena arena;
    private final MemorySegment segment;
    private final VarHandle keyHandle;
    private final VarHandle valueHandle;
    private final VarHandle expiresAtHandle;

    public OffHeapHashStore() {
        arena = Arena.ofShared();
        segment = arena.allocate(SLOTS * SLOT_SIZE);

        var slotLayout = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("key"),
            ValueLayout.JAVA_LONG.withName("value"),
            ValueLayout.JAVA_LONG.withName("expiresAt")
        );

        var tableLayout = MemoryLayout.sequenceLayout(SLOTS, slotLayout);

        keyHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("key")
        );

        valueHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("value")
        );

        expiresAtHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("expiresAt")
        );
    }

    /**
     * Retrieves a long value from the segment using the given handle and index
     *
     * @param handle The VarHandle to use for retrieving the value
     * @param index The index within the segment to retrieve the value from
     *
     * @return The long value retrieved from the segment
     */
    private long getLong(VarHandle handle, long index) {
        return (long) handle.get(segment, 0L, index);
    }

    /**
     * Sets a long value in the segment using the given handle and index
     *
     * @param handle The VarHandle to use for setting the value
     * @param index The index within the segment to set the value at
     * @param value The long value to set
     */
    private void setLong(VarHandle handle, long index, long value) {
        handle.set(segment, 0L, index, value);
    }

    /**
     * Computes the index within the segment for the given key
     *
     * @param key The key to compute the index for
     *
     * @return The computed index within the segment
     */
    public long computeIndex(long key) {
        return key & MASK;
    }

    /**
     * Retrieves a value from the segment using the given key
     *
     * @param key The key to retrieve the value for
     *
     * @return The value retrieved from the segment, or {@link #EMPTY_VALUE} if not found
     */
    public long get(long key) {
        var startIndex = computeIndex(key);
        var index = startIndex;

        while (true) {
            var existingKey = getLong(keyHandle, index);

            // Found an empty slot, so the key doesn't exist
            if (existingKey == 0L) {
                return EMPTY_VALUE;
            }

            // Found our key
            if (existingKey == key) {
                var expiresAt = getLong(expiresAtHandle, index);
                if (isExpired(expiresAt)) {
                    clearSlot(index);
                    return EMPTY_VALUE;
                }
                return getLong(valueHandle, index);
            }

            index = (index + 1) & MASK;

            if (index == startIndex) {
                log.warn("Hash store is full, key not found: {}", key);
                return EMPTY_VALUE;
            }
        }
    }

    /**
     * Puts a key-value pair into the segment, overwriting any existing value for the key
     *
     * @param key The key to put into the segment
     * @param value The value to associate with the key
     *
     * @return The previous value associated with the key, or {@link #EMPTY_VALUE} if none
     */
    public long put(long key, long value) {
        var startIndex = computeIndex(key);
        var index = startIndex;
        var expiresAt = safeAdd(System.currentTimeMillis(), TTL_MILLIS);

        // Probe for an empty slot or our key (update), or an expired slot
        while (true) {
            var existingKey = getLong(keyHandle, index);

            // Found an empty slot, or slot has our exact key (update), or slot is expired
            if (
                existingKey == 0L ||
                existingKey == key ||
                isExpired(getLong(expiresAtHandle, index))
            ) {
                setLong(keyHandle, index, key);
                setLong(valueHandle, index, value);
                setLong(expiresAtHandle, index, expiresAt);
                return value;
            }

            index = (index + 1) & MASK;

            if (index == startIndex) {
                throw new IllegalStateException(
                    "Hash store is completely full!"
                );
            }
        }
    }

    /**
     * Dumps the contents of the segment to a binary file at the given path
     *
     * @param path The path to write the binary dump to
     *
     * @throws java.io.IOException if an I/O error occurs while writing the file
     */
    public void dumpBinary(Path path) throws java.io.IOException {
        var buffer = ByteBuffer.allocate(
            Integer.BYTES * 2 + Long.BYTES * 3 + (int) (SLOTS * SLOT_SIZE)
        ).order(ByteOrder.LITTLE_ENDIAN);

        var snapshotTime = System.currentTimeMillis();

        buffer.putInt(DUMP_MAGIC);
        buffer.putInt(DUMP_VERSION);
        buffer.putLong(SLOTS);
        buffer.putLong(SLOT_SIZE);
        buffer.putLong(snapshotTime);

        for (var index = 0L; index < SLOTS; index++) {
            buffer.putLong(getLong(keyHandle, index));
            buffer.putLong(getLong(valueHandle, index));
            buffer.putLong(getLong(expiresAtHandle, index));
        }

        buffer.flip();
        Files.write(path, buffer.array());
    }

    /**
     * Restores the contents of the segment from a binary dump file at the given path
     *
     * @param path The path to read the binary dump from
     *
     * @throws java.io.IOException if an I/O error occurs while reading the file
     */
    public void restoreBinary(Path path) throws java.io.IOException {
        var bytes = Files.readAllBytes(path);
        var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        var magic = buffer.getInt();
        var version = buffer.getInt();
        var storedSlots = buffer.getLong();
        var storedSlotSize = buffer.getLong();
        var restoredAt = buffer.getLong();

        if (magic != DUMP_MAGIC) {
            throw new IllegalArgumentException("Invalid dump format");
        }

        if (version != DUMP_VERSION) {
            throw new IllegalArgumentException("Unsupported dump version");
        }

        if (storedSlots != SLOTS || storedSlotSize != SLOT_SIZE) {
            throw new IllegalArgumentException("Incompatible table format");
        }

        var now = System.currentTimeMillis();

        for (var index = 0L; index < SLOTS; index++) {
            var key = buffer.getLong();
            var value = buffer.getLong();
            var expiresAt = buffer.getLong();

            if (key == 0L && value == 0L && expiresAt == 0L) {
                continue;
            }

            var remainingTtl = expiresAt - restoredAt;
            if (remainingTtl <= 0) {
                continue;
            }

            expiresAt = safeAdd(now, remainingTtl);

            setLong(keyHandle, index, key);
            setLong(valueHandle, index, value);
            setLong(expiresAtHandle, index, expiresAt);
        }
    }

    /**
     * Clears all key-value pairs from the segment, leaving it empty
     */
    public void clear() {
        segment.fill((byte) 0);
    }

    /**
     * Returns {@code true} if the given expiration time is in the past, {@code false} otherwise
     *
     * @param expiresAt The expiration time to check
     *
     * @return {@code true} if the expiration time is in the past, {@code false} otherwise
     */
    private boolean isExpired(long expiresAt) {
        return expiresAt <= System.currentTimeMillis();
    }

    /**
     * Clears the slot at the given index, setting all fields to 0
     *
     * @param index The index of the slot to clear
     */
    private void clearSlot(long index) {
        setLong(keyHandle, index, 0L);
        setLong(valueHandle, index, 0L);
        setLong(expiresAtHandle, index, 0L);
    }

    /**
     * Safely adds two longs, throwing an exception if the result overflows
     *
     * @param left  The left operand
     * @param right The right operand
     *
     * @return The sum of the two operands
     */
    private static long safeAdd(long left, long right) {
        return Math.addExact(left, right);
    }
}
