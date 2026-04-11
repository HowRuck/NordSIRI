package org.example.gtfsynq.infrastructure.protobuf;

import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapLongTable;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapMinuteWheel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * An off-heap hash store for {@code long -> long} entries with minute-level TTL support.
 *
 * <p>The store is split into two parts:
 * <ul>
 *   <li>{@link OffHeapLongTable} for packed off-heap table storage</li>
 *   <li>{@link OffHeapMinuteWheel} for minute-based expiration scheduling</li>
 * </ul>
 *
 * <p>TTL precision is minute-level, which keeps the implementation simpler and easier to maintain
 * than a millisecond timing wheel.
 */
@Component
@Slf4j
public class OffHeapHashStore {

    private static final long SLOT_COUNT = 262_144L;
    private static final long SLOT_MASK = SLOT_COUNT - 1L;
    private static final long SLOT_SIZE_BYTES = 24L;

    private static final int DUMP_MAGIC = 0x4F484853; // OHHS
    private static final int DUMP_VERSION = 7;

    private static final long TTL_MILLIS = 60 * 60 * 1000L;
    private static final long MINUTE_MILLIS = 60_000L;
    private static final int WHEEL_BUCKET_COUNT = (int) (TTL_MILLIS /
        MINUTE_MILLIS);

    public static final long EMPTY_VALUE = -1L;

    private final Arena arena;
    private final OffHeapLongTable table;
    private final OffHeapMinuteWheel minuteWheel;

    private final Object lock = new Object();

    private volatile long currentTimeMs;

    public OffHeapHashStore() {
        arena = Arena.ofShared();
        table = new OffHeapLongTable(arena, SLOT_COUNT);
        minuteWheel = new OffHeapMinuteWheel(
            arena,
            WHEEL_BUCKET_COUNT,
            SLOT_COUNT
        );

        currentTimeMs = System.currentTimeMillis();
    }

    public long computeIndex(long key) {
        return key & SLOT_MASK;
    }

    /**
     * Returns the value for the given key, or {@link #EMPTY_VALUE} if the key is not found or has expired.
     */
    public long get(long key) {
        synchronized (lock) {
            var index = findKeyIndexLocked(key);
            if (index < 0) {
                return EMPTY_VALUE;
            }

            var expiresAt = table.getExpiresAt(index);
            if (isExpired(expiresAt)) {
                table.clearSlot(index);
                return EMPTY_VALUE;
            }

            return table.getValue(index);
        }
    }

    /**
     * Puts a key-value pair into the table, overwriting any existing value for the key.
     */
    public long put(long key, long value) {
        synchronized (lock) {
            var index = computeIndex(key);
            var startIndex = index;
            var expiresAt = safeAdd(currentTimeMs, TTL_MILLIS);
            var expiresAtMinute = toMinute(expiresAt);

            while (true) {
                var existingKey = table.getKey(index);

                if (existingKey == 0L) {
                    table.setKey(index, key);
                    table.setValue(index, value);
                    table.setExpiresAt(index, expiresAt);
                    minuteWheel.enqueue(key, expiresAtMinute);
                    return value;
                }

                if (existingKey == key) {
                    table.setValue(index, value);
                    table.setExpiresAt(index, expiresAt);
                    minuteWheel.enqueue(key, expiresAtMinute);
                    return value;
                }

                index = (index + 1) & SLOT_MASK;

                if (index == startIndex) {
                    throw new IllegalStateException(
                        "Hash store is completely full!"
                    );
                }
            }
        }
    }

    /**
     * Removes a key from the store.
     */
    public long remove(long key) {
        synchronized (lock) {
            return removeWithoutCleanupLocked(key);
        }
    }

    /**
     * Clears all key-value pairs from the table, leaving it empty.
     */
    public void clear() {
        synchronized (lock) {
            table.clear();
            minuteWheel.clear();
            currentTimeMs = System.currentTimeMillis();
        }
    }

    /**
     * Dumps the contents of the table to a binary file at the given path.
     */
    public void dumpBinary(Path path) throws java.io.IOException {
        synchronized (lock) {
            var buffer = ByteBuffer.allocate(
                Integer.BYTES * 2 +
                    Long.BYTES * 3 +
                    (int) (SLOT_COUNT * SLOT_SIZE_BYTES)
            ).order(ByteOrder.LITTLE_ENDIAN);

            buffer.putInt(DUMP_MAGIC);
            buffer.putInt(DUMP_VERSION);
            buffer.putLong(SLOT_COUNT);
            buffer.putLong(SLOT_SIZE_BYTES);
            buffer.putLong(currentTimeMs);

            for (var index = 0L; index < SLOT_COUNT; index++) {
                buffer.putLong(table.getKey(index));
                buffer.putLong(table.getValue(index));
                buffer.putLong(table.getExpiresAt(index));
            }

            buffer.flip();
            Files.write(path, buffer.array());
        }
    }

    /**
     * Restores the contents of the table from a binary dump file at the given path.
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

        if (storedSlots != SLOT_COUNT || storedSlotSize != SLOT_SIZE_BYTES) {
            throw new IllegalArgumentException("Incompatible table format");
        }

        synchronized (lock) {
            clear();

            var now = System.currentTimeMillis();
            currentTimeMs = now;

            for (var index = 0L; index < SLOT_COUNT; index++) {
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

                var restoredExpiresAt = safeAdd(now, remainingTtl);

                table.setKey(index, key);
                table.setValue(index, value);
                table.setExpiresAt(index, restoredExpiresAt);
                minuteWheel.enqueue(key, toMinute(restoredExpiresAt));
            }
        }
    }

    /**
     * Returns {@code true} if the given expiration time is in the past, {@code false} otherwise.
     */
    private boolean isExpired(long expiresAt) {
        return expiresAt <= currentTimeMs;
    }

    /**
     * Safely adds two longs, throwing an exception if the result overflows.
     */
    private static long safeAdd(long left, long right) {
        return Math.addExact(left, right);
    }

    /**
     * Converts a wall-clock time to a minute number.
     */
    private static long toMinute(long timeMs) {
        return Math.floorDiv(timeMs, MINUTE_MILLIS);
    }

    /**
     * Advances the minute wheel and evicts expired entries.
     */
    private void cleanupExpiredEntriesLocked(long now) {
        currentTimeMs = now;
        minuteWheel.drainMinute(
            toMinute(now),
            this::expireKeyIfStillExpiredLocked
        );
    }

    private void expireKeyIfStillExpiredLocked(long key, long expiresAtMinute) {
        var index = findKeyIndexLocked(key);
        if (index < 0) {
            return;
        }

        var storedExpiresAt = table.getExpiresAt(index);
        if (
            storedExpiresAt <= currentTimeMs &&
            toMinute(storedExpiresAt) == expiresAtMinute
        ) {
            table.clearSlot(index);
        }
    }

    private long removeWithoutCleanupLocked(long key) {
        var index = findKeyIndexLocked(key);
        if (index < 0) {
            return EMPTY_VALUE;
        }

        var existingValue = table.getValue(index);
        table.clearSlot(index);
        return existingValue;
    }

    private long findKeyIndexLocked(long key) {
        var index = computeIndex(key);
        var startIndex = index;

        while (true) {
            var existingKey = table.getKey(index);

            if (existingKey == 0L) {
                return -1L;
            }

            if (existingKey == key) {
                return index;
            }

            index = (index + 1) & SLOT_MASK;

            if (index == startIndex) {
                return -1L;
            }
        }
    }

    @Scheduled(fixedRate = 60_000)
    public void updateCurrentMinute() {
        synchronized (lock) {
            cleanupExpiredEntriesLocked(System.currentTimeMillis());
        }
    }
}
