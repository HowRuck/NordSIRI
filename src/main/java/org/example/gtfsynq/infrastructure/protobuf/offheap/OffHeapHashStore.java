package org.example.gtfsynq.infrastructure.protobuf.offheap;

import java.util.concurrent.locks.StampedLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OffHeapHashStore implements AutoCloseable {

    private final OffHeapLongTable binTable = new OffHeapLongTable();

    private final StampedLock lock = new StampedLock();

    public static final long PROHIBITED_WRITE = -1L;
    public static final long[] PROHIBITED_WRITE_ARRAY = new long[] {
        PROHIBITED_WRITE,
        PROHIBITED_WRITE,
        PROHIBITED_WRITE,
    };

    private static final int TTL_MINUTES = 60;
    public volatile int currentMinute;

    public OffHeapHashStore() {
        currentMinute = (int) (System.currentTimeMillis() / 60000);
    }

    public long get(long key) {
        return getAndTryOptimistic(key)[0];
    }

    public long[] getWithCustomSlots(long key) {
        return getAndTryOptimistic(key);
    }

    public long[] getAndTryOptimistic(long key) {
        var optimisticLockStamp = lock.tryOptimisticRead();

        var data = getWithLock(key, optimisticLockStamp);
        if (data[0] != PROHIBITED_WRITE) {
            return data;
        }

        var fullLockStamp = lock.readLock();
        try {
            return getWithLock(key, fullLockStamp);
        } finally {
            lock.unlockRead(fullLockStamp);
        }
    }

    public long[] getWithLock(long key, long stamp) {
        var index = hash(key) & OffHeapLongTable.CAPACITY_MASK;

        for (var i = 0; i < OffHeapLongTable.CAPACITY; i++) {
            var slotKey = binTable.getKey(index);

            if (slotKey == OffHeapLongTable.EMPTY_VALUE) {
                return OffHeapLongTable.EMPTY_VALUE_ARRAY;
            }

            if (slotKey == key) {
                var slotExpiry = binTable.getExpiry(index);
                if (slotExpiry <= currentMinute) {
                    return OffHeapLongTable.EMPTY_VALUE_ARRAY;
                }

                var data = new long[] {
                    binTable.getValue(index),
                    binTable.getCustomSlot1(index),
                    binTable.getCustomSlot2(index),
                };

                // Validate optimistic read lock
                if (!lock.validate(stamp)) {
                    return PROHIBITED_WRITE_ARRAY;
                }

                return data;
            }

            // Move to the next slot in the chain
            index = (index + 1) & OffHeapLongTable.CAPACITY_MASK;
        }

        return OffHeapLongTable.EMPTY_VALUE_ARRAY;
    }

    public void put(long key, long value) {
        if (key == OffHeapLongTable.EMPTY_VALUE) {
            throw new IllegalArgumentException("Key cannot be empty");
        }

        var stamp = lock.writeLock();
        try {
            insert(key, value, currentMinute + TTL_MINUTES, 0, 0);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void put(long key, long value, int customSlot1, int customSlot2) {
        if (key == OffHeapLongTable.EMPTY_VALUE) {
            throw new IllegalArgumentException("Key cannot be empty");
        }

        var stamp = lock.writeLock();
        try {
            insert(
                key,
                value,
                currentMinute + TTL_MINUTES,
                customSlot1,
                customSlot2
            );
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void insert(
        long key,
        long value,
        int expiry,
        int customSlot1,
        int customSlot2
    ) {
        var index = hash(key) & OffHeapLongTable.CAPACITY_MASK;
        long firstAvailableIndex = -1;

        for (var i = 0; i < OffHeapLongTable.CAPACITY; i++) {
            var slotKey = binTable.getKey(index);

            // If we hit an empty slot, the key is definitely not in the map.
            if (slotKey == OffHeapLongTable.EMPTY_VALUE) {
                // If we saw an expired slot earlier, overwrite it to keep elements tightly packed.
                // Otherwise, write to this empty slot.
                if (firstAvailableIndex != -1) {
                    writeSlot(
                        firstAvailableIndex,
                        key,
                        value,
                        expiry,
                        customSlot1,
                        customSlot2
                    );
                } else {
                    writeSlot(
                        index,
                        key,
                        value,
                        expiry,
                        customSlot1,
                        customSlot2
                    );
                }
                return;
            }

            // If we find the exact key, update it in place.
            if (slotKey == key) {
                writeSlot(index, key, value, expiry, customSlot1, customSlot2);
                return;
            }

            // If we pass an expired slot, remember it so we can overwrite it later.
            // We MUST NOT return yet, because the exact key might be further down the chain!
            var slotExpiry = binTable.getExpiry(index);
            if (slotExpiry <= currentMinute && firstAvailableIndex == -1) {
                firstAvailableIndex = index;
            }

            // Move to the next slot in the chain
            index = (index + 1) & OffHeapLongTable.CAPACITY_MASK;
        }

        // If we loop through the whole table and it's full:
        if (firstAvailableIndex != -1) {
            // We can evict the expired element we found
            writeSlot(
                firstAvailableIndex,
                key,
                value,
                expiry,
                customSlot1,
                customSlot2
            );
        } else {
            throw new IllegalStateException(
                "OffHeapHashStore is completely full!"
            );
        }
    }

    private void writeSlot(long index, long k, long v, int e, int c1, int c2) {
        binTable.setKey(index, k);
        binTable.setValue(index, v);
        binTable.setExpiry(index, e);
        binTable.setPsl(index, 0);
        binTable.setCustomSlot1(index, c1);
        binTable.setCustomSlot2(index, c2);
    }

    private static long hash(long key) {
        key = (key ^ (key >>> 30)) * 0xbf58476d1ce4e5b9L;
        key = (key ^ (key >>> 27)) * 0x94d049bb133111ebL;
        return key ^ (key >>> 31);
    }

    @Override
    public void close() {
        binTable.close();
    }

    @Scheduled(fixedRate = 60000)
    public void tickMinute() {
        currentMinute = (int) (System.currentTimeMillis() / 60000);
    }

    @Scheduled(fixedRate = 60000)
    public void printLoadPercentage() {
        var stamp = lock.readLock();
        var occupied = 0;

        try {
            for (var i = 0; i < OffHeapLongTable.CAPACITY; i++) {
                var key = binTable.getKey(i);
                if (
                    key != OffHeapLongTable.EMPTY_VALUE &&
                    binTable.getExpiry(i) > currentMinute
                ) {
                    occupied++;
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }

        var loadPercentage =
            ((double) occupied / OffHeapLongTable.CAPACITY) * 100;
        log.info(
            String.format(
                "[OffHeapHashStore] Load: %.2f%% (%d/%d)",
                loadPercentage,
                occupied,
                OffHeapLongTable.CAPACITY
            )
        );
    }
}
