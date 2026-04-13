package org.example.gtfsynq.infrastructure.protobuf.offheap;

import java.util.concurrent.locks.StampedLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OffHeapHashStore implements AutoCloseable {

    private OffHeapLongTable binTable = new OffHeapLongTable();

    private final StampedLock lock = new StampedLock();
    public static final long PROHIBITED_WRITE = -1L;

    private final int TTL_MINUTES = 60;
    public volatile int currentMinute;

    public OffHeapHashStore() {
        currentMinute = (int) (System.currentTimeMillis() / 60000);
    }

    public long get(long key) {
        var optimisticLockStamp = lock.tryOptimisticRead();

        var val = getWithLock(key, optimisticLockStamp);

        if (val != PROHIBITED_WRITE) return val;

        var fullLockStamp = lock.readLock();

        try {
            val = getWithLock(key, fullLockStamp);

            return val;
        } finally {
            lock.unlockRead(fullLockStamp);
        }
    }

    public long getWithLock(long key, long stamp) {
        var homeIndex = key & OffHeapLongTable.CAPACITY_MASK;

        for (var psl = 0; psl < OffHeapLongTable.CAPACITY; psl++) {
            var index = (homeIndex + psl) & OffHeapLongTable.CAPACITY_MASK;

            var slotKey = binTable.getKey(index);

            if (
                slotKey == OffHeapLongTable.EMPTY_VALUE
            ) return OffHeapLongTable.EMPTY_VALUE;

            var slotPsl = binTable.getPsl(index);
            if (psl > slotPsl) return OffHeapLongTable.EMPTY_VALUE;

            if (slotKey == key) {
                var val = binTable.getValue(index);
                var exp = binTable.getExpiry(index);

                if (!lock.validate(stamp)) {
                    return PROHIBITED_WRITE;
                }
                return (exp > currentMinute)
                    ? val
                    : OffHeapLongTable.EMPTY_VALUE;
            }
        }

        return OffHeapLongTable.EMPTY_VALUE;
    }

    public void put(long key, long value) {
        if (
            key == OffHeapLongTable.EMPTY_VALUE
        ) throw new IllegalArgumentException("Key cannot be 0");

        var stamp = lock.writeLock();

        try {
            insert(key, value, currentMinute + TTL_MINUTES);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void insert(long key, long value, int expiry) {
        var homeIndex = key & OffHeapLongTable.CAPACITY_MASK;
        var curKey = key;
        var curVal = value;
        var curExp = expiry;

        var curPsl = 0;

        for (var i = 0; i < OffHeapLongTable.CAPACITY; i++) {
            var index = (homeIndex + i) & OffHeapLongTable.CAPACITY_MASK;
            var slotKey = binTable.getKey(index);

            if (slotKey == 0 || binTable.getExpiry(index) <= currentMinute) {
                writeSlot(index, curKey, curVal, curExp, curPsl);
                return;
            }

            var slotPsl = binTable.getPsl(index);
            if (curPsl > slotPsl) {
                var tmpKey = slotKey;
                var tmpVal = binTable.getValue(index);
                var tmpExp = binTable.getExpiry(index);

                writeSlot(index, curKey, curVal, curExp, curPsl);

                curKey = tmpKey;
                curVal = tmpVal;
                curExp = tmpExp;
                curPsl = slotPsl;
            }
            curPsl++;
        }
    }

    private void writeSlot(long index, long k, long v, int e, int p) {
        binTable.setKey(index, k);
        binTable.setValue(index, v);
        binTable.setExpiry(index, e);
        binTable.setPsl(index, p);
    }

    @Override
    public void close() {
        // Need to expose Arena in OffHeapLongTable
        binTable.close();
    }

    @Scheduled(fixedRate = 60000)
    public void tickMinute() {
        var currentMinute = (int) (System.currentTimeMillis() / 60000);
        this.currentMinute = currentMinute;
    }
}
