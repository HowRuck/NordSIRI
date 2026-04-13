package org.example.gtfsynq.infrastructure.protobuf.offheap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Shared off-heap memory helper for a packed {@code long -> long -> long} table row layout.
 *
 * <p>This class centralizes the low-level foreign-memory setup so the hash store can focus on
 * behavior rather than repeated layout and access boilerplate.
 */
public final class OffHeapLongTable implements AutoCloseable {

    public static final int CAPACITY = 262144;
    public static final int CAPACITY_MASK = CAPACITY - 1;
    public static final int SLOT_SIZE = 32; // 8 (key) + 8 (val) + 4 (expiry) + 4 (psl) = 24, but aligned to 32

    public static final long EMPTY_VALUE = 0L;

    private final Arena arena;
    private final MemorySegment segment;

    // VarHandles for each field
    private final VarHandle keyHandle;
    private final VarHandle valueHandle;
    private final VarHandle expiryHandle;
    private final VarHandle pslHandle;

    public OffHeapLongTable() {
        arena = Arena.ofShared();
        segment = this.arena.allocate(CAPACITY * SLOT_SIZE, 64);

        // Define the slot layout
        var slotLayout = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("key"),
            ValueLayout.JAVA_LONG.withName("value"),
            ValueLayout.JAVA_INT.withName("expiry"),
            ValueLayout.JAVA_INT.withName("psl")
        );

        // Create a sequence layout for all slots
        var tableLayout = MemoryLayout.sequenceLayout(CAPACITY, slotLayout);

        // Create VarHandles for each field
        keyHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("key")
        );
        valueHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("value")
        );
        expiryHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("expiry")
        );
        pslHandle = tableLayout.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("psl")
        );
    }

    // Helper methods for readability
    public long getKey(long index) {
        return (long) keyHandle.get(segment, 0L, index);
    }

    public long getValue(long index) {
        return (long) valueHandle.get(segment, 0L, index);
    }

    public int getExpiry(long index) {
        return (int) expiryHandle.get(segment, 0L, index);
    }

    public int getPsl(long index) {
        return (int) pslHandle.get(segment, 0L, index);
    }

    public void setKey(long index, long key) {
        keyHandle.set(segment, 0L, index, key);
    }

    public void setValue(long index, long value) {
        valueHandle.set(segment, 0L, index, value);
    }

    public void setExpiry(long index, int expiry) {
        expiryHandle.set(segment, 0L, index, expiry);
    }

    public void setPsl(long index, int psl) {
        pslHandle.set(segment, 0L, index, psl);
    }

    @Override
    public void close() {
        arena.close();
    }
}
