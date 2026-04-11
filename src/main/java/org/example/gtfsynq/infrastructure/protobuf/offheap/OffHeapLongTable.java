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
public final class OffHeapLongTable {

    private static final long SLOT_SIZE_BYTES = Long.BYTES * 3L;

    private final MemorySegment segment;
    private final VarHandle keyHandle;
    private final VarHandle valueHandle;
    private final VarHandle expiresAtHandle;

    public OffHeapLongTable(Arena arena, long slotCount) {
        var slotLayout = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("key"),
            ValueLayout.JAVA_LONG.withName("value"),
            ValueLayout.JAVA_LONG.withName("expiresAt")
        );

        var tableLayout = MemoryLayout.sequenceLayout(slotCount, slotLayout);

        segment = arena.allocate(slotCount * SLOT_SIZE_BYTES);

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
     * Returns the underlying memory segment for this table
     */
    public MemorySegment segment() {
        return segment;
    }

    /**
     * Returns the key at the given index
     */
    public long getKey(long index) {
        return getLong(keyHandle, index);
    }

    /**
     * Returns the value at the given index
     */
    public long getValue(long index) {
        return getLong(valueHandle, index);
    }

    /**
     * Returns the expiration time at the given index
     */
    public long getExpiresAt(long index) {
        return getLong(expiresAtHandle, index);
    }

    /**
     * Sets the key at the given index
     */
    public void setKey(long index, long value) {
        setLong(keyHandle, index, value);
    }

    /**
     * Sets the value at the given index
     */
    public void setValue(long index, long value) {
        setLong(valueHandle, index, value);
    }

    /**
     * Sets the expiration time at the given index
     */
    public void setExpiresAt(long index, long value) {
        setLong(expiresAtHandle, index, value);
    }

    /**
     * Clears the slot at the given index, setting all fields to 0
     */
    public void clearSlot(long index) {
        setKey(index, 0L);
        setValue(index, 0L);
        setExpiresAt(index, 0L);
    }

    /**
     * Clears all slots in the table, setting all fields to 0
     */
    public void clear() {
        segment.fill((byte) 0);
    }

    /**
     * Helper method to get a long value from the segment using a VarHandle
     */
    private long getLong(VarHandle handle, long index) {
        return (long) handle.get(segment, 0L, index);
    }

    /**
     * Helper method to set a long value in the segment using a VarHandle
     */
    private void setLong(VarHandle handle, long index, long value) {
        handle.set(segment, 0L, index, value);
    }
}
