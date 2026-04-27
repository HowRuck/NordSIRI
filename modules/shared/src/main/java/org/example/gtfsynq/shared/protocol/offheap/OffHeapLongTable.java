package org.example.gtfsynq.shared.protocol.offheap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.example.gtfsynq.shared.persistence.OffHeapFileScribe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Shared off-heap memory helper for a packed {@code long -> long -> long} table row layout.
 *
 * <p>This class centralizes the low-level foreign-memory setup so the hash store can focus on
 * behavior rather than repeated layout and access boilerplate.
 */
@Component
public final class OffHeapLongTable implements AutoCloseable {

    public static final int CAPACITY = 4_194_304;
    public static final int CAPACITY_MASK = CAPACITY - 1;

    private final OffHeapFileScribe scribe;

    /**
     * Row size in bytes.
     *
     * <p>Layout:
     * <ul>
     *   <li>key: 8 bytes</li>
     *   <li>value: 8 bytes</li>
     *   <li>expiry: 4 bytes</li>
     *   <li>psl: 4 bytes</li>
     *   <li>customSlot1: 4 bytes</li>
     *   <li>customSlot2: 4 bytes</li>
     * </ul>
     *
     * <p>Total: 32 bytes.
     */
    public static final int SLOT_SIZE = 32;

    private static final int KEY_OFFSET = 0;
    private static final int VALUE_OFFSET = KEY_OFFSET + Long.BYTES;
    private static final int EXPIRY_OFFSET = VALUE_OFFSET + Long.BYTES;
    private static final int PSL_OFFSET = EXPIRY_OFFSET + Integer.BYTES;
    private static final int CUSTOM_SLOT1_OFFSET = PSL_OFFSET + Integer.BYTES;
    private static final int CUSTOM_SLOT2_OFFSET =
        CUSTOM_SLOT1_OFFSET + Integer.BYTES;

    public static final long EMPTY_VALUE = 0L;
    public static final long[] EMPTY_VALUE_ARRAY = new long[] {
        EMPTY_VALUE,
        EMPTY_VALUE,
        EMPTY_VALUE,
    };

    private final Arena arena;
    private final MemorySegment segment;

    @Autowired
    public OffHeapLongTable(OffHeapFileScribe scribe) {
        arena = Arena.ofShared();
        segment = this.arena.allocate((long) CAPACITY * SLOT_SIZE, 64);
        this.scribe = scribe;

        scribe.load(this);
    }

    public long getKey(long index) {
        return segment.get(
            ValueLayout.JAVA_LONG,
            slotOffset(index) + KEY_OFFSET
        );
    }

    public long getValue(long index) {
        return segment.get(
            ValueLayout.JAVA_LONG,
            slotOffset(index) + VALUE_OFFSET
        );
    }

    public int getExpiry(long index) {
        return segment.get(
            ValueLayout.JAVA_INT,
            slotOffset(index) + EXPIRY_OFFSET
        );
    }

    public int getPsl(long index) {
        return segment.get(
            ValueLayout.JAVA_INT,
            slotOffset(index) + PSL_OFFSET
        );
    }

    public int getCustomSlot1(long index) {
        return segment.get(
            ValueLayout.JAVA_INT,
            slotOffset(index) + CUSTOM_SLOT1_OFFSET
        );
    }

    public int getCustomSlot2(long index) {
        return segment.get(
            ValueLayout.JAVA_INT,
            slotOffset(index) + CUSTOM_SLOT2_OFFSET
        );
    }

    public void setKey(long index, long key) {
        segment.set(ValueLayout.JAVA_LONG, slotOffset(index) + KEY_OFFSET, key);
    }

    public void setValue(long index, long value) {
        segment.set(
            ValueLayout.JAVA_LONG,
            slotOffset(index) + VALUE_OFFSET,
            value
        );
    }

    public void setExpiry(long index, int expiry) {
        segment.set(
            ValueLayout.JAVA_INT,
            slotOffset(index) + EXPIRY_OFFSET,
            expiry
        );
    }

    public void setPsl(long index, int psl) {
        segment.set(ValueLayout.JAVA_INT, slotOffset(index) + PSL_OFFSET, psl);
    }

    public void setCustomSlot1(long index, int customSlot1) {
        segment.set(
            ValueLayout.JAVA_INT,
            slotOffset(index) + CUSTOM_SLOT1_OFFSET,
            customSlot1
        );
    }

    public void setCustomSlot2(long index, int customSlot2) {
        segment.set(
            ValueLayout.JAVA_INT,
            slotOffset(index) + CUSTOM_SLOT2_OFFSET,
            customSlot2
        );
    }

    private long slotOffset(long index) {
        return index * SLOT_SIZE;
    }

    @Override
    public void close() {
        arena.close();
    }

    public MemorySegment getSegment() {
        return segment;
    }

    public long byteSize() {
        return segment.byteSize();
    }

    @EventListener(ContextRefreshedEvent.class)
    public void onStartup() {
        scribe.load(this);
    }

    @Scheduled(fixedRate = 60_000)
    public void backup() {
        scribe.dump(this);
    }
}
