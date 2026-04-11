package org.example.gtfsynq.infrastructure.protobuf.offheap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Off-heap minute-based expiration wheel.
 *
 * <p>This wheel stores one primitive node per scheduled expiration and groups entries by the
 * minute in which they should expire. It is designed for coarse TTL management where minute-level
 * precision is sufficient.
 */
public final class OffHeapMinuteWheel {

    public static final long NO_NEXT = -1L;

    private static final long NODE_SIZE_BYTES = Long.BYTES * 3L;

    private final MemorySegment nodes;
    private final VarHandle keyHandle;
    private final VarHandle expiresAtMinuteHandle;
    private final VarHandle nextHandle;

    private final long[] bucketHeads;
    private final long[] bucketTails;

    private final int bucketCount;
    private final long nodeCount;

    private long nextNodeIndex;

    public OffHeapMinuteWheel(Arena arena, int bucketCount, long nodeCount) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive");
        }

        this.bucketCount = bucketCount;
        this.nodeCount = nodeCount;
        this.nodes = arena.allocate(nodeCount * NODE_SIZE_BYTES);

        var nodeLayout = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("key"),
            ValueLayout.JAVA_LONG.withName("expiresAtMinute"),
            ValueLayout.JAVA_LONG.withName("next")
        );

        var nodeTable = MemoryLayout.sequenceLayout(nodeCount, nodeLayout);

        keyHandle = nodeTable.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("key")
        );

        expiresAtMinuteHandle = nodeTable.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("expiresAtMinute")
        );

        nextHandle = nodeTable.varHandle(
            MemoryLayout.PathElement.sequenceElement(),
            MemoryLayout.PathElement.groupElement("next")
        );

        bucketHeads = new long[bucketCount];
        bucketTails = new long[bucketCount];
        clear();
    }

    /*
     * Clears all entries from the wheel, resetting the bucket heads and tails.
     */
    public void clear() {
        nodes.fill((byte) 0);
        for (var i = 0; i < bucketCount; i++) {
            bucketHeads[i] = NO_NEXT;
            bucketTails[i] = NO_NEXT;
        }
        nextNodeIndex = 0L;
    }

    /*
     * Enqueues a key with the given expiration minute
     */
    public void enqueue(long key, long expiresAtMinute) {
        var bucketIndex = bucketIndex(expiresAtMinute);
        var nodeIndex = allocateNodeIndex();

        setLong(keyHandle, nodeIndex, key);
        setLong(expiresAtMinuteHandle, nodeIndex, expiresAtMinute);
        setLong(nextHandle, nodeIndex, NO_NEXT);

        // Insert the node into the bucket, either at the head or after the tail
        if (bucketHeads[bucketIndex] == NO_NEXT) {
            bucketHeads[bucketIndex] = nodeIndex;
            bucketTails[bucketIndex] = nodeIndex;
        } else {
            var tail = bucketTails[bucketIndex];
            setLong(nextHandle, tail, nodeIndex);
            bucketTails[bucketIndex] = nodeIndex;
        }
    }

    /*
     * Drains all keys that have expired at or before the given minute, invoking the consumer for each.
     */
    public void drainMinute(long minute, ExpirationConsumer consumer) {
        var bucketIndex = bucketIndex(minute);

        // Clear the bucket, preserving the head and tail for re-enqueueing non-expired keys
        var head = bucketHeads[bucketIndex];
        bucketHeads[bucketIndex] = NO_NEXT;
        bucketTails[bucketIndex] = NO_NEXT;

        // Iterate through the bucket, consuming expired keys and re-enqueueing non-expired ones
        while (head != NO_NEXT) {
            var next = getLong(nextHandle, head);
            var key = getLong(keyHandle, head);
            var expiresAtMinute = getLong(expiresAtMinuteHandle, head);

            if (expiresAtMinute <= minute) {
                consumer.accept(key, expiresAtMinute);
            } else {
                enqueue(key, expiresAtMinute);
            }

            head = next;
        }
    }

    /*
     * Allocates a node index for a new entry, wrapping around if necessary.
     */
    private long allocateNodeIndex() {
        var node = nextNodeIndex;
        // Wrap around to the beginning if we've exhausted all nodes
        if (node >= nodeCount) {
            node = 0L;
            nextNodeIndex = 1L;
            return node;
        }

        nextNodeIndex = node + 1L;
        return node;
    }

    /*
     * Computes the bucket index for the given minute.
     */
    private int bucketIndex(long minute) {
        return (int) Math.floorMod(minute, bucketCount);
    }

    /*
     * Reads a long value from the node at the given index.
     */
    private long getLong(VarHandle handle, long index) {
        return (long) handle.get(nodes, 0L, index);
    }

    /*
     * Writes a long value to the node at the given index.
     */
    private void setLong(VarHandle handle, long index, long value) {
        handle.set(nodes, 0L, index, value);
    }

    /*
     * Functional interface for consuming expired keys.
     */
    @FunctionalInterface
    public interface ExpirationConsumer {
        /*
         * Called for each expired key.
         */
        void accept(long key, long expiresAtMinute);
    }
}
