package org.example.sirianalyzer.repositories;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Repository;

/**
 * Repository for managing GTFS state data using LMDB.
 */
@AllArgsConstructor
@Repository
public class GtfsStateRepository {

    /** LMDB database handle for GTFS state data. */
    private final Dbi<ByteBuffer> db;

    /** Reusable key buffer for LMDB operations. */
    private final ThreadLocal<ByteBuffer> keyBufThreadLocal =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(256));

    /** Reusable value buffer for LMDB operations. */
    private final ThreadLocal<ByteBuffer> valBufThreadLocal =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(Long.BYTES));

    /**
     * Checks if the hash for the specified key has changed.
     *
     * @param txn   LMDB transaction
     * @param key LMDB key buffer
     * @param h     Hash value to compare with
     * @return True if the hash has changed, false otherwise
     */
    public boolean hasChanged(Txn<ByteBuffer> txn, ByteString key, long h) {
        var keyBuf = prepareKeyBuffer(key);

        var existing = db.get(txn, keyBuf);

        return existing == null || existing.getLong(0) != h;
    }

    /**
     * Stores the given hash for the specified key in LMDB.
     *
     * @param txn    LMDB transaction
     * @param key LMDB key buffer
     * @param h      Hash value to store
     */
    public void putHash(Txn<ByteBuffer> txn, ByteString key, long h) {
        var keyBuf = prepareKeyBuffer(key);
        var valBuf = prepareValueBuffer(h);

        db.put(txn, keyBuf, valBuf);
    }

    /**
     * Ensure the thread-local key buffer has at least the requested capacity.
     *
     * <p>
     * If it's too small, replace it with a larger direct buffer and save it back
     * to the ThreadLocal so future calls reuse the larger buffer.
     * </p>
     */
    private ByteBuffer ensureKeyBufCapacity(int required) {
        var buf = keyBufThreadLocal.get();

        if (buf.capacity() < required) {
            var newCap = Math.max(required, buf.capacity() * 2);
            buf = ByteBuffer.allocateDirect(newCap);
            keyBufThreadLocal.set(buf);
        }

        return buf.clear();
    }

    /**
     * Prepare a direct ByteBuffer containing the given hash value
     *
     * @param h the hash value
     * @return a direct ByteBuffer containing the hash value
     */
    private ByteBuffer prepareValueBuffer(long h) {
        // As long has a fixed length, we can just rewind the buffer
        var valBuf = valBufThreadLocal.get().rewind();

        return valBuf.putLong(h).flip();
    }

    /**
     * Prepare a direct ByteBuffer containing exactly the remaining bytes of the provided key
     *
     * <p>
     * The returned buffer has position=0 and limit=keyLength.
     * </p>
     */
    private ByteBuffer prepareKeyBuffer(ByteString key) {
        var len = key.size();
        var dstBuf = ensureKeyBufCapacity(len);

        key.copyTo(dstBuf);

        return dstBuf.flip();
    }
}