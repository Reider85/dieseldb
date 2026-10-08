package diesel.wal;

import java.util.concurrent.CompletableFuture;

/**
 * Write request for WAL entries (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p>Package-private: used by WALQueue and WALWriter.
 */
public record WALWriteRequest(
    long txid,
    WALOpcode op,
    byte[] before,
    byte[] after,
    CompletableFuture<WALEntry> future,
    long enqueueNanos,
    boolean isFlushBarrier
) {

    /**
     * Creates a regular WAL write request.
     */
    public WALWriteRequest(long txid, WALOpcode op, byte[] before, byte[] after, 
                         CompletableFuture<WALEntry> future, long enqueueNanos) {
        this(txid, op, before, after, future, enqueueNanos, false);
    }

    /**
     * Creates a flush barrier request.
     */
    @SuppressWarnings("unchecked")
    public static WALWriteRequest createFlushBarrier(CompletableFuture<Void> future) {
        return new WALWriteRequest(0, null, null, null, 
            (CompletableFuture<WALEntry>) (CompletableFuture<?>) future, 0, true);
    }
}