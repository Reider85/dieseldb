package diesel.replication;

import diesel.recovery.PitrSink;
import diesel.wal.CommitPayload;
import diesel.wal.DmlPayload;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Applies streamed WAL entries to a hot-standby database (prompt4 #58,
 * R3-016 step 1/5).
 *
 * <p><b>Buffered-until-commit.</b> DieselDB persists row files at COMMIT time
 * and hides in-flight rows only through MVCC metadata, which does not survive
 * a network hop. The applier therefore buffers the decoded DML of each
 * in-flight transaction and, once that transaction's {@link WALOpcode#COMMIT}
 * record arrives, replays the buffer through the value-guarded
 * {@link PitrSink} forward-apply operations and finalizes with
 * {@link PitrSink#onCommit}. {@link WALOpcode#ABORT} discards the buffer and
 * registers the abort. Readers on the standby never observe the physical rows
 * of a transaction that has not committed — the hot-standby visibility
 * contract.
 *
 * <p><b>Idempotence.</b> Two guards make re-delivery after a reconnect safe:
 * <ul>
 *   <li>{@code highestSeenLsn} — the high-water mark of fully processed
 *       entries; a re-delivered entry at or below it is dropped (its effects
 *       are either buffered in {@code pending} or already applied).</li>
 *   <li>The sink's value guards ({@code DatabasePitrSink}) — a re-applied
 *       INSERT whose row already holds the same values, or an UPDATE already
 *       at its after-image, is a no-op.</li>
 * </ul>
 * If the sink throws mid-transaction, the buffer is restored and the water
 * mark is left behind, so the reconnect replay retries the whole COMMIT —
 * the value guards make the already-done part a no-op.
 *
 * <p><b>Index remap.</b> Like {@code PointInTimeRecovery}, the applier keeps a
 * per-table master-index → standby-index map built from {@code applyInsert}
 * return values and translates later UPDATE targets and COMMIT payload row
 * indexes through it (base-backup row counts may skew the indexes).
 *
 * <p><b>Applied LSN (resume point).</b> {@link #getAppliedLsn()} never
 * advances past the first entry of an in-flight transaction, so a
 * reconnect from {@code appliedLsn + 1} re-delivers exactly the unresolved
 * tail; already-processed overlap is dropped by the water-mark guard.
 */
public final class StreamingApplier implements WalApplier {

    private static final Logger LOGGER = Logger.getLogger(StreamingApplier.class.getName());

    private final PitrSink sink;

    /** txid → decoded DML entries of the still-uncommitted transaction, in LSN order. */
    private final Map<Long, List<WALEntry>> pending = new HashMap<>();

    /** Per-table map: master row index → actual standby row index. */
    private final Map<String, Map<Integer, Integer>> remap = new HashMap<>();

    /** Highest LSN whose processing completed (see class javadoc). */
    private long highestSeenLsn;

    /** Highest LSN fully resolved with no in-flight transaction below it. */
    private long appliedLsn;

    /** Diagnostics: entries applied / discarded / skipped by guards. */
    private long appliedInserts;
    private long appliedUpdates;
    private long abortedTransactions;
    private long skippedEntries;

    /**
     * Creates an applier over the given sink.
     *
     * @param sink the standby's forward-apply sink (never null)
     */
    public StreamingApplier(PitrSink sink) {
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * Seeds the water marks after a base backup or a marker-based resume:
     * every entry at or below {@code lsn} is treated as already processed, so
     * the stream may safely start at {@code lsn + 1}.
     *
     * @param lsn the last LSN covered by the local state (0 = start from 1)
     */
    public synchronized void setInitialLsn(long lsn) {
        this.highestSeenLsn = lsn;
        this.appliedLsn = lsn;
    }

    @Override
    public synchronized void apply(WALEntry entry) throws IOException {
        Objects.requireNonNull(entry, "entry");
        long lsn = entry.getLsn();
        if (lsn <= highestSeenLsn) {
            // Re-delivered overlap after a reconnect: already buffered or
            // applied. Dropping it keeps the replay idempotent.
            skippedEntries++;
            return;
        }

        WALOpcode op = entry.getOp();
        long txid = entry.getTxid();
        switch (op) {
            case INSERT, UPDATE, DELETE -> {
                pending.computeIfAbsent(txid, k -> new ArrayList<>(2)).add(entry);
                highestSeenLsn = lsn;
                advanceAppliedLsn(); // still blocked by this transaction's first entry
            }
            case COMMIT -> {
                List<WALEntry> buffered = pending.remove(txid);
                try {
                    if (buffered != null) {
                        for (WALEntry dml : buffered) {
                            applyDml(txid, dml);
                        }
                    }
                    CommitPayload payload = decodeCommit(entry, lsn);
                    if (payload != null) {
                        sink.onCommit(remapPayload(payload));
                    }
                } catch (IOException | RuntimeException e) {
                    // Restore the buffer and leave the water mark behind: the
                    // reconnect replay retries this COMMIT, and the sink's
                    // value guards make the already-applied part a no-op.
                    if (buffered != null && !buffered.isEmpty()) {
                        pending.put(txid, buffered);
                    }
                    throw e;
                }
                highestSeenLsn = lsn;
                advanceAppliedLsn();
            }
            case ABORT -> {
                List<WALEntry> discarded = pending.remove(txid);
                if (discarded != null) {
                    skippedEntries += discarded.size();
                }
                sink.onAbort(txid);
                abortedTransactions++;
                highestSeenLsn = lsn;
                advanceAppliedLsn();
            }
            default -> {
                // BEGIN/CHECKPOINT/PAGE_IMAGE/SAVEPOINT/RELEASE/TRUNCATE:
                // no logical effect on the standby's row store.
                highestSeenLsn = lsn;
                advanceAppliedLsn();
            }
        }
    }

    @Override
    public synchronized long getAppliedLsn() {
        return appliedLsn;
    }

    /** Returns the number of buffered DML entries replayed as INSERT. */
    public synchronized long getAppliedInserts() {
        return appliedInserts;
    }

    /** Returns the number of buffered DML entries replayed as UPDATE. */
    public synchronized long getAppliedUpdates() {
        return appliedUpdates;
    }

    /** Returns the number of aborted transactions whose buffer was discarded. */
    public synchronized long getAbortedTransactions() {
        return abortedTransactions;
    }

    /** Returns the number of entries dropped by the reconnect overlap guard. */
    public synchronized long getSkippedEntries() {
        return skippedEntries;
    }

    /**
     * Returns the number of transactions whose DML is buffered but unresolved.
     * Zero means everything received so far is fully applied.
     */
    public synchronized int getInFlightTransactions() {
        return pending.size();
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private void applyDml(long txid, WALEntry entry) throws IOException {
        WALOpcode op = entry.getOp();
        switch (op) {
            case INSERT -> {
                DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn(), op);
                if (after == null) {
                    return;
                }
                int actual = sink.applyInsert(txid, after.getTableName(),
                        resolve(after.getTableName(), after.getRowIndex()), after.getValues());
                if (actual >= 0) {
                    remap.computeIfAbsent(after.getTableName(), k -> new HashMap<>())
                            .put(after.getRowIndex(), actual);
                    appliedInserts++;
                } else {
                    skippedEntries++;
                }
            }
            case UPDATE -> {
                DmlPayload before = decodeDml(entry.getBeforeImage(), entry.getLsn(), op);
                DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn(), op);
                if (before == null || after == null) {
                    return;
                }
                int applied = sink.applyUpdate(txid, after.getTableName(),
                        resolve(after.getTableName(), after.getRowIndex()),
                        before.getValues(), after.getValues());
                if (applied > 0) {
                    appliedUpdates++;
                } else {
                    skippedEntries++;
                }
            }
            case DELETE -> {
                // Physical tombstoning happens inside onCommit from the
                // COMMIT payload's deletedRows (same split as PITR's apply
                // pass) — nothing to do here.
            }
            default -> {
                // Defensive: only DML reaches the buffer by construction.
            }
        }
    }

    /** Translates a master row index to the standby's actual index. */
    private int resolve(String table, int sourceIndex) {
        Map<Integer, Integer> tableRemap = remap.get(table);
        if (tableRemap == null) {
            return sourceIndex;
        }
        return tableRemap.getOrDefault(sourceIndex, sourceIndex);
    }

    /** Rewrites the commit payload's row indexes through the remap table. */
    private CommitPayload remapPayload(CommitPayload payload) {
        if (remap.isEmpty()) {
            return payload;
        }
        return new CommitPayload(payload.getTxid(), payload.getCommitCsn(),
                remapRows(payload.getModifiedRows()), remapRows(payload.getDeletedRows()),
                payload.getCommitTimestampMs());
    }

    private Map<String, java.util.List<Integer>> remapRows(Map<String, int[]> rows) {
        Map<String, java.util.List<Integer>> out = new HashMap<>(rows.size());
        for (Map.Entry<String, int[]> entry : rows.entrySet()) {
            Map<Integer, Integer> tableRemap = remap.get(entry.getKey());
            int[] source = entry.getValue();
            java.util.List<Integer> translated = new java.util.ArrayList<>(source.length);
            for (int value : source) {
                translated.add(tableRemap == null ? value : tableRemap.getOrDefault(value, value));
            }
            out.put(entry.getKey(), translated);
        }
        return out;
    }

    /**
     * Advances the resume point: entries may only be "applied" up to the LSN
     * just before the first buffered entry of a still-open transaction, so a
     * reconnect from {@code appliedLsn + 1} never skips uncommitted DML.
     */
    private void advanceAppliedLsn() {
        long blockedBy = Long.MAX_VALUE;
        for (List<WALEntry> entries : pending.values()) {
            if (!entries.isEmpty()) {
                blockedBy = Math.min(blockedBy, entries.get(0).getLsn());
            }
        }
        long candidate = blockedBy == Long.MAX_VALUE ? highestSeenLsn : blockedBy - 1;
        if (candidate > appliedLsn) {
            appliedLsn = candidate;
        }
    }

    private static CommitPayload decodeCommit(WALEntry entry, long lsn) {
        if (!entry.hasAfterImage()) {
            LOGGER.log(Level.WARNING, "Streaming: COMMIT record without payload at LSN " + lsn);
            return null;
        }
        try {
            return CommitPayload.deserialize(entry.getAfterImage());
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Streaming: malformed COMMIT payload at LSN "
                    + lsn + ", skipping finalize: " + e.getMessage());
            return null;
        }
    }

    private static DmlPayload decodeDml(byte[] data, long lsn, WALOpcode op) {
        if (data == null || data.length == 0) {
            LOGGER.log(Level.WARNING, "Streaming: " + op + " record without payload at LSN " + lsn);
            return null;
        }
        try {
            return DmlPayload.deserialize(data);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Streaming: malformed " + op + " payload at LSN "
                    + lsn + ", skipping entry: " + e.getMessage());
            return null;
        }
    }
}
