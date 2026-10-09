package diesel;

import diesel.recovery.MvccRedoSink;
import diesel.recovery.MvccUndoSink;
import diesel.wal.CommitPayload;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Production MVCC sink for ARIES recovery (prompt 4 #19): implements both
 * {@link MvccRedoSink} (COMMIT replay) and {@link MvccUndoSink} (logical undo
 * of active transactions) against a live {@link Database}.
 *
 * <p>Lives in the {@code diesel} package (not {@code diesel.recovery}) because
 * {@code Database}/{@code Table} are package-private engine classes, mirroring
 * how {@link VacuumManager} binds to the engine.
 *
 * <p><b>Redo (committed transactions).</b> {@code onCommit} registers the
 * transaction as COMMITTED in the {@link TxStatusTracker} with its original
 * commit CSN, re-applies commit-time tombstones for the payload's deleted row
 * indexes ({@code Table.deletedRows} is transient and lost on restart), and
 * stamps the modified/deleted rows as committed. Rows whose index is outside
 * the loaded table are counted and logged — physical redo never fails.
 *
 * <p><b>Undo (active transactions).</b> The undo callbacks re-stamp the MVCC
 * metadata that the restart discarded, so visibility hides the uncommitted
 * rows again:
 * <ul>
 *   <li>INSERT → a fresh {@link RowVersionMeta} with {@code xmin = txid} and
 *       cleared pending flags: the creator is ABORTED in the tracker, so
 *       {@code TupleVisibility.visibleByStatus} hides the row from every
 *       reader. Physical reclamation happens at vacuum (dead predicate:
 *       xmin ABORTED);</li>
 *   <li>UPDATE → the physical row is restored to the before-image when it
 *       currently holds the after-image (lenient comparison, see below);</li>
 *   <li>DELETE → uncommitted deletes are never tombstoned on disk (the
 *       tombstone is a COMMIT-time action), so the recovered row is already
 *       alive — the callback verifies that and only warns on surprises.</li>
 * </ul>
 *
 * <p><b>Lenient value guards.</b> Delimited storage may reload a numeric
 * column with a different boxed width (Integer vs Long — known parallel-load
 * behaviour), so value guards compare numbers by numeric value and other
 * values by their string form before falling back to strict equality. A
 * mismatch means the row on disk is not the row the WAL describes (index
 * skew from an out-of-band file change): the operation is skipped with a
 * warning instead of corrupting the wrong row.
 */
public final class DatabaseRecoverySink implements MvccRedoSink, MvccUndoSink {

    private static final Logger LOGGER = Logger.getLogger(DatabaseRecoverySink.class.getName());

    private final Database database;
    private final AtomicLong commitsReplayed = new AtomicLong();
    private final AtomicLong rowsSkipped = new AtomicLong();
    private final AtomicLong undosApplied = new AtomicLong();

    /**
     * Creates a sink bound to the given database.
     *
     * @param database the database whose tables and tracker are recovered
     */
    public DatabaseRecoverySink(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    // ─── MvccRedoSink ───────────────────────────────────────────────────

    @Override
    public void onCommit(CommitPayload payload) throws IOException {
        long txid = payload.getTxid();
        long csn = payload.getCommitCsn();
        TxStatusTracker tracker = database.getTxStatusTracker();
        if (tracker != null) {
            tracker.registerRecoveredCommitted(txid, csn);
        }

        for (Map.Entry<String, int[]> entry : payload.getDeletedRows().entrySet()) {
            Table table = database.getTable(entry.getKey());
            if (table == null) {
                LOGGER.log(Level.WARNING, "Recovery: unknown table in COMMIT payload: " + entry.getKey());
                rowsSkipped.addAndGet(entry.getValue().length);
                continue;
            }
            // The in-memory tombstone set is transient: commit-time deletes
            // must be re-applied from the payload, mirroring executeCommit.
            for (int rowIndex : entry.getValue()) {
                if (rowIndex < 0 || rowIndex >= table.getRawRowCount()) {
                    LOGGER.log(Level.WARNING, "Recovery: deleted row index out of range: "
                            + entry.getKey() + "[" + rowIndex + "]");
                    rowsSkipped.incrementAndGet();
                    continue;
                }
                table.markDeleted(rowIndex);
                table.markRowCommitted(rowIndex, csn);
            }
        }

        for (Map.Entry<String, int[]> entry : payload.getModifiedRows().entrySet()) {
            Table table = database.getTable(entry.getKey());
            if (table == null) {
                LOGGER.log(Level.WARNING, "Recovery: unknown table in COMMIT payload: " + entry.getKey());
                rowsSkipped.addAndGet(entry.getValue().length);
                continue;
            }
            for (int rowIndex : entry.getValue()) {
                if (rowIndex < 0 || rowIndex >= table.getRawRowCount()) {
                    // Row never made it to disk (crash before the commit-time
                    // persist): nothing to stamp, the index no longer exists.
                    LOGGER.log(Level.FINE, "Recovery: modified row index out of range: "
                            + entry.getKey() + "[" + rowIndex + "]");
                    rowsSkipped.incrementAndGet();
                    continue;
                }
                table.markRowCommitted(rowIndex, csn);
            }
        }
        commitsReplayed.incrementAndGet();
    }

    // ─── MvccUndoSink ───────────────────────────────────────────────────

    @Override
    public void onInsertUndo(long txid, String tableName, int rowIndex,
                             Map<String, Object> inserted) {
        Table table = resolve(tableName);
        if (table == null || !inRange(table, rowIndex)) {
            rowsSkipped.incrementAndGet();
            return;
        }
        if (!valuesMatch(table, rowIndex, inserted)) {
            LOGGER.log(Level.WARNING, "Recovery: INSERT undo values mismatch at "
                    + tableName + "[" + rowIndex + "], skipping");
            rowsSkipped.incrementAndGet();
            return;
        }
        // Re-stamp the creator: xmin = aborted txid, no pending flags — the
        // tracker hides the row from every reader, vacuum reclaims it later.
        RowVersionMeta meta = new RowVersionMeta(txid, null);
        meta.markAborted();
        table.setRowVersionMeta(rowIndex, meta);
        table.markRowRolledBack(rowIndex);
        undosApplied.incrementAndGet();
    }

    @Override
    public void onUpdateUndo(long txid, String tableName, int rowIndex,
                             Map<String, Object> before, Map<String, Object> after) {
        Table table = resolve(tableName);
        if (table == null || !inRange(table, rowIndex)) {
            rowsSkipped.incrementAndGet();
            return;
        }
        Map<String, Object> current = table.getVisibleRowForReader(rowIndex);
        if (valuesMatch(current, before)) {
            // Never persisted after the update: already at the before state.
            undosApplied.incrementAndGet();
            return;
        }
        if (!valuesMatch(current, after)) {
            LOGGER.log(Level.WARNING, "Recovery: UPDATE undo values mismatch at "
                    + tableName + "[" + rowIndex + "], skipping");
            rowsSkipped.incrementAndGet();
            return;
        }
        table.restoreRowAfterUpdateUndo(rowIndex, before, null);
        undosApplied.incrementAndGet();
    }

    @Override
    public void onDeleteUndo(long txid, String tableName, int rowIndex,
                             Map<String, Object> before) {
        Table table = resolve(tableName);
        if (table == null || !inRange(table, rowIndex)) {
            // Row never persisted: the delete is trivially undone.
            undosApplied.incrementAndGet();
            return;
        }
        if (table.isDeleted(rowIndex)) {
            // Uncommitted deletes are never tombstoned on disk; a tombstone
            // here means index skew — do not touch the wrong row.
            LOGGER.log(Level.WARNING, "Recovery: DELETE undo found unexpected tombstone at "
                    + tableName + "[" + rowIndex + "], skipping");
            rowsSkipped.incrementAndGet();
            return;
        }
        // The row survived without a tombstone: the uncommitted delete is
        // already undone. Values stay as-is (a delete never mutates data).
        undosApplied.incrementAndGet();
    }

    // ─── metrics ────────────────────────────────────────────────────────

    /**
     * Returns the number of COMMIT payloads replayed into the tracker/tables.
     */
    public long getCommitsReplayed() {
        return commitsReplayed.get();
    }

    /**
     * Returns the number of row operations skipped by the value/index guards.
     */
    public long getRowsSkipped() {
        return rowsSkipped.get();
    }

    /**
     * Returns the number of logical undo operations applied.
     */
    public long getUndosApplied() {
        return undosApplied.get();
    }

    // ─── helpers ────────────────────────────────────────────────────────

    private Table resolve(String tableName) {
        Table table = database.getTable(tableName);
        if (table == null) {
            LOGGER.log(Level.WARNING, "Recovery: unknown table in DML record: " + tableName);
        }
        return table;
    }

    private static boolean inRange(Table table, int rowIndex) {
        return rowIndex >= 0 && rowIndex < table.getRawRowCount();
    }

    private static boolean valuesMatch(Table table, int rowIndex, Map<String, Object> expected) {
        if (expected == null || expected.isEmpty()) {
            return true; // nothing to verify
        }
        if (rowIndex < 0 || rowIndex >= table.getRawRowCount()) {
            return false;
        }
        // Direct per-row access: getRows() would copy the whole table per undo
        // record (O(N^2) for a long transaction).
        return valuesMatch(table.getVisibleRowForReader(rowIndex), expected);
    }

    private static boolean valuesMatch(Map<String, Object> current, Map<String, Object> expected) {
        if (expected == null || expected.isEmpty()) {
            return true;
        }
        if (current == null) {
            return false;
        }
        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            if (!lenientEquals(current.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Lenient equality for recovery guards: numbers compare by numeric value
     * (boxed width may differ after a delimited reload), everything else by
     * string form, with strict {@link Objects#equals} as the first check.
     */
    static boolean lenientEquals(Object a, Object b) {
        if (Objects.equals(a, b)) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue()) == 0;
        }
        return String.valueOf(a).equals(String.valueOf(b));
    }
}
