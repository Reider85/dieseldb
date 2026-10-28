package diesel;

import diesel.recovery.PitrSink;
import diesel.wal.CommitPayload;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Engine-bound applier for point-in-time recovery (prompt 4 #43): realizes the
 * {@link PitrSink} callbacks against a live {@link Database} whose tables were
 * loaded from a restored backup directory.
 *
 * <p>Lives in the {@code diesel} package (not {@code diesel.recovery}) because
 * {@code Database}/{@code Table} are package-private engine classes, mirroring
 * {@link DatabaseRecoverySink}. It reuses that sink's MVCC commit/undo guards
 * for the commit-finalization path and adds forward-apply logic that crash
 * recovery does not need (crash recovery never replays committed DML forward —
 * it relies on the physical pages already being durable).
 *
 * <p><b>Forward apply.</b> {@link #applyInsert} appends the after-image via
 * {@link Table#addRowWithMVCC} and returns the actual row index so the driver
 * can remap later references; {@link #applyUpdate} restores the after-image
 * through {@link Table#restoreRowAfterUpdateUndo}, which rewrites secondary
 * index entries for the keys that change. Both are value-guarded: a row that
 * matches the target image is treated as already applied (idempotent re-run),
 * and a mismatched row is skipped with a warning instead of being corrupted.
 *
 * <p><b>Durable hide.</b> Hidden transactions are made invisible with the
 * engine's own commit/abort semantics rather than transient MVCC stamps, so
 * the restored directory stays correct after a later reopen: hidden INSERTs
 * are tombstoned via {@link Table#markDeleted} (the {@code deletedRows} bitset
 * is serialized; a legacy tombstone with no xmax is hidden from every reader),
 * hidden UPDATEs restore their before-image, and every hidden txid is
 * registered ABORTED in the {@link TxStatusTracker}.
 */
public final class DatabasePitrSink implements PitrSink {

    private static final Logger LOGGER = Logger.getLogger(DatabasePitrSink.class.getName());

    private final Database database;
    private final TxStatusTracker tracker;

    /** Per-table count of rows hidden/committed, surfaced via metric getters. */
    private final Map<String, Long> appliedByTable = new LinkedHashMap<>();

    /**
     * Creates a sink bound to the given database.
     *
     * @param database the restored database whose tables are replayed into
     */
    public DatabasePitrSink(Database database) {
        this.database = Objects.requireNonNull(database, "database");
        this.tracker = database.getTxStatusTracker();
    }

    @Override
    public int applyInsert(long txid, String table, int rowIndex, Map<String, Object> after)
            throws IOException {
        Table t = database.findTable(table);
        if (t == null) {
            LOGGER.log(Level.WARNING, "PITR: unknown table in INSERT: " + table);
            return -1;
        }
        int rawCount = t.getRawRowCount();
        // Idempotent guard: a row already present at this index with the same
        // values was applied by an earlier pass or captured by the backup.
        if (rowIndex >= 0 && rowIndex < rawCount) {
            Map<String, Object> current = t.getVisibleRowForReader(rowIndex);
            if (DatabaseRecoverySink.valuesMatch(current, after)) {
                return rowIndex;
            }
        }
        try {
            int actual = t.addRowWithMVCC(new LinkedHashMap<>(after), txid);
            // The transaction is already known-committed at the target; stamp
            // the row immediately so no reader ever observes it as pending.
            t.markRowCommitted(actual, 0);
            noteApplied(table);
            return actual;
        } catch (RuntimeException e) {
            // Unique/type constraint or index skew: never corrupt, skip.
            LOGGER.log(Level.WARNING, "PITR: skipping INSERT into " + table
                    + " at index " + rowIndex + ": " + e.getMessage());
            return -1;
        }
    }

    @Override
    public int applyUpdate(long txid, String table, int rowIndex,
                           Map<String, Object> before, Map<String, Object> after)
            throws IOException {
        Table t = database.findTable(table);
        if (t == null) {
            LOGGER.log(Level.WARNING, "PITR: unknown table in UPDATE: " + table);
            return 0;
        }
        if (rowIndex < 0 || rowIndex >= t.getRawRowCount()) {
            LOGGER.log(Level.FINE, "PITR: UPDATE row index out of range in "
                    + table + "[" + rowIndex + "], skipping");
            return 0;
        }
        Map<String, Object> current = t.getVisibleRowForReader(rowIndex);
        if (DatabaseRecoverySink.valuesMatch(current, after)) {
            return 0; // already at the after-image (idempotent re-run)
        }
        if (!DatabaseRecoverySink.valuesMatch(current, before)) {
            LOGGER.log(Level.WARNING, "PITR: UPDATE values mismatch in "
                    + table + "[" + rowIndex + "], skipping");
            return 0;
        }
        t.restoreRowAfterUpdateUndo(rowIndex, new LinkedHashMap<>(after), null);
        noteApplied(table);
        return 1;
    }

    @Override
    public int hideInsert(long txid, String table, int rowIndex, Map<String, Object> after)
            throws IOException {
        Table t = database.findTable(table);
        if (t == null) {
            return 0;
        }
        if (rowIndex < 0 || rowIndex >= t.getRawRowCount()) {
            return 0; // row never persisted: nothing to hide
        }
        if (t.isDeleted(rowIndex)) {
            return 0; // already tombstoned
        }
        Map<String, Object> current = t.getVisibleRowForReader(rowIndex);
        if (!DatabaseRecoverySink.valuesMatch(current, after)) {
            LOGGER.log(Level.FINE, "PITR: hidden INSERT values mismatch in "
                    + table + "[" + rowIndex + "], leaving untouched");
            return 0;
        }
        t.markDeleted(rowIndex);
        noteApplied(table);
        return 1;
    }

    @Override
    public int hideUpdate(long txid, String table, int rowIndex,
                          Map<String, Object> before, Map<String, Object> after)
            throws IOException {
        Table t = database.findTable(table);
        if (t == null) {
            return 0;
        }
        if (rowIndex < 0 || rowIndex >= t.getRawRowCount()) {
            return 0;
        }
        Map<String, Object> current = t.getVisibleRowForReader(rowIndex);
        if (DatabaseRecoverySink.valuesMatch(current, before)) {
            return 0; // already at the before-image
        }
        if (!DatabaseRecoverySink.valuesMatch(current, after)) {
            LOGGER.log(Level.FINE, "PITR: hidden UPDATE values mismatch in "
                    + table + "[" + rowIndex + "], leaving untouched");
            return 0;
        }
        t.restoreRowAfterUpdateUndo(rowIndex, new LinkedHashMap<>(before), null);
        noteApplied(table);
        return 1;
    }

    @Override
    public int onCommit(CommitPayload payload) throws IOException {
        // Reuse the production MVCC commit path: register the txid COMMITTED
        // with its original CSN, re-apply commit-time tombstones and stamp the
        // modified rows. Guards and out-of-range skips are identical to crash
        // recovery.
        int tombstones = 0;
        for (int[] rows : payload.getDeletedRows().values()) {
            tombstones += rows.length;
        }
        commitDelegate().onCommit(payload);
        return tombstones;
    }

    @Override
    public void onAbort(long txid) throws IOException {
        if (tracker != null) {
            tracker.registerRecoveredAbort(txid);
        }
    }

    /** Returns the number of tables touched by apply/hide operations. */
    public int getTouchedTableCount() {
        return appliedByTable.size();
    }

    /** Returns total row operations (applies + hides) recorded per table. */
    public Map<String, Long> getAppliedByTable() {
        return java.util.Collections.unmodifiableMap(appliedByTable);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private DatabaseRecoverySink commitDelegate;

    /**
     * Lazily builds the production commit sink. Kept as a separate instance so
     * the PITR apply/hide counters stay independent of crash-recovery metrics.
     */
    private DatabaseRecoverySink commitDelegate() {
        if (commitDelegate == null) {
            commitDelegate = new DatabaseRecoverySink(database);
        }
        return commitDelegate;
    }

    private void noteApplied(String table) {
        appliedByTable.merge(table, 1L, Long::sum);
    }
}
