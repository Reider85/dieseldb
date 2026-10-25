package diesel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Online ALTER TABLE ... ALGORITHM=COPY (prompt4 #38, R3-010 step 2/3).
 *
 * <p>Phases:
 * <ol>
 *   <li><b>Start</b> - quiesce writers briefly, attach the
 *       {@link DualWriteCoordinator} journal and snapshot the baseline row
 *       structure (shallow reference copy) into a shadow table with the new
 *       schema.</li>
 *   <li><b>Background copy</b> - deep-copies every baseline row into the
 *       shadow while writers keep working on the old table; each mutation is
 *       journaled. Progress is reported through {@code ALTER STATUS}.</li>
 *   <li><b>Finalize</b> - seal the journal (writers block for a moment),
 *       replay the journal onto the shadow, atomically install the shadow's
 *       row/MVCC structures into the live table, bump the schema version,
 *       rebuild indexes, publish the catalog, then rewrite the data file
 *       (atomic rename) outside the write lock.</li>
 *   <li><b>Rollback</b> - on cancel or failure nothing is installed: the old
 *       schema and data remain authoritative and the journal is discarded.</li>
 * </ol>
 *
 * <p>Writes are not duplicated synchronously into the shadow (classic
 * dual-write); the journal replay at finalize is equivalent because the
 * barrier protocol makes every mutation atomic with respect to the seal.
 *
 * @see DualWriteCoordinator
 */
final class AlterTableCopyAlgorithm implements Runnable {

    /** Raised when {@link #requestCancel()} interrupted the copy. */
    static final class AlterCopyCancelledException extends RuntimeException {
        AlterCopyCancelledException(String tableName) {
            super("ALTER COPY cancelled on table " + tableName);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(AlterTableCopyAlgorithm.class.getName());

    private final Database database;
    private final Table table;
    private final String tableName;
    private final List<String> newColumns;
    private final Map<String, Class<?>> newTypes;
    private final String description;
    private final DualWriteCoordinator coordinator;
    private final Table shadow;
    private final int batchSize;
    private final long batchSleepMs;
    /** Column whose indexes are removed at swap time (DROP COLUMN copies, prompt4 #39); null otherwise. */
    private final String stripIndexColumn;

    private volatile Future<?> future;
    /** Terminal state (COMPLETED/ROLLED_BACK/FAILED) or null while running. */
    private volatile String terminalState;
    private volatile String terminalMessage = "";
    /** True once the shadow structures are installed in the live table (rollback no longer undoes). */
    private volatile boolean installed;

    AlterTableCopyAlgorithm(Database database, Table table, List<String> newColumns,
                            Map<String, Class<?>> newTypes, String description) {
        this(database, table, newColumns, newTypes, description, null);
    }

    /**
     * @param stripIndexColumn for DROP COLUMN copies (prompt4 #39): every index
     *        keyed on this column is removed inside the finalize write-lock
     *        window (so a rollback leaves the indexes untouched); null for ADD
     *        COLUMN copies.
     */
    AlterTableCopyAlgorithm(Database database, Table table, List<String> newColumns,
                            Map<String, Class<?>> newTypes, String description,
                            String stripIndexColumn) {
        this.database = database;
        this.table = table;
        this.tableName = table.getName();
        this.newColumns = newColumns;
        this.newTypes = newTypes;
        this.description = description;
        this.stripIndexColumn = stripIndexColumn;
        this.coordinator = new DualWriteCoordinator(tableName);
        this.batchSize = Math.max(1, Integer.getInteger("alter.copy.batch.size", 2048));
        this.batchSleepMs = Math.max(0, Long.getLong("alter.copy.batch.sleep.ms", 0L));
        // Fails fast (before anything is journaled) on an invalid schema change.
        Table.validateSchemaChange(table.getColumns(), table.getPrimaryKeyColumn(), newColumns, newTypes);
        // Prompt4 #39: a DROP COLUMN copy must strip the removed column's keys
        // from baseline copies and replayed journal rows - the shadow never
        // sees them, so the installed state is physically clean.
        java.util.Set<String> stripColumns = null;
        if (newColumns.size() < table.getColumns().size()) {
            stripColumns = new java.util.HashSet<>(table.getColumns());
            stripColumns.removeAll(newColumns);
        }
        this.shadow = stripColumns == null
                ? Table.forAlterShadow(database, tableName + "$copy",
                        newColumns, newTypes, table.getPrimaryKeyColumn())
                : Table.forAlterShadow(database, tableName + "$copy",
                        newColumns, newTypes, table.getPrimaryKeyColumn(), stripColumns);
    }

    /**
     * Quiesces writers, attaches the journal and captures the baseline, then
     * submits the background copy. Any failure here rolls the coordinator back
     * before rethrowing, so the caller sees a clean error with the table
     * untouched.
     */
    void start(ExecutorService executor) {
        coordinator.quiesceBegin();
        boolean attached = false;
        boolean baselineTaken = false;
        try {
            table.getTableLock().writeLock().lock();
            try {
                table.attachDualWrite(coordinator);
                attached = true;
                shadow.adoptBaselineFrom(table);
                baselineTaken = true;
                coordinator.setRowsTotal(shadow.alterBaselineSize());
            } finally {
                table.getTableLock().writeLock().unlock();
            }
            coordinator.quiesceEnd();
        } catch (RuntimeException e) {
            if (attached) {
                table.getTableLock().writeLock().lock();
                try {
                    table.detachDualWrite();
                } finally {
                    table.getTableLock().writeLock().unlock();
                }
            }
            coordinator.markRolledBack();
            if (!baselineTaken) {
                coordinator.quiesceEnd();
            }
            throw e;
        }
        this.future = executor.submit(this);
    }

    @Override
    public void run() {
        try {
            copyPhase();
            finalizePhase();
        } catch (AlterCopyCancelledException c) {
            rollback("ROLLED_BACK", "copy cancelled before completion");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rollback("ROLLED_BACK", "copy interrupted before completion");
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "ALTER COPY failed on table " + tableName, t);
            rollback("FAILED", "copy failed: " + t.getMessage());
        }
    }

    /** Deep-copies every baseline row into the shadow table, reporting progress. */
    private void copyPhase() throws InterruptedException {
        coordinator.setPhaseMessage("copying rows");
        int total = shadow.alterBaselineSize();
        for (int i = 0; i < total; i++) {
            if (coordinator.isCancelRequested()) {
                throw new AlterCopyCancelledException(tableName);
            }
            shadow.alterDeepCopyRowAt(i);
            if ((i + 1) % batchSize == 0 || i + 1 == total) {
                coordinator.setRowsCopied(i + 1);
                if (batchSleepMs > 0) {
                    Thread.sleep(batchSleepMs);
                } else {
                    Thread.yield();
                }
            }
        }
    }

    /**
     * Seals the journal, replays it onto the shadow and installs the new
     * state. Writers are blocked from seal until {@link #markDone} - this
     * section stays lock-only (no disk IO): the catalog flush and the data
     * file rewrite run after the lock is released.
     *
     * <p>No index rebuild happens here: the shadow's row layout is
     * position-identical to the live table by construction (baseline + the
     * same ordered mutations the live table already applied), so every index
     * entry's row position stays valid across the swap.
     */
    private void finalizePhase() {
        coordinator.setPhaseMessage("finalizing");
        long t0 = System.nanoTime();
        // Seal WITHOUT the table write lock: an in-flight UPDATE holds the
        // barrier while acquiring that lock, so waiting under it would deadlock.
        List<DualWriteCoordinator.Event> pending = coordinator.seal();
        long t1 = System.nanoTime();
        if (coordinator.isCancelRequested()) {
            throw new AlterCopyCancelledException(tableName);
        }
        table.getTableLock().writeLock().lock();
        long t2 = System.nanoTime();
        try {
            for (DualWriteCoordinator.Event event : pending) {
                shadow.applyAlterEvent(event);
            }
            long t3 = System.nanoTime();
            table.adoptAlterState(shadow);
            installed = true;
            table.applySchemaChange(newColumns, newTypes, description);
            if (stripIndexColumn != null) {
                // DROP COLUMN (prompt4 #39): the column's indexes are removed
                // inside the swap window - rollback (installed == false) never
                // reaches here, so a cancelled copy keeps every index intact.
                table.dropIndexesForColumn(stripIndexColumn);
            }
            coordinator.markDone();
            table.detachDualWrite();
            long t4 = System.nanoTime();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.log(Level.INFO,
                        "ALTER COPY finalize phases for {0}: seal={1}ms lockAcquire={2}ms replay={3}ms install={4}ms events={5}",
                        new Object[]{tableName, (t1 - t0) / 1_000_000, (t2 - t1) / 1_000_000,
                                (t3 - t2) / 1_000_000, (t4 - t3) / 1_000_000, pending.size()});
            }
        } finally {
            table.getTableLock().writeLock().unlock();
        }
        long t5 = System.nanoTime();
        database.publishAlterCatalog(tableName, table);
        table.saveToFile(tableName);
        long t6 = System.nanoTime();
        LOGGER.log(Level.INFO, "ALTER COPY durable phase for {0}: catalog={1}ms save={2}ms",
                new Object[]{tableName, (t6 - t5) / 1_000_000, (t6 - t5) / 1_000_000});
        finish("COMPLETED", "copy completed: " + tableName);
    }

    /**
     * Abandons the copy: the journal is discarded, the shadow dropped and the
     * old table (schema + data) remains authoritative. When the structures
     * were already installed ({@link #installed}) the rollback only reports
     * the failure - undoing a completed swap is not attempted.
     */
    private void rollback(String state, String message) {
        if (installed) {
            // The swap already landed; terminate the barrier so writers waiting
            // at beginMutation resume against the installed structures.
            coordinator.markDone();
            table.detachDualWrite();
        } else {
            try {
                coordinator.seal();
            } catch (RuntimeException sealError) {
                LOGGER.log(Level.WARNING, "Seal during ALTER COPY rollback failed on " + tableName, sealError);
            }
            table.getTableLock().writeLock().lock();
            try {
                coordinator.markRolledBack();
                table.detachDualWrite();
            } finally {
                table.getTableLock().writeLock().unlock();
            }
        }
        finish(state, message);
    }

    private void finish(String state, String message) {
        terminalMessage = message;
        terminalState = state;
        coordinator.setPhaseMessage(message);
        database.onAlterFinished(this);
    }

    void requestCancel() {
        coordinator.requestCancel();
    }

    boolean isFinished() {
        return terminalState != null;
    }

    String getTableName() {
        return tableName;
    }

    Future<?> getFuture() {
        return future;
    }

    /**
     * One {@code ALTER STATUS} row for this operation: table name, state,
     * completion percentage and row counters.
     */
    Map<String, Object> statusRow() {
        String state = terminalState != null ? terminalState : "RUNNING";
        long total = coordinator.getRowsTotal();
        long copied = coordinator.getRowsCopied();
        int percent;
        if (total <= 0) {
            percent = terminalState != null ? 100 : 0;
        } else {
            percent = (int) Math.min(100L, (copied * 100L) / total);
        }
        Map<String, Object> row = new HashMap<>();
        row.put("TABLE_NAME", tableName);
        row.put("STATE", state);
        row.put("PROGRESS_PERCENT", percent);
        row.put("ROWS_COPIED", copied);
        row.put("ROWS_TOTAL", total);
        row.put("BARRIER_WAIT_MS", coordinator.getBarrierWaitNanos() / 1_000_000L);
        row.put("MESSAGE", terminalState != null ? terminalMessage : coordinator.getPhaseMessage());
        return row;
    }
}
