package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write journal for online ALTER TABLE ... ALGORITHM=COPY (prompt4 #38,
 * R3-010 step 2/3).
 *
 * <p>While a copy runs, the old table stays authoritative for every reader and
 * writer. Every DML mutation of the altered table brackets its body with
 * {@link #beginMutation}/{@link #endMutation}, which gives two guarantees the
 * copy relies on:
 *
 * <ol>
 *   <li><b>Atomicity vs. the swap</b> - the barrier protocol (active counter +
 *       seal) guarantees that at seal time every in-flight mutation has fully
 *       recorded its event, and that every later mutation waits until the new
 *       structures are installed before touching the table again. This is what
 *       makes "journal replay" equivalent to synchronous dual-write.</li>
 *   <li><b>Ordering of shifting operations</b> - INSERT and physical REMOVE
 *       additionally serialize on a dedicated lock so the journal order of
 *       position-changing events equals their actual mutation order.</li>
 * </ol>
 *
 * <p>Lock order: {@code shiftingLock -> (row/table locks inside the mutation
 * body) -> monitor}. The seal only ever holds the monitor while <b>waiting</b>
 * ({@code Object.wait} releases it), so a gated writer holding row/table locks
 * can always finish and record. The monitor itself is never held while
 * acquiring a row/table lock.
 *
 * <p>Lifecycle: {@code RECORDING -> SEALED (temporary, baseline capture) ->
 * RECORDING -> SEALED (final) -> DONE | ROLLED_BACK}.
 *
 * @see AlterTableCopyAlgorithm
 */
final class DualWriteCoordinator {

    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(DualWriteCoordinator.class.getName());

    /** Lifecycle state of the journal. */
    enum State {
        RECORDING,
        SEALED,
        DONE,
        ROLLED_BACK
    }

    /** Token returned by {@link #beginMutation}; pass it back to {@link #endMutation}. */
    static final class Token {
        final boolean tracking;
        final boolean shifting;
        final long startNanos = System.nanoTime();

        Token(boolean tracking, boolean shifting) {
            this.tracking = tracking;
            this.shifting = shifting;
        }
    }

    // --- Journal events ------------------------------------------------

    sealed interface Event
            permits InsertEvent, UpdateEvent, DeleteEvent, TombstoneEvent,
                    MetaPutEvent, MetaRemoveEvent, UncommittedStateEvent, RemoveEvent {
    }

    /** A row was inserted at the given position (validated row values, no new column yet). */
    record InsertEvent(int index, Map<String, Object> row, RowVersionMeta meta) implements Event {}

    /** The row at the given index was replaced wholesale. */
    record UpdateEvent(int index, Map<String, Object> row) implements Event {}

    /** MVCC delete mark applied to the row at the given index. */
    record DeleteEvent(int index, long txid, Map<String, Object> preImage) implements Event {}

    /** Legacy tombstone set at the given index. */
    record TombstoneEvent(int index) implements Event {}

    /** Version metadata was stored for the given index. */
    record MetaPutEvent(int index, RowVersionMeta meta) implements Event {}

    /** Version metadata was removed for the given index. */
    record MetaRemoveEvent(int index) implements Event {}

    /** The row index entered/left the uncommitted-MVCC set. */
    record UncommittedStateEvent(int index, boolean present) implements Event {}

    /** The row at the given index was physically removed (index shifts). */
    record RemoveEvent(int index) implements Event {}

    private static final long DEFAULT_GATE_TIMEOUT_MS = 10_000L;

    private final String tableName;
    private final long gateTimeoutMs;
    private final Object monitor = new Object();
    /** Serializes position-changing mutations (INSERT / physical REMOVE) with each other. */
    private final ReentrantLock shiftingLock = new ReentrantLock();
    private final List<Event> journal = new ArrayList<>();

    private int activeMutations;
    private State state = State.RECORDING;
    /** Debug: description of the most recent begin() (thread + timestamp). */
    private volatile String activeDesc = "-";
    /** Cumulative time writers spent waiting at the barrier (ALTER-caused blocking only). */
    private volatile long barrierWaitNanos;

    private volatile boolean cancelRequested;

    // Progress (written by the copy thread, polled by ALTER STATUS).
    private volatile long rowsTotal;
    private volatile long rowsCopied;
    private volatile String phaseMessage = "copying rows";

    DualWriteCoordinator(String tableName) {
        this.tableName = tableName;
        this.gateTimeoutMs = Long.getLong("alter.copy.gate.timeout.ms", DEFAULT_GATE_TIMEOUT_MS);
    }

    String getTableName() {
        return tableName;
    }

    //  Mutation barrier 

    /**
     * Enters the mutation barrier for one DML operation.
     *
     * @param shifting true for position-changing operations (INSERT, physical
     *                 REMOVE) that additionally serialize against each other
     * @return a token; when {@code Token.tracking} is false the coordinator is
     *         finished (DONE/ROLLED_BACK) and the caller must run the mutation
     *         directly without calling {@link #endMutation}
     * @throws DieselException if the wait exceeds {@code alter.copy.gate.timeout.ms}
     *                         (protects against callers that hold the table
     *                         write lock while entering the barrier)
     */
    Token beginMutation(boolean shifting) {
        long deadline = System.currentTimeMillis() + gateTimeoutMs;
        long waitStart = System.nanoTime();
        synchronized (monitor) {
            while (state == State.SEALED) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new DieselException("Timed out waiting for ALTER COPY on table "
                            + tableName + " to finish its critical section");
                }
                try {
                    monitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DieselException("Interrupted while waiting for ALTER COPY on table " + tableName, e);
                }
            }
            long waitedMs = (System.nanoTime() - waitStart) / 1_000_000;
            if (waitedMs >= 5) {
                LOGGER.log(java.util.logging.Level.INFO,
                        "DML barrier wait on table {0}: {1} ms (state={2})", new Object[]{tableName, waitedMs, state});
            }
            barrierWaitNanos += (System.nanoTime() - waitStart);
            if (state != State.RECORDING) {
                return new Token(false, false);
            }
            activeMutations++;
            activeDesc = Thread.currentThread().getName() + "@" + System.nanoTime();
        }
        if (shifting) {
            shiftingLock.lock();
        }
        return new Token(true, shifting);
    }

    /**
     * Leaves the mutation barrier, recording the event this mutation produced.
     * No-op for a token with {@code tracking == false}.
     *
     * @param token the token returned by {@link #beginMutation}
     * @param event the journal event, or null if the method produced none
     */
    void endMutation(Token token, Event event) {
        if (token == null || !token.tracking) {
            return;
        }
        try {
            long tookMs = (System.nanoTime() - token.startNanos) / 1_000_000;
            synchronized (monitor) {
                if (event != null) {
                    journal.add(event);
                }
                activeMutations--;
                monitor.notifyAll();
                if (tookMs >= 10) {
                    LOGGER.log(java.util.logging.Level.INFO,
                            "Gated mutation took {0} ms on table {1} (thread {2})",
                            new Object[]{tookMs, tableName, Thread.currentThread().getName()});
                }
            }
        } finally {
            if (token.shifting) {
                shiftingLock.unlock();
            }
        }
    }

    //  Quiesce / seal protocol 

    /**
     * Temporarily blocks new mutations and waits for in-flight ones so the
     * caller can capture the baseline under the table write lock. Must be
     * paired with {@link #quiesceEnd()} (also on failure).
     */
    void quiesceBegin() {
        awaitActiveAndSeal("baseline capture");
    }

    /** Reopens the journal after a successful baseline capture. */
    void quiesceEnd() {
        synchronized (monitor) {
            if (state == State.SEALED) {
                state = State.RECORDING;
            }
            monitor.notifyAll();
        }
    }

    /**
     * Final barrier: waits for in-flight mutations, seals the journal and
     * returns everything recorded since the baseline. From here on every DML
     * on the table blocks until {@link #markDone()}/{@link #markRolledBack()}.
     * Must be called WITHOUT holding the table write lock (an in-flight
     * UPDATE holds the barrier while acquiring that lock).
     *
     * @return the ordered event list to replay onto the shadow table
     */
    List<Event> seal() {
        awaitActiveAndSeal("finalize");
        synchronized (monitor) {
            List<Event> drained = new ArrayList<>(journal);
            journal.clear();
            return drained;
        }
    }

    private void awaitActiveAndSeal(String phase) {
        long deadline = System.currentTimeMillis() + gateTimeoutMs;
        long waitStart = System.nanoTime();
        synchronized (monitor) {
            while (activeMutations > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new DieselException("Timed out waiting for in-flight writers during ALTER COPY "
                            + phase + " on table " + tableName);
                }
                try {
                    monitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DieselException("Interrupted during ALTER COPY " + phase + " on table " + tableName, e);
                }
            }
            long waitedMs = (System.nanoTime() - waitStart) / 1_000_000;
            if (waitedMs >= 5) {
                LOGGER.log(java.util.logging.Level.INFO,
                        "ALTER COPY {0} waited {1} ms for in-flight writers on table {2} (last begin: {3})",
                        new Object[]{phase, waitedMs, tableName, activeDesc});
            }
            state = State.SEALED;
        }
    }

    /** Marks the journal finished: the new schema/data are installed (or the copy was abandoned). */
    void markDone() {
        synchronized (monitor) {
            state = State.DONE;
            journal.clear();
            monitor.notifyAll();
        }
    }

    /** Marks the journal rolled back: the old table remains authoritative, nothing installed. */
    void markRolledBack() {
        synchronized (monitor) {
            state = State.ROLLED_BACK;
            journal.clear();
            monitor.notifyAll();
        }
    }

    State getState() {
        synchronized (monitor) {
            return state;
        }
    }

    //  Cancellation 

    void requestCancel() {
        cancelRequested = true;
    }

    boolean isCancelRequested() {
        return cancelRequested;
    }

    //  Progress 

    void setRowsTotal(long total) {
        this.rowsTotal = total;
    }

    void setRowsCopied(long copied) {
        this.rowsCopied = copied;
    }

    void setPhaseMessage(String message) {
        this.phaseMessage = message;
    }

    long getRowsTotal() {
        return rowsTotal;
    }

    long getRowsCopied() {
        return rowsCopied;
    }

    String getPhaseMessage() {
        return phaseMessage;
    }

    int getPendingJournalSize() {
        synchronized (monitor) {
            return journal.size();
        }
    }

    /**
     * Cumulative time all writers spent blocked at the barrier - the part of
     * writer latency actually caused by the ALTER (used by the acceptance
     * criterion; per-insert costs like CSV persist are excluded).
     */
    long getBarrierWaitNanos() {
        return barrierWaitNanos;
    }
}
