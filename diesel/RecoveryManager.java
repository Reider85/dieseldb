package diesel;

import diesel.recovery.ARIESAlgorithm;
import diesel.recovery.AnalysisResult;
import diesel.recovery.RecoveryResult;
import diesel.storage.page.PageManager;
import diesel.wal.WALManager;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Startup ARIES recovery orchestrator (prompt 4 #19): wraps
 * {@link ARIESAlgorithm#recover} with MVCC wiring, wall-clock timing and a
 * JMX metric surface ({@code diesel:type=RecoveryManager,id=N}).
 *
 * <p>Lives in the {@code diesel} package (not {@code diesel.recovery}) because
 * {@code Database} is a package-private engine class, mirroring how
 * {@link VacuumManager} binds to the engine.
 *
 * <p>{@link #recover()} runs analysis → redo → undo against the database's
 * WAL and page manager, using a {@link DatabaseRecoverySink} to restore the
 * committed transaction state and to hide uncommitted rows again. It is
 * invoked from {@code DatabaseServer.start()} strictly before the server
 * accepts client connections; when the WAL is disabled the manager is never
 * created and recovery is skipped entirely.
 *
 * <p>The manager is {@link AutoCloseable}: {@link #close()} unregisters the
 * JMX MBean (idempotent, like every other engine MBean).
 */
public final class RecoveryManager implements AutoCloseable, DynamicMBean {

    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());

    /** JMX object name prefix. */
    static final String OBJECT_NAME_PREFIX = "diesel:type=RecoveryManager";
    /** JMX attribute names. */
    static final String ATTR_DURATION_MS = "recovery.duration.ms";
    static final String ATTR_COMPLETED = "recovery.completed";
    static final String ATTR_COMMITS_REPLAYED = "recovery.commits.replayed";
    static final String ATTR_UNDOS_APPLIED = "recovery.undo.operations";
    static final String ATTR_ACTIVE_TXIDS = "recovery.active.txids";
    static final String ATTR_LAST_LSN = "recovery.last.lsn";

    /** MBean sequence counter for multiple instances. */
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final Database database;
    private final AtomicLong recoveryDurationMs = new AtomicLong(-1);
    private final AtomicLong lastLsn = new AtomicLong(-1);
    private volatile boolean recoveryCompleted;
    private volatile RecoveryResult lastResult;
    private volatile DatabaseRecoverySink lastSink;
    private volatile ObjectName registeredName;

    /**
     * Creates a recovery manager bound to the given database.
     *
     * @param database the database whose WAL/pages/tables are recovered
     */
    public RecoveryManager(Database database) {
        this.database = database;
        registerMBean();
    }

    /**
     * Runs full ARIES recovery: analysis → redo → undo, timed.
     *
     * <p>Idempotent in the sense that re-running it re-applies the same
     * logical state (redo page installs are LSN-idempotent, undo meta stamps
     * overwrite themselves, commit registrations are keyed by txid).
     *
     * @return the combined recovery result
     * @throws IOException if reading the WAL/checkpoint, writing pages, or a
     *                     sink operation fails
     */
    public RecoveryResult recover() throws IOException {
        WALManager wal = database.getWalManager();
        PageManager pages = database.getPageManager();
        if (wal == null) {
            throw new IllegalStateException("Recovery requires an enabled WAL (walWriter is null)");
        }

        DatabaseRecoverySink sink = new DatabaseRecoverySink(database);
        long start = System.nanoTime();
        RecoveryResult result = ARIESAlgorithm.recover(wal, pages, sink, sink);
        long durationMs = (System.nanoTime() - start) / 1_000_000L;

        // Restore the tracker: active txids become ABORTED (the undo phase
        // rolled their changes back — without this registration the visibility
        // contract would treat their xmin as unknown/committed and show the
        // rows again), and the counters are floored past every recovered id so
        // post-restart allocations never collide.
        applyTrackerRecovery(result);

        recoveryDurationMs.set(durationMs);
        lastLsn.set(result.getUndo().getLastLsn());
        lastResult = result;
        lastSink = sink;
        recoveryCompleted = true;

        LOGGER.info(String.format(
                "ARIES recovery finished in %d ms: %d committed / %d active txids, "
                        + "redo[applied=%d skipped=%d commits=%d ignored=%d], "
                        + "undo[inserts=%d updates=%d deletes=%d ignored=%d]",
                durationMs,
                result.getCommittedTxidCount(), result.getActiveTxidCount(),
                result.getRedo().getApplied(), result.getRedo().getSkipped(),
                result.getRedo().getCommitsReplayed(), result.getRedo().getIgnored(),
                result.getUndo().getUndoneInserts(), result.getUndo().getUndoneUpdates(),
                result.getUndo().getUndoneDeletes(), result.getUndo().getIgnored()));
        return result;
    }

    private void applyTrackerRecovery(RecoveryResult result) {
        TxStatusTracker tracker = database.getTxStatusTracker();
        if (tracker == null) {
            return;
        }
        long maxTxid = 0;
        AnalysisResult analysis = result.getAnalysis();
        for (Long txid : analysis.getActive()) {
            tracker.registerRecoveredAbort(txid);
            maxTxid = Math.max(maxTxid, txid);
        }
        for (Long txid : analysis.getCommitted()) {
            // Committed txids were registered by the redo sink with their
            // original CSN (payload-less legacy commits stay unknown, which
            // the visibility contract treats as committed/alive).
            maxTxid = Math.max(maxTxid, txid);
        }
        if (maxTxid > 0) {
            tracker.advanceNextTxidBeyond(maxTxid);
        }
    }

    /**
     * Returns the wall-clock duration of the last {@link #recover()} call in
     * milliseconds, or {@code -1} when recovery has not run yet.
     */
    public long getRecoveryDurationMs() {
        return recoveryDurationMs.get();
    }

    /**
     * Returns true once {@link #recover()} has completed.
     */
    public boolean isRecoveryCompleted() {
        return recoveryCompleted;
    }

    /**
     * Returns the combined result of the last recovery, or {@code null}.
     */
    public RecoveryResult getLastResult() {
        return lastResult;
    }

    /**
     * Returns the sink of the last recovery (metric access), or {@code null}.
     */
    public DatabaseRecoverySink getLastSink() {
        return lastSink;
    }

    @Override
    public void close() {
        unregisterMBean();
    }

    // ─── DynamicMBean implementation ─────────────────────────────────────

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case ATTR_DURATION_MS:
                return getRecoveryDurationMs();
            case ATTR_COMPLETED:
                return isRecoveryCompleted();
            case ATTR_COMMITS_REPLAYED:
                return lastSink != null ? lastSink.getCommitsReplayed() : 0L;
            case ATTR_UNDOS_APPLIED:
                return lastSink != null ? lastSink.getUndosApplied() : 0L;
            case ATTR_ACTIVE_TXIDS:
                return lastResult != null ? lastResult.getActiveTxidCount() : 0;
            case ATTR_LAST_LSN:
                return lastLsn.get();
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("RecoveryManager attributes are read-only");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList result = new AttributeList();
        for (String attribute : attributes) {
            try {
                result.add(new Attribute(attribute, getAttribute(attribute)));
            } catch (Exception ignored) {
                // Skip attributes that cannot be read.
            }
        }
        return result;
    }

    @Override
    @SuppressWarnings("unused")
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    @SuppressWarnings("unused")
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        throw new ReflectionException(new UnsupportedOperationException(
                "No operations on RecoveryManager; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
            ATTR_DURATION_MS, ATTR_COMPLETED, ATTR_COMMITS_REPLAYED,
            ATTR_UNDOS_APPLIED, ATTR_ACTIVE_TXIDS, ATTR_LAST_LSN
        };
        String[] types = {
            "long", "boolean", "long", "long", "int", "long"
        };
        String[] descriptions = {
            "Wall-clock duration of the last recovery run in milliseconds",
            "Whether startup recovery has completed",
            "Number of COMMIT payloads replayed during recovery",
            "Number of logical undo operations applied during recovery",
            "Number of transactions found active (uncommitted) by analysis",
            "End-of-log LSN snapshot used by the recovery pass"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], types[i], descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(RecoveryManager.class.getName(),
                "DieselDB ARIES Recovery Manager (startup analysis/redo/undo)",
                attributes, null, null, null);
    }

    // ─── JMX registration ───────────────────────────────────────────────

    private void registerMBean() {
        if (registeredName != null) {
            return;
        }
        synchronized (this) {
            if (registeredName != null) {
                return;
            }
            try {
                ObjectName name = new ObjectName(
                        OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
                ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
                registeredName = name;
            } catch (Exception e) {
                LOGGER.warning("Failed to register RecoveryManager MBean: " + e);
            }
        }
    }

    private void unregisterMBean() {
        ObjectName name = registeredName;
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
            registeredName = null;
        }
    }
}
