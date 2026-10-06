package diesel;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reclaims row versions that no transaction can ever observe again
 * (prompt4.md step 3, ROADMAP3 R3-001).
 *
 * <p><b>Dead-row predicate.</b> A raw row is dead when any of these holds:
 * <ol>
 *   <li>it is tombstoned ({@link Table#isDeleted(int)}) — a committed DELETE;</li>
 *   <li>it has MVCC metadata whose creating transaction aborted and whose
 *       undo already cleared the uncommitted flags;</li>
 *   <li>its deleting transaction committed with a commit CSN at or below the
 *       vacuum horizon (the minimum snapshot CSN of every active, non-batch
 *       transaction) — every current and future reader already sees the
 *       delete;</li>
 *   <li>rows with {@code meta == null} (auto-commit/bulk rows) and rows with
 *       pending uncommitted changes are conservatively kept alive.</li>
 * </ol>
 *
 * <p><b>Algorithm (batched, prompt4.md acceptance: writers must not be blocked
 * longer than 100 ms).</b> Each pass scans the raw row list in
 * {@code vacuum.batch.size} batches. A batch runs under the table
 * {@code tableLock} write lock: it re-validates the structural stamp
 * (raw row count) before and after marking, so concurrent structural changes
 * abort the pass instead of corrupting positions. Between batches the lock is
 * released, so writers interleave freely. The final phase re-validates every
 * pending position, disassociates the storage index entries via
 * {@code IndexManager.removeDeadEntries} and physically removes the rows with
 * a single {@link Table#compact()} (which rebuilds all indexes and remaps the
 * MVCC metadata). Unstable passes are retried up to three times.
 *
 * <p><b>Metrics</b> are exposed through this class as a {@link DynamicMBean}
 * ({@code diesel:type=VacuumManager,id=N}): {@code vacuum.duration.ms},
 * {@code vacuum.dead_tuples_removed}, {@code vacuum.runs} and
 * {@code vacuum.lastRunEpochMs}. The MBean is registered lazily on the first
 * vacuum run (or auto-vacuum start) and unregistered by {@link #stop()}.
 *
 * <p><b>Auto-vacuum</b> runs a daemon thread every {@code vacuum.interval.ms}
 * (default 60 000 ms); it is not started by the {@link Database} constructor —
 * the server ({@code DatabaseServer.start()}) or explicit tests call
 * {@link #startAutoVacuum()}.
 */
public class VacuumManager implements DynamicMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(VacuumManager.class);

    /** Configuration key: auto-vacuum period in milliseconds. */
    static final String INTERVAL_PROPERTY = "vacuum.interval.ms";
    /** Configuration key: rows scanned per write-lock batch. */
    static final String BATCH_SIZE_PROPERTY = "vacuum.batch.size";
    /** Default auto-vacuum period (60 seconds). */
    static final long DEFAULT_INTERVAL_MS = 60_000L;
    /** Default number of rows scanned per locked batch. */
    static final int DEFAULT_BATCH_SIZE = 10_000;
    /** Maximum mark/finalize attempts before a pass yields to concurrent churn. */
    static final int MAX_PASSES = 3;

    static final String OBJECT_NAME_PREFIX = "diesel:type=VacuumManager";
    static final String ATTR_DURATION_MS = "vacuum.duration.ms";
    static final String ATTR_DEAD_TUPLES = "vacuum.dead_tuples_removed";
    static final String ATTR_RUNS = "vacuum.runs";
    static final String ATTR_LAST_RUN_EPOCH_MS = "vacuum.lastRunEpochMs";

    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final Database database;
    private final long intervalMs;
    private final int batchSize;

    private final AtomicLong deadTuplesRemoved = new AtomicLong();
    private final AtomicLong vacuumRuns = new AtomicLong();
    private volatile long lastDurationMs;
    private volatile long lastRunEpochMs;

    private final Object lifecycleLock = new Object();
    private Thread autoVacuumThread;
    private volatile boolean autoVacuumRunning;
    private volatile ObjectName registeredName;

    /**
     * Creates a manager reading {@code vacuum.interval.ms} and
     * {@code vacuum.batch.size} from the system properties first (test
     * override, same pattern as the profiler's slow-threshold property) and
     * then from {@code config.properties}.
     *
     * @param database the database whose tables are vacuumed
     */
    public VacuumManager(Database database) {
        this(database, loadLong(INTERVAL_PROPERTY, DEFAULT_INTERVAL_MS),
                (int) loadLong(BATCH_SIZE_PROPERTY, DEFAULT_BATCH_SIZE));
    }

    private static long loadLong(String key, long defaultValue) {
        String property = System.getProperty(key);
        if (property != null) {
            try {
                return Long.parseLong(property.trim());
            } catch (NumberFormatException ignored) {
                // Fall through to the config file/default.
            }
        }
        return ConfigLoader.getLong(key, defaultValue);
    }

    /**
     * Creates a manager with explicit scheduling parameters (used by tests).
     *
     * @param database  the database whose tables are vacuumed
     * @param intervalMs auto-vacuum period in milliseconds; non-positive disables scheduling
     * @param batchSize rows scanned per locked batch; must be positive
     */
    public VacuumManager(Database database, long intervalMs, int batchSize) {
        if (database == null) {
            throw new IllegalArgumentException("Database must not be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("vacuum.batch.size must be positive");
        }
        this.database = database;
        this.intervalMs = intervalMs;
        this.batchSize = batchSize;
    }

    // ─── Public vacuum operations ───────────────────────────────────

    /**
     * Vacuums every registered table.
     *
     * @return status message with the reclaimed tuple count
     */
    public String vacuumAll() {
        registerMBean();
        long start = System.nanoTime();
        int tableCount = 0;
        long removed = 0;
        for (Table table : database.getTables()) {
            VacuumOutcome outcome = vacuumTableInternal(table);
            removed += outcome.removed;
            tableCount++;
        }
        long durationMs = finishRun(start, removed);
        return "VACUUM: " + tableCount + " table(s), " + removed
                + " dead tuples removed in " + durationMs + " ms";
    }

    /**
     * Vacuums a single table by name.
     *
     * @param tableName the table to vacuum
     * @return status message with the reclaimed tuple count
     * @throws TableNotFoundException if no such table exists
     */
    public String vacuumTable(String tableName) {
        return vacuumTable(database.getTable(tableName));
    }

    /**
     * Vacuums a single table: marks dead rows in bounded batches, then
     * reindexes and physically removes them in one compaction.
     *
     * @param table the table to vacuum
     * @return status message with the reclaimed tuple count
     */
    public String vacuumTable(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("Table must not be null");
        }
        registerMBean();
        long start = System.nanoTime();
        VacuumOutcome outcome = vacuumTableInternal(table);
        long durationMs = finishRun(start, outcome.removed);
        return "VACUUM " + table.getName() + ": " + outcome.removed
                + " dead tuples removed in " + durationMs + " ms ("
                + outcome.passes + " pass(es))";
    }

    /**
     * Starts the background auto-vacuum daemon. Repeated calls are no-ops
     * while the thread is running. A non-positive interval disables the
     * scheduler.
     */
    public void startAutoVacuum() {
        if (intervalMs <= 0) {
            LOGGER.info("Auto-vacuum disabled ({} <= 0)", INTERVAL_PROPERTY);
            return;
        }
        synchronized (lifecycleLock) {
            if (autoVacuumRunning) {
                return;
            }
            autoVacuumRunning = true;
            registerMBean();
            autoVacuumThread = new Thread(this::autoVacuumLoop, "diesel-vacuum");
            autoVacuumThread.setDaemon(true);
            autoVacuumThread.start();
            LOGGER.info("Auto-vacuum started: interval {} ms, batch size {}", intervalMs, batchSize);
        }
    }

    /**
     * Stops the auto-vacuum daemon (if running) and unregisters the JMX
     * MBean. Safe to call repeatedly.
     */
    public void stop() {
        Thread thread;
        synchronized (lifecycleLock) {
            autoVacuumRunning = false;
            thread = autoVacuumThread;
            autoVacuumThread = null;
            if (thread != null) {
                thread.interrupt();
            }
        }
        if (thread != null) {
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        unregisterMBean();
    }

    // ─── Vacuum core ────────────────────────────────────────────────

    private VacuumOutcome vacuumTableInternal(Table table) {
        int removed = 0;
        int passes = 0;
        boolean stable = false;
        while (passes < MAX_PASSES && !stable) {
            passes++;
            PassResult result = runPass(table);
            removed += result.removed;
            stable = result.stable;
        }
        if (!stable) {
            LOGGER.info("Vacuum of table {} yielded to concurrent changes after {} pass(es)",
                    table.getName(), passes);
        }
        return new VacuumOutcome(removed, passes);
    }

    private PassResult runPass(Table table) {
        long horizon = database.computeVacuumHorizonCsn();
        TxStatusTracker tracker = database.getTxStatusTracker();
        Set<Integer> pending = new HashSet<>();
        int rawBefore = table.getRawRowCount();
        int cursor = 0;
        while (cursor < rawBefore) {
            final int batchStart = cursor;
            final int batchEnd = Math.min(cursor + batchSize, rawBefore);
            boolean stable;
            try {
                stable = table.withWriteLock(() -> {
                    if (table.getRawRowCount() != rawBefore) {
                        return false;
                    }
                    for (int i = batchStart; i < batchEnd; i++) {
                        if (isDead(table, i, horizon, tracker)) {
                            if (!table.isDeleted(i)) {
                                table.markDeleted(i);
                            }
                            if (table.getRowVersionMeta(i) != null) {
                                table.removeRowVersionMeta(i);
                            }
                            pending.add(i);
                        }
                    }
                    return table.getRawRowCount() == rawBefore;
                });
            } catch (Exception e) {
                throw new IllegalStateException("Vacuum batch failed for table " + table.getName(), e);
            }
            if (!stable) {
                return new PassResult(0, false);
            }
            cursor = batchEnd;
        }

        try {
            return table.withWriteLock(() -> {
                if (table.getRawRowCount() != rawBefore) {
                    return new PassResult(0, false);
                }
                for (int pos : pending) {
                    if (pos >= table.getRawRowCount() || !table.isDeleted(pos)) {
                        return new PassResult(0, false);
                    }
                }
                int rawRowsBefore = table.getRawRowCount();
                if (table.getStorage() != null) {
                    table.getStorage().removeDeadEntries(pending);
                }
                table.compact();
                return new PassResult(rawRowsBefore - table.getRawRowCount(), true);
            });
        } catch (Exception e) {
            throw new IllegalStateException("Vacuum finalization failed for table " + table.getName(), e);
        }
    }

    /**
     * Evaluates the dead-row predicate for one raw row position. Kept
     * deliberately conservative: unknown or uncommitted state means alive.
     */
    private boolean isDead(Table table, int row, long horizon, TxStatusTracker tracker) {
        if (table.isDeleted(row)) {
            // MVCC tombstones stay alive while an open snapshot can still see
            // them (delete committed after the vacuum horizon); legacy
            // tombstones carry no metadata and are always reclaimable.
            RowVersionMeta deletedMeta = table.getRowVersionMeta(row);
            if (deletedMeta == null) {
                return true;
            }
            return !deletedMeta.hasUncommittedChanges()
                    && deletedMeta.getLastCommittedCsn() <= horizon;
        }
        RowVersionMeta meta = table.getRowVersionMeta(row);
        if (meta == null) {
            return false;
        }
        if (meta.hasUncommittedChanges()) {
            return false;
        }
        long xmin = meta.getXmin();
        if (xmin != 0 && tracker.getStatus(xmin) == TxStatusTracker.TxStatus.ABORTED) {
            return true;
        }
        long xmax = meta.getXmax();
        return xmax != 0 && tracker.isCommittedBefore(xmax, horizon);
    }

    private long finishRun(long startNanos, long removed) {
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        lastDurationMs = durationMs;
        lastRunEpochMs = System.currentTimeMillis();
        vacuumRuns.incrementAndGet();
        deadTuplesRemoved.addAndGet(removed);
        LOGGER.info("Vacuum run finished: {} dead tuples removed in {} ms", removed, durationMs);
        return durationMs;
    }

    private void autoVacuumLoop() {
        while (autoVacuumRunning) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!autoVacuumRunning) {
                break;
            }
            try {
                vacuumAll();
            } catch (Throwable t) {
                LOGGER.warn("Auto-vacuum run failed: {}", t.toString());
            }
        }
    }

    // ─── Metrics / accessors ────────────────────────────────────────

    /** Cumulative number of physically removed dead tuples. */
    public long getDeadTuplesRemoved() {
        return deadTuplesRemoved.get();
    }

    /** Number of completed vacuum runs. */
    public long getVacuumRuns() {
        return vacuumRuns.get();
    }

    /** Wall time of the most recent vacuum run in milliseconds. */
    public long getLastDurationMs() {
        return lastDurationMs;
    }

    /** Epoch milliseconds of the most recent vacuum run. */
    public long getLastRunEpochMs() {
        return lastRunEpochMs;
    }

    /** Returns whether the auto-vacuum daemon thread is running. */
    public boolean isAutoVacuumRunning() {
        return autoVacuumRunning;
    }

    /** Returns the auto-vacuum interval in milliseconds. */
    public long getIntervalMs() {
        return intervalMs;
    }

    /** Returns the batch size used by the mark phase. */
    public int getBatchSize() {
        return batchSize;
    }

    // ─── JMX (DynamicMBean, mirrors QueryProfiler) ──────────────────

    /**
     * Registers {@code diesel:type=VacuumManager,id=N} on the platform MBean
     * server on first use. The id suffix keeps multiple database instances in
     * one JVM independent.
     */
    private void registerMBean() {
        if (registeredName != null) {
            return;
        }
        synchronized (lifecycleLock) {
            if (registeredName != null) {
                return;
            }
            try {
                ObjectName name = new ObjectName(
                        OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
                ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
                registeredName = name;
            } catch (Exception e) {
                LOGGER.warn("Failed to register VacuumManager MBean: {}", e.toString());
            }
        }
    }

    private void unregisterMBean() {
        ObjectName name;
        synchronized (lifecycleLock) {
            name = registeredName;
            registeredName = null;
        }
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
        }
    }

    /**
     * Returns the JMX object name this manager is registered under, or
     * {@code null} when no vacuum has run yet.
     *
     * @return the registered object name, or null
     */
    public ObjectName getObjectName() {
        return registeredName;
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case ATTR_DURATION_MS:
                return lastDurationMs;
            case ATTR_DEAD_TUPLES:
                return deadTuplesRemoved.get();
            case ATTR_RUNS:
                return vacuumRuns.get();
            case ATTR_LAST_RUN_EPOCH_MS:
                return lastRunEpochMs;
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("VacuumManager attributes are read-only");
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
                "No operations on VacuumManager; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {ATTR_DURATION_MS, ATTR_DEAD_TUPLES, ATTR_RUNS, ATTR_LAST_RUN_EPOCH_MS};
        String[] descriptions = {
            "Wall time of the most recent vacuum run in milliseconds",
            "Cumulative number of physically removed dead tuples",
            "Number of completed vacuum runs",
            "Epoch milliseconds of the most recent vacuum run"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], "long", descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(VacuumManager.class.getName(),
                "DieselDB MVCC vacuum manager (dead row version reclamation)",
                attributes, null, null, null);
    }

    // ─── Internal result holders ────────────────────────────────────

    /** Outcome of one vacuum attempt on a table. */
    private static final class VacuumOutcome {
        final int removed;
        final int passes;

        VacuumOutcome(int removed, int passes) {
            this.removed = removed;
            this.passes = passes;
        }
    }

    /** Outcome of a single mark/finalize pass. */
    private static final class PassResult {
        final int removed;
        final boolean stable;

        PassResult(int removed, boolean stable) {
            this.removed = removed;
            this.stable = stable;
        }
    }
}
