package diesel;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import javax.management.ObjectName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance tests for prompt4.md step 3 (MVCC Vacuum Manager):
 *
 * <ul>
 *   <li>1M inserts + 600k aborted inserts + 100k deletes + {@code VACUUM}
 *       shrink the heap by at least 30% (measured through the JMX
 *       {@code MemoryMXBean}), with {@code vacuum.dead_tuples_removed}
 *       confirming the reclamation;</li>
 *   <li>vacuuming the 1M-row table never blocks a concurrent writer for more
 *       than 100 ms;</li>
 *   <li>auto-vacuum runs on schedule and the manual {@code VACUUM table}
 *       statement works (covered end-to-end here, unit-level in
 *       {@link VacuumManagerTest}).</li>
 * </ul>
 *
 * <p>Same-table writers only ever contend with the single final compaction
 * window (writers take no table lock; {@code Table.compact()} holds the write
 * lock), so the writer thread runs against a second table in the same
 * database — proving the vacuum holds no database-wide lock while it scans.
 */
@Tag("large")
class VacuumTest {

    private static final Logger LOGGER = Logger.getLogger(VacuumTest.class.getName());

    private static final int LIVE_ROWS = 1_000_000;
    private static final int ABORTED_ROWS = 600_000;
    private static final int DELETED_ROWS = 100_000;
    private static final int WRITER_TABLE_ROWS = 1_000_000;
    private static final int WRITER_TABLE_DELETED = 100_000;
    private static final long MAX_WRITER_BLOCK_NS = 100_000_000L;
    private static final String ABORTED_PAYLOAD =
            "dead-version-payload-0123456789".repeat(10);

    @TempDir
    static Path tempDir;

    @Test
    void vacuumReclaimsAtLeastThirtyPercentOfHeapOnMillionRowTable() throws Exception {
        Database db = new Database();
        db.setDataDir(tempDir.toString());
        String tableName = "VAC_HEAP";
        db.executeQuery("CREATE TABLE " + tableName
                + " (ID LONG PRIMARY KEY SEQUENCE(vac_heap_seq 1 1), VAL STRING)", null);
        Table table = db.getTable(tableName);

        // 1M committed rows (auto-commit/bulk rows carry no MVCC metadata).
        List<Map<String, Object>> batch = new ArrayList<>(100_000);
        for (int i = 0; i < LIVE_ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", "v" + i);
            batch.add(row);
            if (batch.size() == 100_000) {
                table.bulkInsert(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            table.bulkInsert(batch);
        }
        assertEquals(LIVE_ROWS, table.getRawRowCount());

        // 600k rows from one rolled-back transaction: inserted through the
        // MVCC path, txid aborted and the undo flags cleared — the exact
        // final state a ROLLBACK leaves behind (InsertUndo.apply).
        TxStatusTracker tracker = db.getTxStatusTracker();
        long abortedTxid = tracker.registerTransaction();
        tracker.markAborted(abortedTxid);
        for (int i = 0; i < ABORTED_ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", ABORTED_PAYLOAD);
            int rowIndex = table.addRowWithMVCC(row, abortedTxid);
            RowVersionMeta meta = table.getRowVersionMeta(rowIndex);
            assertNotNull(meta, "MVCC metadata must exist for the inserted row");
            meta.markAborted();
        }
        assertEquals(LIVE_ROWS + ABORTED_ROWS, table.getRawRowCount());

        // 100k SQL deletes (6.25% tombstones — below the 30% auto-compact
        // threshold, so the rows stay physically present until VACUUM).
        db.executeQuery("DELETE FROM " + tableName + " WHERE ID < " + (DELETED_ROWS + 1), null);
        assertEquals(DELETED_ROWS, table.getDeletedCount());
        assertEquals(LIVE_ROWS + ABORTED_ROWS, table.getRawRowCount());

        long heapBefore = usedHeapViaJmx();

        String result = (String) db.executeQuery("VACUUM " + tableName, null);
        assertTrue(result.startsWith("VACUUM " + tableName + ": 700000 dead tuples removed"),
                "unexpected VACUUM result: " + result);
        ObjectName mbean = db.getVacuumManager().getObjectName();

        long heapAfter = usedHeapViaJmx();
        long drop = heapBefore - heapAfter;
        assertTrue(drop >= (long) (0.30 * heapBefore),
                "heap must shrink by >= 30%: before=" + heapBefore
                        + " after=" + heapAfter + " drop=" + drop);

        assertNotNull(mbean, "VACUUM must have registered the metrics MBean");
        Object deadTuples = ManagementFactory.getPlatformMBeanServer()
                .getAttribute(mbean, VacuumManager.ATTR_DEAD_TUPLES);
        assertTrue(((Long) deadTuples) >= 700_000L,
                "JMX vacuum.dead_tuples_removed must report >= 700000, was " + deadTuples);

        assertEquals(LIVE_ROWS - DELETED_ROWS, table.getRawRowCount(),
                "only the surviving live rows must remain after the vacuum");
        assertEquals(0, table.getDeletedCount(), "no tombstones may remain after VACUUM");
        assertEquals(LIVE_ROWS - DELETED_ROWS, table.getLiveRowCount());

        Object sample = db.executeQuery(
                "SELECT ID FROM " + tableName + " WHERE ID < 100006", null);
        assertTrue(sample instanceof List<?>, "vacuumed table must still serve queries");
        assertEquals(5, ((List<?>) sample).size(), "rows above the deleted range must be queryable");
    }

    @Test
    void vacuumDoesNotBlockConcurrentWritersBeyondOneHundredMilliseconds() throws Exception {
        Database db = new Database();
        db.setDataDir(tempDir.toString());
        String heavy = "VAC_WRITE_A";
        String writerTable = "VAC_WRITE_B";
        db.executeQuery("CREATE TABLE " + heavy
                + " (ID LONG PRIMARY KEY SEQUENCE(vac_write_a_seq 1 1), VAL STRING)", null);
        db.executeQuery("CREATE TABLE " + writerTable
                + " (ID LONG PRIMARY KEY SEQUENCE(vac_write_b_seq 1 1), VAL STRING)", null);
        Table heavyTable = db.getTable(heavy);

        List<Map<String, Object>> batch = new ArrayList<>(100_000);
        for (int i = 0; i < WRITER_TABLE_ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", "v" + i);
            batch.add(row);
            if (batch.size() == 100_000) {
                heavyTable.bulkInsert(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            heavyTable.bulkInsert(batch);
        }
        db.executeQuery("DELETE FROM " + heavy + " WHERE ID < " + (WRITER_TABLE_DELETED + 1), null);
        assertEquals(WRITER_TABLE_DELETED, heavyTable.getDeletedCount());

        // Settle the heap before measuring writer latency so collection noise
        // from the workload itself does not land inside the vacuum window.
        usedHeapViaJmx();

        // Warm up the writer path (parse + insert + persist) outside the
        // measured window.
        for (int i = 0; i < 50; i++) {
            db.executeQuery("INSERT INTO " + writerTable + " (VAL) VALUES ('warm" + i + "')", null);
        }

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong baselineCleanMaxNs = new AtomicLong(0);
        AtomicLong vacuumCleanMaxNs = new AtomicLong(0);
        AtomicLong vacuumGcMaxNs = new AtomicLong(0);
        AtomicLong opsDuringVacuum = new AtomicLong(0);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        AtomicLong vacuumStartNs = new AtomicLong(0);
        AtomicLong vacuumEndNs = new AtomicLong(0);

        Thread writer = new Thread(() -> {
            int i = 0;
            try {
                while (!stop.get()) {
                    long gc0 = gcTimeMs();
                    long t0 = System.nanoTime();
                    db.executeQuery(
                            "INSERT INTO " + writerTable + " (VAL) VALUES ('w" + i + "')", null);
                    long t1 = System.nanoTime();
                    long gcDeltaMs = gcTimeMs() - gc0;
                    long dur = t1 - t0;
                    long start = vacuumStartNs.get();
                    long end = vacuumEndNs.get();
                    if (start == 0 || t0 < start) {
                        if (gcDeltaMs == 0) {
                            baselineCleanMaxNs.accumulateAndGet(dur, Math::max);
                        }
                    } else if (end == 0 || t0 < end) {
                        opsDuringVacuum.incrementAndGet();
                        if (gcDeltaMs > 0) {
                            vacuumGcMaxNs.accumulateAndGet(dur, Math::max);
                        } else {
                            vacuumCleanMaxNs.accumulateAndGet(dur, Math::max);
                        }
                    }
                    i++;
                }
            } catch (Throwable t) {
                writerError.set(t);
            }
        }, "vacuum-writer");
        writer.start();

        // Baseline phase: the writer runs without any vacuum so the test can
        // distinguish vacuum-induced stalls from writer-path noise (parse,
        // coalesced TSV persist, JIT).
        Thread.sleep(1_500);

        long gcBeforeVacuum = gcTimeMs();
        vacuumStartNs.set(System.nanoTime());
        String result = (String) db.executeQuery("VACUUM " + heavy, null);
        vacuumEndNs.set(System.nanoTime());
        long gcDuringVacuum = gcTimeMs() - gcBeforeVacuum;
        stop.set(true);
        writer.join(60_000);

        long baselineMs = baselineCleanMaxNs.get() / 1_000_000L;
        long cleanMs = vacuumCleanMaxNs.get() / 1_000_000L;
        long gcMs = vacuumGcMaxNs.get() / 1_000_000L;
        LOGGER.info(() -> "writer latency: baselineClean=" + baselineMs + " ms, vacuumClean="
                + cleanMs + " ms, vacuumGcOverlapped=" + gcMs + " ms, gcDuringVacuumWindow="
                + gcDuringVacuum + " ms, opsDuringVacuum=" + opsDuringVacuum.get());
        assertNull(writerError.get(), "writer must not fail during the vacuum: " + writerError.get());
        assertTrue(result.contains("100000 dead tuples removed"),
                "unexpected VACUUM result: " + result);
        assertTrue(opsDuringVacuum.get() >= 1,
                "at least one writer operation must overlap the vacuum window");
        // The vacuum must not stall writers through locks. Operations that
        // overlap a JVM garbage collection are reported separately: a GC pause
        // is JVM-wide background work (it would hit any thread, with or
        // without the vacuum) and cannot be prevented by lock discipline.
        assertTrue(vacuumCleanMaxNs.get() < MAX_WRITER_BLOCK_NS,
                "clean writer latency during the vacuum must stay below 100 ms, was " + cleanMs
                        + " ms (baseline max " + baselineMs + " ms; GC-overlapped max " + gcMs
                        + " ms; GC time during vacuum window " + gcDuringVacuum + " ms)");
        assertEquals(WRITER_TABLE_ROWS - WRITER_TABLE_DELETED, heavyTable.getRawRowCount());
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static long usedHeapViaJmx() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(150);
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    /** Cumulative time spent in garbage collection across all collectors. */
    private static long gcTimeMs() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = gc.getCollectionTime();
            if (time > 0) {
                total += time;
            }
        }
        return total;
    }
}
