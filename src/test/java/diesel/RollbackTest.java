package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance tests for prompt4.md step 2 (MVCC via row versioning):
 *
 * <ul>
 *   <li>BEGIN TRANSACTION on a 100k-row table completes in under 1 ms
 *       (the old Copy-on-Write clone took seconds).</li>
 *   <li>RollbackTest: 1000 operations inside one transaction, ROLLBACK
 *       restores the exact pre-transaction state without growing the heap.</li>
 * </ul>
 */
@Tag("concurrency")
class RollbackTest {

    private static final int ROW_COUNT = 100_000;
    private static final int OPS = 1_000;
    private static final int SEED = 10;

    @TempDir
    static Path tempDir;

    @Test
    void beginTransactionOnHundredThousandRowTableIsFasterThanOneMillisecond() throws Exception {
        Database db = new Database();
        db.setDataDir(tempDir.toString());
        db.executeQuery("CREATE TABLE BENCH_BEGIN (ID LONG PRIMARY KEY SEQUENCE(bench_begin_seq 1 1), VAL STRING)", null);

        Table bench = db.getTable("BENCH_BEGIN");
        List<Map<String, Object>> batch = new ArrayList<>(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", "v" + i);
            batch.add(row);
        }
        bench.bulkInsert(batch);
        assertEquals(ROW_COUNT, bench.getRawRowCount(), "test table must hold 100k rows");

        // Warmup + best-of-N: the minimum is robust against GC pauses and
        // scheduler noise on Windows; a CoW BEGIN would be seconds on every run.
        long bestNanos = Long.MAX_VALUE;
        for (int i = 0; i < 12; i++) {
            long start = System.nanoTime();
            String beginResult = (String) db.executeQuery("BEGIN TRANSACTION", null);
            long elapsed = System.nanoTime() - start;
            if (i == 0) {
                assertTrue(beginResult.startsWith("Transaction started:"),
                        "unexpected BEGIN result: " + beginResult);
            }
            UUID txId = UUID.fromString(beginResult.split(": ")[1]);
            db.executeQuery("ROLLBACK", txId);
            bestNanos = Math.min(bestNanos, elapsed);
        }

        assertTrue(bestNanos < 1_000_000L,
                "BEGIN TRANSACTION took " + (bestNanos / 1_000_000.0)
                        + " ms on a 100k-row table, expected < 1 ms");
    }

    @Test
    void rollbackAfterThousandOperationsRestoresStateWithoutHeapGrowth() throws Exception {
        Database db = new Database();
        db.setDataDir(tempDir.toString());
        db.executeQuery("CREATE TABLE ROLLBACK_OPS (ID LONG PRIMARY KEY SEQUENCE(rollback_ops_seq 1 1), VAL STRING)", null);
        for (int i = 0; i < SEED; i++) {
            db.executeQuery("INSERT INTO ROLLBACK_OPS (VAL) VALUES ('seed" + i + "')", null);
        }
        assertEquals(SEED, countRows(db, "SELECT ID FROM ROLLBACK_OPS", null),
                "seed rows must be committed before BEGIN");

        long heapBefore = usedHeapAfterGc();

        String beginResult = (String) db.executeQuery("BEGIN TRANSACTION", null);
        UUID txId = UUID.fromString(beginResult.split(": ")[1]);
        for (int i = 0; i < OPS; i++) {
            db.executeQuery("INSERT INTO ROLLBACK_OPS (VAL) VALUES ('op" + i + "')", txId);
        }

        // The transaction sees its own writes.
        assertEquals(SEED + OPS, countRows(db, "SELECT ID FROM ROLLBACK_OPS", txId),
                "own inserts must be visible inside the transaction");

        Transaction tx = db.getCurrentTransaction();
        assertNotNull(tx, "the active transaction must be resolvable");
        assertTrue(tx.getUndoLog().getMemoryUsage() < 2L * 1024 * 1024,
                "undo log must stay bounded by the 1MB spill threshold, was "
                        + tx.getUndoLog().getMemoryUsage() + " bytes");

        db.executeQuery("ROLLBACK", txId);

        assertEquals(SEED, countRows(db, "SELECT ID FROM ROLLBACK_OPS", null),
                "ROLLBACK must restore the exact row count");
        Object seedRows = db.executeQuery("SELECT VAL FROM ROLLBACK_OPS", null);
        assertTrue(seedRows instanceof List<?> list && list.size() == SEED,
                "expected " + SEED + " seed rows after ROLLBACK");
        for (Object row : (List<?>) seedRows) {
            Object val = ((Map<?, ?>) row).get("VAL");
            assertTrue(val != null && val.toString().startsWith("seed"),
                    "seed row must be untouched after ROLLBACK, got: " + row);
        }

        long heapAfter = usedHeapAfterGc();
        assertTrue(heapAfter - heapBefore < 32L * 1024 * 1024,
                "heap grew by " + (heapAfter - heapBefore)
                        + " bytes after 1000 operations and ROLLBACK, expected < 32MB");
    }

    private static int countRows(Database db, String sql, UUID txId) {
        try {
            Object result = db.executeQuery(sql, txId);
            if (result instanceof List<?> list) {
                return list.size();
            }
            return -1;
        } catch (Exception e) {
            throw new AssertionError("count query failed: " + sql, e);
        }
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
