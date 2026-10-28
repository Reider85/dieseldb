package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LOCK=NONE|SHARED|EXCLUSIVE acceptance for online ALTER (prompt4 #39,
 * R3-010 step 3): gate semantics (SHARED blocks writers, EXCLUSIVE also
 * blocks readers, timeout instead of a hang), statement-level LOCK parsing,
 * and the LockNoneTest criterion - parallel SELECTs observe a consistent
 * schema (old or new, never a mix) during an unblocked ALTER.
 *
 * {@code @Isolated}: the tests assert blocking windows against the
 * {@code alter.inplace.hold.ms} test hook and starve under class parallelism
 * (same rationale as CopyAlgorithmTest's wall-clock assert).
 */
@Tag("smoke")
@Isolated
@ResourceLock("diesel.alter.copy.config")
public class AlterLockTest {

    private static final String TABLE = "ALTER_LOCK_T";

    private Database database;
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("alter.copy.gate.timeout.ms");
        System.clearProperty("alter.inplace.hold.ms");
        if (database != null) {
            database.close();
        }
    }

    @Test
    void lockModesParseAndExecute() {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");

        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN C1 INTEGER ALGORITHM=INPLACE LOCK=NONE", null);
        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN C2 INTEGER ALGORITHM=INPLACE, LOCK=SHARED", null);
        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN C3 INTEGER LOCK=EXCLUSIVE", null);
        database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN C3 LOCK=NONE", null);
        List<String> columns = database.getTable(TABLE).getColumns();
        assertTrue(columns.contains("C1") && columns.contains("C2"));
        assertFalse(columns.contains("C3"));

        RuntimeException badLock = assertThrows(RuntimeException.class, () ->
                database.executeQuery(
                        "ALTER TABLE " + TABLE + " ADD COLUMN C4 INTEGER LOCK=WORLD", null));
        assertTrue(String.valueOf(badLock.getMessage()).contains("Unsupported LOCK mode"),
                "unexpected message: " + badLock.getMessage());
    }

    @Test
    void sharedGateBlocksWriterUntilRelease() throws Exception {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);

        table.beginAlterGate(diesel.schema.AlterLockMode.SHARED);
        AtomicBoolean writerDone = new AtomicBoolean(false);
        Thread writer = new Thread(() -> {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", 1);
            table.addRow(row);
            writerDone.set(true);
        }, "lock-gate-writer");
        writer.start();
        try {
            Thread.sleep(250);
            assertFalse(writerDone.get(), "writer must block while SHARED gate is armed");
        } finally {
            table.endAlterGate(diesel.schema.AlterLockMode.SHARED);
        }
        writer.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(writer.isAlive(), "writer must proceed once the gate is released");
        assertTrue(writerDone.get());
        assertEquals(1, table.rowCount());
    }

    @Test
    void exclusiveGateBlocksReaderUntilRelease() throws Exception {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);

        table.beginAlterGate(diesel.schema.AlterLockMode.EXCLUSIVE);
        AtomicBoolean readerDone = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            table.awaitAlterReadersUnblocked();
            readerDone.set(true);
        }, "lock-gate-reader");
        reader.start();
        try {
            Thread.sleep(250);
            assertFalse(readerDone.get(), "reader must block while EXCLUSIVE gate is armed");
        } finally {
            table.endAlterGate(diesel.schema.AlterLockMode.EXCLUSIVE);
        }
        reader.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(reader.isAlive(), "reader must proceed once the gate is released");
        assertTrue(readerDone.get());
    }

    @Test
    void gateWaitTimesOutInsteadOfHangingForever() {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);
        System.setProperty("alter.copy.gate.timeout.ms", "150");
        try {
            table.beginAlterGate(diesel.schema.AlterLockMode.SHARED);
            long t0 = System.nanoTime();
            DieselException e = assertThrows(DieselException.class,
                    table::awaitAlterWritersUnblocked);
            long waitedMs = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(String.valueOf(e.getMessage()).contains("Timed out waiting for ALTER LOCK"),
                    "unexpected message: " + e.getMessage());
            assertTrue(waitedMs >= 100, "waited only " + waitedMs + " ms, expected the 150 ms timeout");
        } finally {
            table.endAlterGate(diesel.schema.AlterLockMode.SHARED);
        }
        // The gate is fully released: the fast path must not throw anymore.
        table.awaitAlterWritersUnblocked();
    }

    @Test
    void sharedLockStatementBlocksInsertsForItsWindow() throws Exception {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);
        System.setProperty("alter.inplace.hold.ms", "600");

        Thread alter = new Thread(() -> database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN Z INTEGER ALGORITHM=INPLACE LOCK=SHARED",
                null), "lock-alter-shared");
        alter.start();
        awaitGateArmed(table, 5_000);

        long t0 = System.nanoTime();
        Map<String, Object> row = new HashMap<>();
        row.put("ID", 42);
        table.addRow(row);
        long insertMs = (System.nanoTime() - t0) / 1_000_000;

        alter.join(TimeUnit.SECONDS.toMillis(15));
        assertFalse(alter.isAlive(), "ALTER statement did not finish");
        assertTrue(insertMs >= 200,
                "insert was not blocked by LOCK=SHARED (only " + insertMs + " ms, expected ~600 ms)");
        assertTrue(table.getColumns().contains("Z"));
        assertEquals(1, table.rowCount());
    }

    @Test
    void exclusiveLockStatementBlocksSelectsForItsWindow() throws Exception {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);
        System.setProperty("alter.inplace.hold.ms", "600");

        Thread alter = new Thread(() -> database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN Z INTEGER ALGORITHM=INPLACE LOCK=EXCLUSIVE",
                null), "lock-alter-exclusive");
        alter.start();
        awaitGateArmed(table, 5_000);

        long t0 = System.nanoTime();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        long selectMs = (System.nanoTime() - t0) / 1_000_000;

        alter.join(TimeUnit.SECONDS.toMillis(15));
        assertFalse(alter.isAlive(), "ALTER statement did not finish");
        assertTrue(selectMs >= 200,
                "SELECT was not blocked by LOCK=EXCLUSIVE (only " + selectMs + " ms, expected ~600 ms)");
        assertNotNull(rows);
        assertTrue(table.getColumns().contains("Z"));
    }

    @Test
    void noneLockDoesNotBlockDml() throws Exception {
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        Table table = database.getTable(TABLE);
        System.setProperty("alter.inplace.hold.ms", "500");

        AtomicBoolean alterFinished = new AtomicBoolean(false);
        Thread alter = new Thread(() -> {
            database.executeQuery(
                    "ALTER TABLE " + TABLE + " ADD COLUMN Z INTEGER ALGORITHM=INPLACE LOCK=NONE",
                    null);
            alterFinished.set(true);
        }, "lock-alter-none");
        alter.start();

        // LOCK=NONE never arms a gate: the insert must not wait out the hold.
        long t0 = System.nanoTime();
        Map<String, Object> row = new HashMap<>();
        row.put("ID", 7);
        table.addRow(row);
        long insertMs = (System.nanoTime() - t0) / 1_000_000;

        alter.join(TimeUnit.SECONDS.toMillis(15));
        assertFalse(alter.isAlive(), "ALTER statement did not finish");
        assertTrue(insertMs < 300,
                "LOCK=NONE must not block the writer, but the insert took " + insertMs + " ms"
                        + " (alterFinished before insert: " + alterFinished.get() + ")");
        assertEquals(1, table.rowCount());
    }

    /**
     * LockNoneTest (prompt4 #39 acceptance): 100 parallel SELECTs during an
     * unblocked INPLACE DROP observe a consistent schema - every result either
     * fully contains the column or fully omits it, never a mix.
     */
    @Test
    void lockNoneReadersSeeConsistentSchema() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < 500; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("NAME", "n-" + i);
            table.addRow(row);
        }
        System.setProperty("alter.inplace.hold.ms", "400");

        int readers = 10;
        int readsPerReader = 10;
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        AtomicBoolean dropFinished = new AtomicBoolean(false);
        List<Thread> threads = new ArrayList<>();

        for (int r = 0; r < readers; r++) {
            Thread reader = new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < readsPerReader; i++) {
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> rows = (List<Map<String, Object>>)
                                database.executeQuery("SELECT * FROM " + TABLE, null);
                        if (rows.isEmpty()) {
                            continue;
                        }
                        long withColumn = rows.stream().filter(x -> x.containsKey("NAME")).count();
                        if (withColumn != 0 && withColumn != rows.size()) {
                            violations.add("mixed schema result: " + withColumn + "/"
                                    + rows.size() + " rows carry NAME");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    violations.add("reader interrupted");
                } catch (RuntimeException e) {
                    violations.add("reader failed: " + e);
                }
            }, "lock-none-reader-" + r);
            threads.add(reader);
            reader.start();
        }

        Thread alter = new Thread(() -> {
            try {
                start.await(10, TimeUnit.SECONDS);
                database.executeQuery(
                        "ALTER TABLE " + TABLE + " DROP COLUMN NAME ALGORITHM=INPLACE LOCK=NONE",
                        null);
                dropFinished.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                violations.add("ALTER interrupted");
            } catch (RuntimeException e) {
                violations.add("ALTER failed: " + e);
            }
        }, "lock-none-alter");
        threads.add(alter);
        alter.start();

        start.countDown();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(t.isAlive(), "thread did not finish: " + t.getName());
        }
        assertTrue(dropFinished.get(), "DROP COLUMN did not finish");
        assertTrue(violations.isEmpty(), "schema consistency violations: " + violations);

        // Final state: the column is gone for everyone.
        assertFalse(table.getColumns().contains("NAME"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> finalRows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        assertEquals(500, finalRows.size());
        assertTrue(finalRows.stream().noneMatch(x -> x.containsKey("NAME")),
                "SELECT * leaked the dropped column after the ALTER completed");
    }

    // ─── helpers ───────────────────────────────────────────────────────

    private void awaitGateArmed(Table table, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (table.isAlterGateArmed()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("ALTER never armed its LOCK gate within " + timeoutMs + " ms");
    }
}
