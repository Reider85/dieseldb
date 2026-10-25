package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OnlineAlterTest (prompt4 #39 acceptance): 1 writer + 1 online ALTER
 * (ALGORITHM=COPY DROP COLUMN) + 10 readers all finish successfully - the
 * writers keep committing through the copy's barrier, readers keep observing
 * a consistent schema, and the final state is coherent (row count includes
 * every writer insert, the dropped column is gone for everyone).
 */
@Tag("smoke")
@ResourceLock("diesel.alter.copy.config")
public class OnlineAlterTest {

    private static final String TABLE = "ONLINE_ALTER_T";
    // 2k rows: the fast profile runs on a 512m heap and a 4-core box - the
    // acceptance shape (1 writer + 1 alter + 10 readers) matters, not volume.
    private static final int ROWS = 2_000;
    private static final int READERS = 10;
    private static final int READS_PER_READER = 400;
    private static final long READER_PAUSE_MS = 30;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
        System.setProperty("alter.copy.batch.size", "100");
        System.setProperty("alter.copy.batch.sleep.ms", "5");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("alter.copy.batch.size");
        System.clearProperty("alter.copy.batch.sleep.ms");
        if (database != null) {
            database.close();
        }
    }

    @Test
    void oneWriterOneAlterTenReadersAllComplete() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < ROWS; i++) {
            table.addRow(row(i, "base-" + i));
        }

        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();
        AtomicBoolean alterStarted = new AtomicBoolean(false);
        AtomicBoolean stopWorkers = new AtomicBoolean(false);
        CountDownLatch writerWarmed = new CountDownLatch(1);
        AtomicInteger writerInserted = new AtomicInteger();

        // 1 writer: keeps inserting through the copy and past the swap (pacing sleep
        // keeps the 512m fast-profile heap and the 4-core box from drowning).
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                table.addRow(row(1_000_000 + i, "warm-" + i));
            }
            writerWarmed.countDown();
            try {
                while (!alterStarted.get()) {
                    Thread.sleep(1);
                }
                while (!stopWorkers.get() && writerInserted.get() < 300) {
                    int id = 2_000_000 + writerInserted.getAndIncrement();
                    table.addRow(row(id, "during-" + id));
                    Thread.sleep(2);
                }
            } catch (RuntimeException e) {
                failures.add("writer: " + e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "online-writer");

        // 10 readers: SELECT * must never throw while the schema swaps. Paced
        // reads (not a tight loop) so CPU contention cannot starve the copy -
        // an unpaced variant OOM'd the 512m heap on this machine.
        AtomicInteger reads = new AtomicInteger();
        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < READERS; r++) {
            Thread reader = new Thread(() -> {
                int localReads = 0;
                try {
                    while (!stopWorkers.get() && localReads < READS_PER_READER) {
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> rows = (List<Map<String, Object>>)
                                database.executeQuery("SELECT * FROM " + TABLE, null);
                        // Consistency: a result never mixes - either every row
                        // carries the dropped column or none does.
                        if (!rows.isEmpty()) {
                            long withName = rows.stream().filter(x -> x.containsKey("NAME")).count();
                            if (withName != 0 && withName != rows.size()) {
                                failures.add("reader: mixed result " + withName + "/" + rows.size());
                            }
                        }
                        localReads++;
                        reads.incrementAndGet();
                        Thread.sleep(READER_PAUSE_MS);
                    }
                } catch (RuntimeException e) {
                    failures.add("reader: " + e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "online-reader-" + r);
            readers.add(reader);
        }

        writer.start();
        assertTrue(writerWarmed.await(30, TimeUnit.SECONDS), "writer warm-up timed out");
        for (Thread reader : readers) {
            reader.start();
        }

        // 1 online ALTER: background DROP COLUMN rewrite.
        alterStarted.set(true);
        database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN NAME ALGORITHM=COPY", null);
        Map<String, Object> completed = awaitAlterState("COMPLETED", 120_000);

        stopWorkers.set(true);
        writer.join(TimeUnit.SECONDS.toMillis(30));
        for (Thread reader : readers) {
            reader.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(reader.isAlive(), "reader did not finish: " + reader.getName());
        }
        assertFalse(writer.isAlive(), "writer did not finish");

        assertTrue(failures.isEmpty(), "worker failures: " + failures);
        assertTrue(reads.get() > 0, "readers never observed a result");
        assertEquals(100, (int) completed.get("PROGRESS_PERCENT"));

        // Final state: every writer insert landed, the column is gone.
        int expected = ROWS + 200 + writerInserted.get();
        assertEquals(expected, table.rowCount());
        assertFalse(table.getColumns().contains("NAME"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> finalRows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        assertEquals(expected, finalRows.size());
        assertTrue(finalRows.stream().noneMatch(r -> r.containsKey("NAME")),
                "dropped column leaked into the final SELECT *");

        // Post-swap inserts work and no longer carry the dropped column.
        database.executeQuery(
                "INSERT INTO " + TABLE + " (ID) VALUES (99999991)", null);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tail = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE + " WHERE ID = 99999991", null);
        assertEquals(1, tail.size());
        assertFalse(tail.get(0).containsKey("NAME"));
    }

    // ─── helpers ───────────────────────────────────────────────────────

    private Map<String, Object> row(int id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("ID", id);
        row.put("NAME", name);
        return row;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> alterStatus() {
        return (List<Map<String, Object>>) database.executeQuery("ALTER STATUS", null);
    }

    private Map<String, Object> awaitAlterState(String state, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Map<String, Object> row : alterStatus()) {
                if (TABLE.equals(row.get("TABLE_NAME")) && state.equals(row.get("STATE"))) {
                    return row;
                }
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for ALTER state " + state + "; status=" + alterStatus());
        return null;
    }
}
