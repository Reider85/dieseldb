package diesel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Optimistic write-write conflict semantics (prompt4.md #4): the second
 * writer on the same row fails fast with {@link TransactionException} at its
 * UPDATE/DELETE — not at COMMIT — while independent transactions and
 * per-transaction row ownership commit without deadlocks.
 */
@Tag("concurrency")
class ConcurrentConflictTest {

    private UUID beginTransaction(Database db) {
        String result = (String) db.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    @Test
    void concurrentWriteWriteConflictDetected(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE accounts (id LONG PRIMARY KEY, balance LONG)", null);
        db.executeQuery("INSERT INTO accounts (id, balance) VALUES (1, 1000)", null);

        UUID txA = beginTransaction(db);
        UUID txB = beginTransaction(db);

        db.executeQuery("UPDATE accounts SET balance = 900 WHERE id = 1", txA);
        // txB touches the row txA holds pending: fails at the writer, txA unaffected
        assertThrows(TransactionException.class,
                () -> db.executeQuery("UPDATE accounts SET balance = 800 WHERE id = 1", txB));

        assertDoesNotThrow(() -> db.executeQuery("COMMIT", txA));
        // rolled-back loser: txB's failed UPDATE left nothing behind
        assertDoesNotThrow(() -> db.executeQuery("ROLLBACK", txB));

        List<Map<String, Object>> rows = selectAccounts(db);
        assertEquals(1, rows.size(), "row must survive the conflicting pair");
        assertEquals(900L, getLong(rows.get(0), "balance"),
                "winner's value must be committed");
    }

    @Test
    void nonConflictingTransactionsCommitSuccessfully(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE t1 (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("CREATE TABLE t2 (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO t1 (id, val) VALUES (1, 'a')", null);
        db.executeQuery("INSERT INTO t2 (id, val) VALUES (1, 'b')", null);

        UUID txA = beginTransaction(db);
        UUID txB = beginTransaction(db);

        db.executeQuery("UPDATE t1 SET val = 'x' WHERE id = 1", txA);
        db.executeQuery("UPDATE t2 SET val = 'y' WHERE id = 1", txB);

        assertDoesNotThrow(() -> db.executeQuery("COMMIT", txA));
        assertDoesNotThrow(() -> db.executeQuery("COMMIT", txB));
    }

    @Test
    void hundredWritersOnOwnRowsCommitWithoutConflict(@TempDir Path tempDir) throws Exception {
        final int writers = 100;
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE acc (id LONG PRIMARY KEY, balance LONG)", null);
        for (int i = 1; i <= writers; i++) {
            db.executeQuery("INSERT INTO acc (id, balance) VALUES (" + i + ", 0)", null);
        }

        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch start = new CountDownLatch(1);
        List<Throwable> failures = new ArrayList<>();
        try {
            for (int i = 1; i <= writers; i++) {
                final int id = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        UUID tx = beginTransaction(db);
                        db.executeQuery("UPDATE acc SET balance = " + id + " WHERE id = " + id, tx);
                        db.executeQuery("COMMIT", tx);
                    } catch (Throwable t) {
                        t.printStackTrace(System.err);
                        synchronized (failures) {
                            failures.add(t);
                        }
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(2, TimeUnit.MINUTES), "writers must finish in time");
        } finally {
            pool.shutdownNow();
        }

        assertTrue(failures.isEmpty(),
                "own-row transactions must not conflict or deadlock: " + failures);

        List<Map<String, Object>> rows = selectAcc(db);
        assertEquals(writers, rows.size(), "every writer's row must be committed");
        for (Map<String, Object> row : rows) {
            long id = getLong(row, "id");
            long balance = getLong(row, "balance");
            assertEquals(id, balance, "writer " + id + " must see its own committed value");
        }
    }

    private List<Map<String, Object>> selectAccounts(Database db) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows =
                (List<Map<String, Object>>) db.executeQuery("SELECT * FROM accounts", null);
        return rows;
    }

    private List<Map<String, Object>> selectAcc(Database db) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows =
                (List<Map<String, Object>>) db.executeQuery("SELECT * FROM acc", null);
        return rows;
    }

    private static long getLong(Map<String, Object> row, String column) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(column)) {
                return ((Number) entry.getValue()).longValue();
            }
        }
        throw new AssertionError("column " + column + " not found in " + row.keySet());
    }
}
