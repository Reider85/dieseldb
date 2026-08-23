package diesel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CopyOnWriteIsolationTest {

    private UUID beginTransaction(Database db) {
        String result = (String) db.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    @Test
    void transactionInsertDoesNotLeakToSharedTable(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE users (id LONG PRIMARY KEY, name STRING)", null);
        db.executeQuery("INSERT INTO users (id, name) VALUES (1, 'Alice')", null);

        UUID txId = beginTransaction(db);
        db.executeQuery("INSERT INTO users (id, name) VALUES (2, 'Bob')", txId);

        List<?> outside = (List<?>) db.executeQuery("SELECT * FROM users", null);
        assertEquals(1, outside.size(), "Uncommitted insert must not leak to shared table");

        List<?> inside = (List<?>) db.executeQuery("SELECT * FROM users", txId);
        assertEquals(2, inside.size(), "Transaction should see its own insert");

        db.executeQuery("ROLLBACK", txId);

        List<?> afterRollback = (List<?>) db.executeQuery("SELECT * FROM users", null);
        assertEquals(1, afterRollback.size(), "Rolled-back insert must not persist");
    }

    @Test
    void transactionUpdateDoesNotLeakToSharedTable(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE users (id LONG PRIMARY KEY, name STRING)", null);
        db.executeQuery("INSERT INTO users (id, name) VALUES (1, 'Alice')", null);

        UUID txId = beginTransaction(db);
        db.executeQuery("UPDATE users SET name = 'Bob' WHERE id = 1", txId);

        List<?> outside = (List<?>) db.executeQuery("SELECT * FROM users", null);
        Map<?, ?> row = (Map<?, ?>) outside.get(0);
        assertEquals("Alice", row.get("name"), "Uncommitted update must not leak");

        List<?> inside = (List<?>) db.executeQuery("SELECT * FROM users", txId);
        Map<?, ?> txRow = (Map<?, ?>) inside.get(0);
        assertEquals("Bob", txRow.get("name"), "Transaction should see its own update");

        db.executeQuery("COMMIT", txId);

        List<?> afterCommit = (List<?>) db.executeQuery("SELECT * FROM users", null);
        Map<?, ?> committedRow = (Map<?, ?>) afterCommit.get(0);
        assertEquals("Bob", committedRow.get("name"), "Committed update should be visible");
    }

    @Test
    void transactionDeleteDoesNotLeakToSharedTable(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE users (id LONG PRIMARY KEY, name STRING)", null);
        db.executeQuery("INSERT INTO users (id, name) VALUES (1, 'Alice')", null);
        db.executeQuery("INSERT INTO users (id, name) VALUES (2, 'Bob')", null);

        UUID txId = beginTransaction(db);
        db.executeQuery("DELETE FROM users WHERE id = 1", txId);

        List<?> outside = (List<?>) db.executeQuery("SELECT * FROM users", null);
        assertEquals(2, outside.size(), "Uncommitted delete must not leak");

        List<?> inside = (List<?>) db.executeQuery("SELECT * FROM users", txId);
        assertEquals(1, inside.size(), "Transaction should see its own delete");

        db.executeQuery("ROLLBACK", txId);

        List<?> afterRollback = (List<?>) db.executeQuery("SELECT * FROM users", null);
        assertEquals(2, afterRollback.size(), "Rolled-back delete must not persist");
    }

    @Test
    void beginDoesNotCopyTablesEagerly(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        for (int t = 0; t < 10; t++) {
            db.executeQuery("CREATE TABLE t" + t + " (id LONG PRIMARY KEY, val STRING)", null);
            for (int i = 0; i < 100; i++) {
                db.executeQuery("INSERT INTO t" + t + " (id, val) VALUES (" + i + ", 'v" + i + "')", null);
            }
        }

        long start = System.nanoTime();
        UUID txId = beginTransaction(db);
        long elapsed = System.nanoTime() - start;

        assertTrue(elapsed < 50_000_000,
                "BEGIN should be fast with lazy snapshots, took " + (elapsed / 1_000_000) + "ms");

        db.executeQuery("ROLLBACK", txId);
    }
}
