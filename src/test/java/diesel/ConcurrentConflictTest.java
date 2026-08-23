package diesel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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
        db.executeQuery("UPDATE accounts SET balance = 800 WHERE id = 1", txB);

        db.executeQuery("COMMIT", txA);

        assertThrows(TransactionException.class, () -> db.executeQuery("COMMIT", txB));
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
}
