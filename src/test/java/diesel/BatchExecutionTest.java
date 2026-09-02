package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL-based batch execution using BEGIN BATCH / END BATCH syntax.
 * This tests the optimization where index updates are deferred until the end
 * of the batch, improving performance for bulk operations.
 */
class BatchExecutionTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable("BATCH_EXEC_TEST");
        } catch (TableNotFoundException ignored) {
            // table does not exist yet
        }
        try {
            database.dropTable("OTHER_TABLE");
        } catch (TableNotFoundException ignored) {
            // table does not exist yet
        }
    }

    @Test
    void beginBatchShouldStartTransaction() {
        Object result = database.executeQuery("BEGIN BATCH", null);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Batch started:"));
    }

    @Test
    void endBatchWithoutBeginShouldFail() {
        assertThrows(TransactionException.class, () ->
            database.executeQuery("END BATCH", null));
    }

    @Test
    void batchWithMultipleInsertsShouldWork() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING)", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));

        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('Alice')", transactionId);
        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('Bob')", transactionId);
        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('Charlie')", transactionId);

        Object endResult = database.executeQuery("END BATCH", transactionId);
        assertEquals("Batch committed", endResult);

        List<?> rows = (List<?>) database.executeQuery("SELECT * FROM BATCH_EXEC_TEST", null);
        assertEquals(3, rows.size());
    }

    @Test
    void batchWithMixedDmlShouldWork() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING, VALUE INTEGER)", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));

        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME, VALUE) VALUES ('Alice', 100)", transactionId);
        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME, VALUE) VALUES ('Bob', 200)", transactionId);
        database.executeQuery("UPDATE BATCH_EXEC_TEST SET VALUE = 150 WHERE NAME = 'Alice'", transactionId);
        database.executeQuery("DELETE FROM BATCH_EXEC_TEST WHERE NAME = 'Bob'", transactionId);

        Object endResult = database.executeQuery("END BATCH", transactionId);
        assertEquals("Batch committed", endResult);

        List<?> rows = (List<?>) database.executeQuery("SELECT * FROM BATCH_EXEC_TEST", null);
        assertEquals(1, rows.size());
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals("Alice", row.get("NAME"));
        assertEquals(150, ((Number) row.get("VALUE")).intValue());
    }

    @Test
    void batchShouldRollbackOnError() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON BATCH_EXEC_TEST (NAME)", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));

        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('Alice')", transactionId);

        // With deferred indexes, the duplicate is allowed at INSERT time but detected at END BATCH
        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('Alice')", transactionId);

        // END BATCH should fail because the deferred index rebuild detects the duplicate
        assertThrows(TransactionException.class, () ->
            database.executeQuery("END BATCH", transactionId));

        List<?> rows = (List<?>) database.executeQuery("SELECT * FROM BATCH_EXEC_TEST", null);
        assertEquals(0, rows.size());
    }

    @Test
    void batchWithIndexShouldDeferIndexUpdates() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING)", null);
        database.executeQuery("CREATE INDEX ON BATCH_EXEC_TEST (NAME)", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));

        for (int i = 0; i < 10; i++) {
            database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('User" + i + "')", transactionId);
        }

        Object endResult = database.executeQuery("END BATCH", transactionId);
        assertEquals("Batch committed", endResult);

        List<?> rows = (List<?>) database.executeQuery("SELECT * FROM BATCH_EXEC_TEST WHERE NAME = 'User5'", null);
        assertEquals(1, rows.size());
    }

    @Test
    void nestedBatchShouldFail() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING)", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));

        assertThrows(TransactionException.class, () ->
            database.executeQuery("BEGIN BATCH", transactionId));
    }

    @Test
    void nonModifiedTableIndexShouldRemainActiveAfterBatch() {
        database.executeQuery("CREATE TABLE BATCH_EXEC_TEST (ID LONG PRIMARY KEY SEQUENCE(batch_seq 1 1), NAME STRING)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON BATCH_EXEC_TEST (NAME)", null);
        database.executeQuery("CREATE TABLE OTHER_TABLE (ID LONG PRIMARY KEY, CODE STRING)", null);
        database.executeQuery("INSERT INTO OTHER_TABLE (ID, CODE) VALUES (1, 'X')", null);

        UUID transactionId = (UUID) parseTransactionId(database.executeQuery("BEGIN BATCH", null));
        database.executeQuery("INSERT INTO OTHER_TABLE (ID, CODE) VALUES (2, 'Y')", transactionId);
        database.executeQuery("END BATCH", transactionId);

        // BATCH_EXEC_TEST was snapshotted but NOT modified by the batch, so its unique
        // index must still be maintained for subsequent auto-commit operations.
        database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('User1')", null);
        assertThrows(IllegalStateException.class, () ->
            database.executeQuery("INSERT INTO BATCH_EXEC_TEST (NAME) VALUES ('User1')", null));

        List<?> rows = (List<?>) database.executeQuery("SELECT * FROM BATCH_EXEC_TEST WHERE NAME = 'User1'", null);
        assertEquals(1, rows.size());
    }

    private UUID parseTransactionId(Object result) {
        String str = result.toString();
        String prefix = "Batch started: ";
        if (str.startsWith(prefix)) {
            return UUID.fromString(str.substring(prefix.length()));
        }
        throw new IllegalArgumentException("Unexpected result: " + str);
    }
}