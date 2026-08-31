package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BatchQueryTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable("BATCH_TEST1");
        } catch (TableNotFoundException ignored) {
            // table does not exist yet, nothing to drop
        }
        try {
            database.dropTable("BATCH_TEST2");
        } catch (TableNotFoundException ignored) {
            // table does not exist yet, nothing to drop
        }
        
        // Create two separate tables
        database.executeQuery("CREATE TABLE BATCH_TEST1 (ID LONG PRIMARY KEY SEQUENCE(batch1_seq 1 1), NAME STRING)", null);
        database.executeQuery("CREATE TABLE BATCH_TEST2 (ID LONG PRIMARY KEY SEQUENCE(batch2_seq 1 1), VALUE INTEGER)", null);
        
        // Insert test data
        database.executeQuery("INSERT INTO BATCH_TEST1 (NAME) VALUES ('Alice')", null);
        database.executeQuery("INSERT INTO BATCH_TEST1 (NAME) VALUES ('Bob')", null);
        database.executeQuery("INSERT INTO BATCH_TEST2 (VALUE) VALUES (100)", null);
        database.executeQuery("INSERT INTO BATCH_TEST2 (VALUE) VALUES (200)", null);
        database.executeQuery("INSERT INTO BATCH_TEST2 (VALUE) VALUES (300)", null);
    }

    @Test
    void batchQueryOnDifferentTablesShouldWork() {
        // Execute queries on different tables in a batch
        List<String> queries = List.of(
            "SELECT COUNT(*) as count FROM BATCH_TEST1",
            "SELECT SUM(VALUE) as total FROM BATCH_TEST2"
        );
        
        List<Object> results = database.executeBatch(queries, null);
        
        assertNotNull(results);
        assertEquals(2, results.size());
        
        // First result: count from BATCH_TEST1 should be 2
        assertInstanceOf(List.class, results.get(0));
        List<?> firstResult = (List<?>) results.get(0);
        assertEquals(1, firstResult.size());
        assertInstanceOf(Map.class, firstResult.get(0));
        Map<String, Object> firstRow = (Map<String, Object>) firstResult.get(0);
        assertEquals(2L, ((Number) firstRow.get("count")).longValue());
        
        // Second result: sum from BATCH_TEST2 should be 600
        assertInstanceOf(List.class, results.get(1));
        List<?> secondResult = (List<?>) results.get(1);
        assertEquals(1, secondResult.size());
        assertInstanceOf(Map.class, secondResult.get(0));
        Map<String, Object> secondRow = (Map<String, Object>) secondResult.get(0);
        assertEquals(600L, ((Number) secondRow.get("total")).longValue());
    }

    @Test
    void batchQueryOnSameTableShouldWorkSequentially() {
        // Execute queries on the same table in a batch (should execute sequentially)
        List<String> queries = List.of(
            "SELECT NAME FROM BATCH_TEST1 WHERE ID = 1",
            "SELECT NAME FROM BATCH_TEST1 WHERE ID = 2"
        );
        
        List<Object> results = database.executeBatch(queries, null);
        
        assertNotNull(results);
        assertEquals(2, results.size());
        
        // First result: should be Alice
        assertInstanceOf(List.class, results.get(0));
        List<?> firstResult = (List<?>) results.get(0);
        assertEquals(1, firstResult.size());
        assertInstanceOf(Map.class, firstResult.get(0));
        Map<String, Object> firstRow = (Map<String, Object>) firstResult.get(0);
        assertEquals("Alice", firstRow.get("NAME"));
        
        // Second result: should be Bob
        assertInstanceOf(List.class, results.get(1));
        List<?> secondResult = (List<?>) results.get(1);
        assertEquals(1, secondResult.size());
        assertInstanceOf(Map.class, secondResult.get(0));
        Map<String, Object> secondRow = (Map<String, Object>) secondResult.get(0);
        assertEquals("Bob", secondRow.get("NAME"));
    }

    @Test
    void batchQueryWithSingleQueryShouldWork() {
        // Execute a single query in a batch
        List<String> queries = List.of("SELECT COUNT(*) as count FROM BATCH_TEST1");
        
        List<Object> results = database.executeBatch(queries, null);
        
        assertNotNull(results);
        assertEquals(1, results.size());
        
        // Result: count from BATCH_TEST1 should be 2
        assertInstanceOf(List.class, results.get(0));
        List<?> result = (List<?>) results.get(0);
        assertEquals(1, result.size());
        assertInstanceOf(Map.class, result.get(0));
        Map<String, Object> row = (Map<String, Object>) result.get(0);
        assertEquals(2L, ((Number) row.get("count")).longValue());
    }

    @Test
    void batchQueryWithEmptyListShouldReturnEmptyList() {
        List<String> queries = List.of();
        
        List<Object> results = database.executeBatch(queries, null);
        
        assertNotNull(results);
        assertEquals(0, results.size());
    }
}