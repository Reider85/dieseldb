package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for composite B-tree indexes (Prompt 56):
 * - Composite index creation via SQL
 * - Composite index used for multi-column WHERE
 * - Prefix search on leading columns
 * - EXPLAIN reports composite index usage
 */
public class CompositeIndexTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
    }

    @AfterEach
    void tearDown() {
        for (String name : new String[]{"T1"}) {
            try { database.dropTable(name); } catch (TableNotFoundException ignored) {}
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> query(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    private String explain(String sql) {
        return (String) database.executeQuery(sql, null);
    }

    private void createTable() {
        database.executeQuery("CREATE TABLE T1 (ID LONG PRIMARY KEY, NAME STRING, AGE LONG, SALARY DOUBLE)", null);
        for (int i = 1; i <= 100; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "name" + i);
            row.put("AGE", (long) (i % 50));
            row.put("SALARY", i * 1000.0);
            database.getTable("T1").addRow(row);
        }
    }

    @Test
    void createCompositeIndexOnTwoColumns() {
        createTable();
        assertDoesNotThrow(() -> database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null));
        assertNotNull(database.getTable("T1").getIndex("AGE+SALARY"));
    }

    @Test
    void compositeIndexUsedForMultiColumnWhere() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 5 AND SALARY = 6000.0");
        assertNotNull(rows);
        assertFalse(rows.isEmpty());
        // All results should have AGE=5 and SALARY=6000.0
        for (Map<String, Object> row : rows) {
            assertEquals(5L, row.get("AGE"));
            assertEquals(6000.0, row.get("SALARY"));
        }
    }

    @Test
    void compositeIndexPrefixSearch() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        // Query on first column only should use prefix search
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 5");
        assertNotNull(rows);
        assertFalse(rows.isEmpty());
        for (Map<String, Object> row : rows) {
            assertEquals(5L, row.get("AGE"));
        }
    }

    @Test
    void compositeIndexWithNullValues() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        // Insert row with null in one of the indexed columns
        assertDoesNotThrow(() -> query("INSERT INTO T1 (ID, NAME, AGE, SALARY) VALUES (200, 'name200', NULL, 200000)"));
        // Query should still work (null rows won't be in the index)
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 5 AND SALARY = 6000.0");
        assertNotNull(rows);
    }

    @Test
    void compositeIndexAfterInsert() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        // Insert new row
        assertDoesNotThrow(() -> query("INSERT INTO T1 (ID, NAME, AGE, SALARY) VALUES (200, 'name200', 99, 200000)"));
        // Query should find the new row
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 99 AND SALARY = 200000.0");
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    @Test
    void compositeIndexExplainPlan() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        String plan = explain("EXPLAIN SELECT ID FROM T1 WHERE AGE = 5 AND SALARY = 6000.0");
        assertTrue(plan.contains("Composite") && plan.contains("AGE"),
                "EXPLAIN should report composite index: " + plan);
    }

    @Test
    void compositeIndexVsSingleColumnIntersection() {
        createTable();
        // Create composite index
        database.executeQuery("CREATE INDEX ON T1(AGE, SALARY)", null);
        // Query should return correct results
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 10 AND SALARY = 11000.0");
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(11L, rows.get(0).get("ID"));
    }
}
