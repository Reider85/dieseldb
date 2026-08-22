package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for covering B-tree indexes (Prompt 56):
 * - Covering index creation via SQL
 * - Covering index stores extra column values
 * - EXPLAIN reports covering index usage
 */
public class CoveringIndexTest {

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
    void createCoveringIndex() {
        createTable();
        assertDoesNotThrow(() -> database.executeQuery(
                "CREATE INDEX ON T1(AGE) COVERING (NAME, SALARY)", null));
        Index index = database.getTable("T1").getIndex("AGE");
        assertNotNull(index);
        assertTrue(index instanceof CoveringBTreeIndex);
    }

    @Test
    void coveringIndexReturnsCorrectResults() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE) COVERING (NAME, SALARY)", null);
        List<Map<String, Object>> rows = query("SELECT ID, NAME, AGE FROM T1 WHERE AGE = 5");
        assertNotNull(rows);
        assertFalse(rows.isEmpty());
        for (Map<String, Object> row : rows) {
            assertEquals(5L, row.get("AGE"));
            assertNotNull(row.get("NAME"));
        }
    }

    @Test
    void coveringIndexAfterInsert() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE) COVERING (NAME, SALARY)", null);
        assertDoesNotThrow(() -> query("INSERT INTO T1 (ID, NAME, AGE, SALARY) VALUES (200, 'name200', 99, 200000)"));
        List<Map<String, Object>> rows = query("SELECT ID, NAME, AGE FROM T1 WHERE AGE = 99");
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals("name200", rows.get(0).get("NAME"));
    }

    @Test
    void coveringIndexExplainPlan() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE) COVERING (NAME, SALARY)", null);
        String plan = explain("EXPLAIN SELECT ID, NAME, AGE FROM T1 WHERE AGE = 5");
        assertTrue(plan.contains("Covering") && plan.contains("AGE"),
                "EXPLAIN should report covering index: " + plan);
    }

    @Test
    void coveringIndexWithNullCoverValues() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1(AGE) COVERING (NAME, SALARY)", null);
        assertDoesNotThrow(() -> query("INSERT INTO T1 (ID, NAME, AGE, SALARY) VALUES (200, NULL, 99, 200000)"));
        List<Map<String, Object>> rows = query("SELECT ID, NAME, AGE FROM T1 WHERE AGE = 99");
        assertNotNull(rows);
        assertFalse(rows.isEmpty());
    }
}
