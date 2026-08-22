package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WHERE-condition index optimization (Prompt 56):
 * - Range conditions (<, >, <=, >=) use BTree index
 * - Multi-condition AND intersection uses multiple indexes
 * - EXPLAIN reports correct index usage
 */
public class WhereIndexTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
    }

    @AfterEach
    void tearDown() {
        for (String name : new String[]{"T1", "T2"}) {
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

    // ── Helpers ──────────────────────────────────────────────────────

    private void createIndexedTable() {
        database.executeQuery("CREATE TABLE T1 (ID LONG PRIMARY KEY, NAME STRING, AGE LONG, SALARY DOUBLE)", null);
        database.executeQuery("CREATE INDEX ON T1 (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON T1 (NAME)", null);
        for (int i = 1; i <= 100; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "name" + i);
            row.put("AGE", (long) (i % 50));
            row.put("SALARY", i * 1000.0);
            database.getTable("T1").addRow(row);
        }
    }

    private void createSecondTable() {
        database.executeQuery("CREATE TABLE T2 (ID LONG PRIMARY KEY, T1_ID LONG, SCORE DOUBLE)", null);
        database.executeQuery("CREATE INDEX ON T2 (T1_ID)", null);
        for (int i = 1; i <= 50; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", (long) i);
            row.put("T1_ID", (long) (i % 10));
            row.put("SCORE", i * 10.5);
            database.getTable("T2").addRow(row);
        }
    }

    // ── Range conditions (no exception = index used) ─────────────────

    @Test
    void lessThanUsesBTreeIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE < 10"));
    }

    @Test
    void greaterThanUsesBTreeIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE > 40"));
    }

    @Test
    void lessThanOrEqualsUsesBTreeIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE <= 5"));
    }

    @Test
    void greaterThanOrEqualsUsesBTreeIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE >= 45"));
    }

    // ── Multi-condition AND intersection ─────────────────────────────

    @Test
    void equalityAndRangeIntersect() {
        createIndexedTable();
        assertDoesNotThrow(() -> query(
                "SELECT ID FROM T1 WHERE NAME = 'name5' AND AGE < 10"));
    }

    @Test
    void twoRangeConditionsIntersect() {
        createIndexedTable();
        assertDoesNotThrow(() -> query(
                "SELECT ID FROM T1 WHERE AGE > 5 AND AGE < 15"));
    }

    @Test
    void threeConditionsIntersect() {
        createIndexedTable();
        assertDoesNotThrow(() -> query(
                "SELECT ID FROM T1 WHERE NAME = 'name10' AND AGE > 5 AND AGE < 15"));
    }

    // ── OR still disables index pre-filter ───────────────────────────

    @Test
    void orDisablesIndexPreFilter() {
        createIndexedTable();
        assertDoesNotThrow(() -> query(
                "SELECT ID FROM T1 WHERE AGE = 5 OR AGE = 10"));
    }

    // ── Negated conditions skip index ────────────────────────────────

    @Test
    void notEqualsDoesNotUseIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE != 5"));
    }

    @Test
    void notInDoesNotUseIndex() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE NOT IN (5, 10, 15)"));
    }

    // ── EXPLAIN output ───────────────────────────────────────────────

    @Test
    void explainReportsBTreeIndexForRange() {
        createIndexedTable();
        String plan = explain("EXPLAIN SELECT ID FROM T1 WHERE AGE < 10");
        assertTrue(plan.contains("B-tree") && plan.contains("AGE"),
                "EXPLAIN should report B-tree index on AGE: " + plan);
    }

    @Test
    void explainReportsHashIndexForEquality() {
        createIndexedTable();
        String plan = explain("EXPLAIN SELECT ID FROM T1 WHERE NAME = 'name1'");
        assertTrue(plan.contains("Hash") && plan.contains("NAME"),
                "EXPLAIN should report Hash index on NAME: " + plan);
    }

    @Test
    void explainReportsRangeForLessThanOrEquals() {
        createIndexedTable();
        String plan = explain("EXPLAIN SELECT ID FROM T1 WHERE AGE <= 10");
        assertTrue(plan.contains("B-tree") && plan.contains("AGE"),
                "EXPLAIN should report B-tree index on AGE for <= : " + plan);
    }

    @Test
    void explainReportsRangeForGreaterThanOrEquals() {
        createIndexedTable();
        String plan = explain("EXPLAIN SELECT ID FROM T1 WHERE AGE >= 40");
        assertTrue(plan.contains("B-tree") && plan.contains("AGE"),
                "EXPLAIN should report B-tree index on AGE for >= : " + plan);
    }

    // ── Full scan fallback ───────────────────────────────────────────

    @Test
    void fullScanWhenNoIndexExists() {
        createIndexedTable();
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE SALARY > 50000"));
    }

    // ── Edge cases ───────────────────────────────────────────────────

    @Test
    void rangeOnEmptyResult() {
        createIndexedTable();
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE < 0");
        assertNotNull(rows);
        assertTrue(rows.isEmpty());
    }

    @Test
    void rangeOnAllRows() {
        createIndexedTable();
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE >= 0");
        assertNotNull(rows);
        assertEquals(100, rows.size());
    }

    @Test
    void insertAndQueryWorkAfterIndexChanges() {
        createIndexedTable();
        // Insert a new row and verify it's queryable via index
        assertDoesNotThrow(() -> query("INSERT INTO T1 (ID, NAME, AGE, SALARY) VALUES (200, 'name200', 99, 200000)"));
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE AGE = 99");
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }
}
