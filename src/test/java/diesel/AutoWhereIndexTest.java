package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for auto-creation of WHERE indexes (Prompt 56):
 * - Auto-creates BTree index for WHERE column
 * - No auto-index when already indexed
 * - No auto-index for OR conditions
 * - No auto-index for negated conditions
 * - Warning logged on auto-creation
 */
public class AutoWhereIndexTest {

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
    void autoCreatesBTreeIndexForWhereColumn() {
        createTable();
        assertNull(database.getTable("T1").getIndex("SALARY"));
        // First query should auto-create index on SALARY
        query("SELECT ID FROM T1 WHERE SALARY > 50000");
        assertNotNull(database.getTable("T1").getIndex("SALARY"));
    }

    @Test
    void noAutoIndexWhenAlreadyIndexed() {
        createTable();
        database.executeQuery("CREATE INDEX ON T1 (AGE)", null);
        assertNotNull(database.getTable("T1").getIndex("AGE"));
        // Query on already-indexed column should not fail
        assertDoesNotThrow(() -> query("SELECT ID FROM T1 WHERE AGE = 5"));
        assertNotNull(database.getTable("T1").getIndex("AGE"));
    }

    @Test
    void noAutoIndexForOrConditions() {
        createTable();
        assertNull(database.getTable("T1").getIndex("SALARY"));
        // OR conditions should not trigger auto-index creation
        query("SELECT ID FROM T1 WHERE AGE = 5 OR SALARY > 50000");
        // SALARY index should NOT be auto-created for OR
        assertNull(database.getTable("T1").getIndex("SALARY"));
    }

    @Test
    void noAutoIndexForNegatedConditions() {
        createTable();
        assertNull(database.getTable("T1").getIndex("SALARY"));
        // Negated conditions should not trigger auto-index creation
        query("SELECT ID FROM T1 WHERE SALARY != 50000");
        assertNull(database.getTable("T1").getIndex("SALARY"));
    }

    @Test
    void autoIndexWarningLogged() {
        createTable();
        // Set up log capture
        Logger logger = Logger.getLogger(SelectQuery.class.getName());
        List<LogRecord> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            @Override public void publish(LogRecord record) { warnings.add(record); }
            @Override public void flush() {}
            @Override public void close() {}
        };
        handler.setLevel(Level.WARNING);
        logger.addHandler(handler);
        try {
            query("SELECT ID FROM T1 WHERE SALARY > 50000");
            assertTrue(warnings.stream().anyMatch(r ->
                    r.getMessage().contains("Auto-created index") && r.getMessage().contains("SALARY")),
                    "Should log WARNING about auto-created index");
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void autoIndexImprovesSubsequentQuery() {
        createTable();
        assertNull(database.getTable("T1").getIndex("SALARY"));
        // First query creates the index
        query("SELECT ID FROM T1 WHERE SALARY > 50000");
        assertNotNull(database.getTable("T1").getIndex("SALARY"));
        // Second query should use the index
        List<Map<String, Object>> rows = query("SELECT ID FROM T1 WHERE SALARY > 50000");
        assertNotNull(rows);
        assertFalse(rows.isEmpty());
    }
}
