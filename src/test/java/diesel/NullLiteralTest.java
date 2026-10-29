package diesel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 4 #48 (R3-013b) acceptance: the NULL literal works in INSERT,
 * WHERE IS NULL / IS NOT NULL, equality against NULL (three-valued logic:
 * {@code col = NULL} never matches), UPDATE SET col = NULL, the constant
 * {@code SELECT NULL}, and OR-combination with IS NULL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("query")
@ResourceLock("diesel.AbstractDieselTest.tempDir")
public class NullLiteralTest extends AbstractDieselTest {

    @BeforeAll
    void setup() {
        dropTable("NL_TEST");
        database.executeQuery(
                "CREATE TABLE NL_TEST (ID LONG PRIMARY KEY SEQUENCE(nl_seq 1 1), COL STRING, AGE INTEGER)",
                null);
        database.executeQuery("INSERT INTO NL_TEST (COL, AGE) VALUES ('a', 25)", null);
        database.executeQuery("INSERT INTO NL_TEST (COL, AGE) VALUES (NULL, 30)", null);
    }

    @AfterAll
    void tearDown() {
        dropTable("NL_TEST");
    }

    @Test
    @Order(1)
    void whereColIsNullReturnsNullRow() {
        Object result = database.executeQuery("SELECT ID, COL, AGE FROM NL_TEST WHERE COL IS NULL", null);
        assertTrue(result instanceof List, "WHERE COL IS NULL must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "exactly one row has COL = NULL");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertNull(row.get("COL"), "matched row must carry a null COL");
    }

    @Test
    @Order(2)
    void whereColIsNotNullReturnsValueRow() {
        Object result = database.executeQuery("SELECT ID, COL, AGE FROM NL_TEST WHERE COL IS NOT NULL", null);
        assertTrue(result instanceof List);
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "exactly one row has a non-null COL");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals("a", row.get("COL"));
    }

    @Test
    @Order(3)
    void whereColEqualsNullReturnsEmpty() {
        Object result = database.executeQuery("SELECT ID, COL FROM NL_TEST WHERE COL = NULL", null);
        assertTrue(result instanceof List, "col = NULL must still return a (empty) result set");
        assertEquals(0, ((List<?>) result).size(),
                "col = NULL is UNKNOWN in three-valued logic and matches no rows");
    }

    @Test
    @Order(4)
    void updateSetNullReplacesValue() {
        database.executeQuery("UPDATE NL_TEST SET COL = NULL WHERE AGE = 25", null);
        Object result = database.executeQuery("SELECT ID, COL, AGE FROM NL_TEST WHERE COL IS NULL", null);
        assertTrue(result instanceof List);
        assertEquals(2, ((List<?>) result).size(),
                "after UPDATE SET COL = NULL both rows have a null COL");
        // restore for later tests
        database.executeQuery("UPDATE NL_TEST SET COL = 'a' WHERE AGE = 25", null);
    }

    @Test
    @Order(5)
    void selectNullLiteralReturnsNullValue() {
        Object result = database.executeQuery("SELECT NULL", null);
        assertTrue(result instanceof List, "constant SELECT NULL must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertNull(row.get("NULL"), "SELECT NULL must return a null value under the NULL column");
    }

    @Test
    @Order(6)
    void orWithIsNullKeepsMatchingAndNullRows() {
        Object result = database.executeQuery(
                "SELECT ID, COL, AGE FROM NL_TEST WHERE AGE = 25 OR AGE IS NULL", null);
        assertTrue(result instanceof List);
        assertEquals(1, ((List<?>) result).size(),
                "AGE = 25 matches the first row; AGE IS NULL matches none (both ages are set)");
    }

    @Test
    @Order(7)
    void andNotNullExcludesNullRows() {
        database.executeQuery("UPDATE NL_TEST SET AGE = NULL WHERE COL IS NULL", null);
        Object result = database.executeQuery(
                "SELECT ID, COL, AGE FROM NL_TEST WHERE AGE = 25 AND AGE IS NOT NULL", null);
        assertTrue(result instanceof List);
        assertEquals(1, ((List<?>) result).size(),
                "only the non-null AGE row survives AND AGE IS NOT NULL");
        Object nullAge = database.executeQuery("SELECT ID FROM NL_TEST WHERE AGE IS NULL", null);
        assertEquals(1, ((List<?>) nullAge).size(), "one row has AGE = NULL after the update");
        // restore
        database.executeQuery("UPDATE NL_TEST SET AGE = 30 WHERE COL IS NULL", null);
    }

    @Test
    @Order(8)
    void notIsNullExcludesNullRows() {
        Object result = database.executeQuery("SELECT ID, COL FROM NL_TEST WHERE NOT COL IS NULL", null);
        assertTrue(result instanceof List);
        assertEquals(1, ((List<?>) result).size(), "NOT COL IS NULL keeps only the non-null row");
    }
}
