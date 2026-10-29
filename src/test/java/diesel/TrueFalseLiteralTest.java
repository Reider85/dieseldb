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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 4 #48 (R3-013b) acceptance: TRUE/FALSE literals in WHERE, the
 * boolean-as-integer coercion (WHERE flag = 1 is equivalent to WHERE flag =
 * TRUE), INSERT/UPDATE with integer boolean literals, IN lists, and constant
 * SELECT expressions (SELECT 1 = 1 returns Boolean TRUE).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("query")
@ResourceLock("diesel.AbstractDieselTest.tempDir")
public class TrueFalseLiteralTest extends AbstractDieselTest {

    @BeforeAll
    void setup() {
        dropTable("TFL_TEST");
        database.executeQuery(
                "CREATE TABLE TFL_TEST (ID LONG PRIMARY KEY SEQUENCE(tfl_seq 1 1), FLAG BOOLEAN, NAME STRING)",
                null);
        database.executeQuery("INSERT INTO TFL_TEST (FLAG, NAME) VALUES (TRUE, 'yes-row')", null);
        database.executeQuery("INSERT INTO TFL_TEST (FLAG, NAME) VALUES (FALSE, 'no-row')", null);
    }

    @AfterAll
    void tearDown() {
        dropTable("TFL_TEST");
    }

    @Test
    @Order(1)
    void whereFlagTrueReturnsMatchingRow() {
        Object result = database.executeQuery("SELECT ID, FLAG, NAME FROM TFL_TEST WHERE FLAG = TRUE", null);
        assertTrue(result instanceof List, "WHERE FLAG = TRUE must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "WHERE FLAG = TRUE must return exactly one row");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals(Boolean.TRUE, row.get("FLAG"), "matched row must carry FLAG = TRUE");
        assertEquals("yes-row", row.get("NAME"));
    }

    @Test
    @Order(2)
    void whereFlagFalseReturnsMatchingRow() {
        Object result = database.executeQuery("SELECT ID, FLAG, NAME FROM TFL_TEST WHERE FLAG = FALSE", null);
        assertTrue(result instanceof List, "WHERE FLAG = FALSE must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "WHERE FLAG = FALSE must return exactly one row");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals(Boolean.FALSE, row.get("FLAG"));
        assertEquals("no-row", row.get("NAME"));
    }

    @Test
    @Order(3)
    void whereFlagEqualsOneIsEquivalentToTrue() {
        Object result = database.executeQuery("SELECT ID, NAME FROM TFL_TEST WHERE FLAG = 1", null);
        assertTrue(result instanceof List, "WHERE FLAG = 1 must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "WHERE FLAG = 1 must match the same single row as FLAG = TRUE");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals("yes-row", row.get("NAME"), "FLAG = 1 must select the TRUE row");
    }

    @Test
    @Order(4)
    void whereFlagEqualsZeroIsEquivalentToFalse() {
        Object result = database.executeQuery("SELECT ID, NAME FROM TFL_TEST WHERE FLAG = 0", null);
        assertTrue(result instanceof List, "WHERE FLAG = 0 must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "WHERE FLAG = 0 must match the same single row as FLAG = FALSE");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals("no-row", row.get("NAME"), "FLAG = 0 must select the FALSE row");
    }

    @Test
    @Order(5)
    void insertIntegerOneStoresBooleanTrue() {
        dropTable("TFL_INS");
        database.executeQuery(
                "CREATE TABLE TFL_INS (ID LONG PRIMARY KEY SEQUENCE(tfl_ins_seq 1 1), FLAG BOOLEAN)", null);
        database.executeQuery("INSERT INTO TFL_INS (FLAG) VALUES (1)", null);
        database.executeQuery("INSERT INTO TFL_INS (FLAG) VALUES (0)", null);
        Object result = database.executeQuery("SELECT ID, FLAG FROM TFL_INS WHERE FLAG = TRUE", null);
        assertTrue(result instanceof List);
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "INSERT VALUES (1) must store a Boolean true");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals(Boolean.TRUE, row.get("FLAG"), "stored value must be Boolean TRUE, not a string or int");
        dropTable("TFL_INS");
    }

    @Test
    @Order(6)
    void updateSetIntegerOneCoercesToBoolean() {
        dropTable("TFL_UPD");
        database.executeQuery(
                "CREATE TABLE TFL_UPD (ID LONG PRIMARY KEY SEQUENCE(tfl_upd_seq 1 1), FLAG BOOLEAN)", null);
        database.executeQuery("INSERT INTO TFL_UPD (FLAG) VALUES (FALSE)", null);
        database.executeQuery("UPDATE TFL_UPD SET FLAG = 1", null);
        Object result = database.executeQuery("SELECT ID, FLAG FROM TFL_UPD WHERE FLAG = TRUE", null);
        assertTrue(result instanceof List);
        assertEquals(1, ((List<?>) result).size(), "UPDATE SET FLAG = 1 must turn the row into TRUE");
        dropTable("TFL_UPD");
    }

    @Test
    @Order(7)
    void whereFlagInOneZeroMatchesAllRows() {
        Object result = database.executeQuery("SELECT ID, NAME FROM TFL_TEST WHERE FLAG IN (1, 0)", null);
        assertTrue(result instanceof List, "WHERE FLAG IN (1, 0) must return a result set");
        assertEquals(2, ((List<?>) result).size(), "IN (1, 0) must match both the TRUE and FALSE rows");
    }

    @Test
    @Order(8)
    void selectOneEqualsOneReturnsBooleanTrue() {
        Object result = database.executeQuery("SELECT 1 = 1", null);
        assertTrue(result instanceof List, "constant SELECT must return a result set");
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size(), "constant SELECT must return exactly one row");
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        Object value = row.get("1 = 1");
        assertEquals(Boolean.TRUE, value, "SELECT 1 = 1 must return Boolean TRUE");
    }

    @Test
    @Order(9)
    void selectOneEqualsTwoReturnsBooleanFalse() {
        Object result = database.executeQuery("SELECT 1 = 2", null);
        assertTrue(result instanceof List);
        List<?> rows = (List<?>) result;
        assertEquals(1, rows.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rows.get(0);
        assertEquals(Boolean.FALSE, row.get("1 = 2"), "SELECT 1 = 2 must return Boolean FALSE");
    }

    @Test
    @Order(10)
    void selectTrueLiteralReturnsBooleanTrue() {
        Object result = database.executeQuery("SELECT TRUE", null);
        assertTrue(result instanceof List);
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) ((List<?>) result).get(0);
        assertEquals(Boolean.TRUE, row.get("TRUE"), "SELECT TRUE must return Boolean TRUE");
    }

    @Test
    @Order(11)
    void selectTwoGreaterThanOneReturnsTrue() {
        Object result = database.executeQuery("SELECT 2 > 1", null);
        assertTrue(result instanceof List);
        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) ((List<?>) result).get(0);
        assertEquals(Boolean.TRUE, row.get("2 > 1"), "SELECT 2 > 1 must return Boolean TRUE");
    }

    @Test
    @Order(12)
    void whereFlagEqualsInvalidIntegerThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> database.executeQuery("SELECT * FROM TFL_TEST WHERE FLAG = 2", null),
                "WHERE FLAG = 2 must be rejected at parse time (strict boolean literal)");
    }
}
