package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LimitOffsetTest {

    private static final int RECORD_COUNT = 30;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTable();
        insertRecords();
    }

    private void createTable() {
        dropTable();
        String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        database.executeQuery(createTableQuery, null);
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void insertRecords() {
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(Locale.US,
                    "INSERT INTO USERS (NAME, AGE, BALANCE) VALUES ('User%d', %d, %s)",
                    i, RECORD_COUNT + 1 - i,
                    new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP)
            );
            database.executeQuery(query, null);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String query) {
        Object result = database.executeQuery(query, null);
        return (List<Map<String, Object>>) result;
    }

    @Test
    void limitOneReturnsSingleRow() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 1");
            assertEquals(1, rows.size(), "LIMIT 1 must return exactly one row");
        }, "limitOneReturnsSingleRow");
    }

    @Test
    void limitTenReturnsTenRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 10");
            assertEquals(10, rows.size(), "LIMIT 10 must return exactly ten rows");
        }, "limitTenReturnsTenRows");
    }

    @Test
    void limitHundredExceedsTotalRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 100");
            assertEquals(RECORD_COUNT, rows.size(), "LIMIT 100 must not exceed total row count");
        }, "limitHundredExceedsTotalRows");
    }

    @Test
    void limitZeroReturnsNoRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 0");
            assertEquals(0, rows.size(), "LIMIT 0 must return no rows");
        }, "limitZeroReturnsNoRows");
    }

    @Test
    void orderByDescLimitReturnsTopRowsInOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID DESC LIMIT 3");
            assertEquals(3, rows.size(), "ORDER BY ID DESC LIMIT 3 must return exactly three rows");
            assertEquals(RECORD_COUNT, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row must have the largest ID");
            assertEquals(RECORD_COUNT - 1, ((Number) rows.get(1).get("ID")).longValue(),
                    "second row must have the second largest ID");
            assertEquals(RECORD_COUNT - 2, ((Number) rows.get(2).get("ID")).longValue(),
                    "third row must have the third largest ID");
        }, "orderByDescLimitReturnsTopRowsInOrder");
    }

    @Test
    void orderByAscLimitReturnsTopRowsInOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID ASC LIMIT 3");
            assertEquals(3, rows.size(), "ORDER BY ID ASC LIMIT 3 must return exactly three rows");
            assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue(), "first row must have the smallest ID");
            assertEquals(2L, ((Number) rows.get(1).get("ID")).longValue(), "second row must have the second smallest ID");
            assertEquals(3L, ((Number) rows.get(2).get("ID")).longValue(), "third row must have the third smallest ID");
        }, "orderByAscLimitReturnsTopRowsInOrder");
    }

    @Test
    void orderBySortsSelectedColumnBeforeLimit() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, AGE FROM USERS ORDER BY AGE DESC LIMIT 5");
            assertEquals(5, rows.size(), "ORDER BY AGE DESC LIMIT 5 must return exactly five rows");
            assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue(),
                    "smallest ID has the largest AGE (30)");
            assertEquals(5L, ((Number) rows.get(4).get("ID")).longValue(),
                    "fifth row must have the fifth largest AGE");
        }, "orderBySortsSelectedColumnBeforeLimit");
    }

    @Test
    void limitAppliedAfterGroupBy() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT AGE, COUNT(*) AS CNT FROM USERS GROUP BY AGE LIMIT 2");
            assertEquals(2, rows.size(), "GROUP BY AGE LIMIT 2 must return exactly two groups");
        }, "limitAppliedAfterGroupBy");
    }

    @Test
    void limitAppliedAfterGroupByOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT AGE, COUNT(*) AS CNT FROM USERS GROUP BY AGE ORDER BY AGE LIMIT 2");
            assertEquals(2, rows.size(), "GROUP BY AGE ORDER BY AGE LIMIT 2 must return exactly two groups");
            assertEquals(1L, ((Number) rows.get(0).get("AGE")).longValue(),
                    "smallest AGE group must come first");
            assertEquals(2L, ((Number) rows.get(1).get("AGE")).longValue(),
                    "second smallest AGE group must come second");
        }, "limitAppliedAfterGroupByOrder");
    }

    @Test
    void aggregateWithoutGroupByIgnoringLimitInput() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) AS CNT FROM USERS LIMIT 1");
            assertEquals(1, rows.size(), "aggregate with LIMIT 1 must return exactly one row");
            assertEquals(RECORD_COUNT, ((Number) rows.get(0).get("CNT")).longValue(),
                    "COUNT(*) must be computed over all rows, not limited rows");
        }, "aggregateWithoutGroupByIgnoringLimitInput");
    }

    @Test
    void aggregateWithoutGroupByLimitZeroReturnsNoRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) AS CNT FROM USERS LIMIT 0");
            assertEquals(0, rows.size(), "aggregate with LIMIT 0 must return no rows");
        }, "aggregateWithoutGroupByLimitZeroReturnsNoRows");
    }

    @Test
    void offsetWithoutLimit() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS ORDER BY ID LIMIT 5 OFFSET 25");
            assertEquals(5, rows.size(), "LIMIT 5 OFFSET 25 must return exactly five rows");
            assertEquals(26L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 25 must have ID 26");
        }, "offsetWithoutLimit");
    }

    @Test
    void limitEqualToTotalRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 30");
            assertEquals(RECORD_COUNT, rows.size(), "LIMIT 30 must return all rows");
        }, "limitEqualToTotalRows");
    }

    @Test
    void limitWithFilterAndOrderBy() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, AGE FROM USERS WHERE AGE > 20 ORDER BY AGE DESC LIMIT 10");
            assertEquals(10, rows.size(), "WHERE AGE > 20 ORDER BY AGE DESC LIMIT 10 must return exactly ten rows");
        }, "limitWithFilterAndOrderBy");
    }
}
