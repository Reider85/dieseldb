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

    @Test
    void offsetZeroReturnsAllRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID OFFSET 0");
            assertEquals(RECORD_COUNT, rows.size(), "OFFSET 0 must return all rows");
            assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue(), "first row must have the smallest ID");
        }, "offsetZeroReturnsAllRows");
    }

    @Test
    void offsetFiveWithoutLimitSkipsFirstFive() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID OFFSET 5");
            assertEquals(RECORD_COUNT - 5, rows.size(), "OFFSET 5 must return all rows after the first five");
            assertEquals(6L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 5 must have ID 6");
        }, "offsetFiveWithoutLimitSkipsFirstFive");
    }

    @Test
    void offsetAppliedAfterDescendingOrderBy() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS ORDER BY ID DESC OFFSET 3");
            assertEquals(RECORD_COUNT - 3, rows.size(), "ORDER BY ID DESC OFFSET 3 must return all rows after the top three");
            assertEquals(27L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 3 on ID DESC must have ID 27");
        }, "offsetAppliedAfterDescendingOrderBy");
    }

    @Test
    void offsetGreaterThanTotalRowsReturnsEmpty() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID OFFSET 100");
            assertEquals(0, rows.size(), "OFFSET 100 must return an empty result, not an error");
        }, "offsetGreaterThanTotalRowsReturnsEmpty");
    }

    @Test
    void offsetEqualToTotalRowsReturnsEmpty() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID OFFSET 30");
            assertEquals(0, rows.size(), "OFFSET equal to total row count must return an empty result");
        }, "offsetEqualToTotalRowsReturnsEmpty");
    }

    @Test
    void offsetWithoutLimitAndOrderByUsesInsertionOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS OFFSET 5");
            assertEquals(RECORD_COUNT - 5, rows.size(), "OFFSET 5 without ORDER BY must return all rows after the first five");
            assertEquals(6L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 5 in insertion order must have ID 6");
        }, "offsetWithoutLimitAndOrderByUsesInsertionOrder");
    }

    @Test
    void offsetZeroWithoutOrderByReturnsAllRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS OFFSET 0");
            assertEquals(RECORD_COUNT, rows.size(), "OFFSET 0 without ORDER BY must return all rows");
        }, "offsetZeroWithoutOrderByReturnsAllRows");
    }

    @Test
    void offsetWithWhereWithoutLimit() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS WHERE AGE > 20 OFFSET 5");
            assertEquals(5, rows.size(), "WHERE AGE > 20 (10 rows) OFFSET 5 must return the remaining five rows");
            assertEquals(6L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 5 of the filtered rows must have ID 6");
            assertEquals(10L, ((Number) rows.get(4).get("ID")).longValue(),
                    "last remaining row must have ID 10");
        }, "offsetWithWhereWithoutLimit");
    }

    @Test
    void offsetAppliedAfterGroupByOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT AGE, COUNT(*) AS CNT FROM USERS GROUP BY AGE ORDER BY AGE OFFSET 5");
            assertEquals(RECORD_COUNT - 5, rows.size(), "GROUP BY AGE (30 groups) ORDER BY AGE OFFSET 5 must return the remaining 25 groups");
            assertEquals(6L, ((Number) rows.get(0).get("AGE")).longValue(),
                    "first group after OFFSET 5 must have AGE 6");
        }, "offsetAppliedAfterGroupByOrder");
    }

    @Test
    void limitTenOffsetFiveReturnsTenRowsAfterOffset() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID LIMIT 10 OFFSET 5");
            assertEquals(10, rows.size(), "LIMIT 10 OFFSET 5 must return exactly ten rows");
            assertEquals(6L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 5 must have ID 6");
            assertEquals(15L, ((Number) rows.get(9).get("ID")).longValue(),
                    "tenth row must have ID 15");
        }, "limitTenOffsetFiveReturnsTenRowsAfterOffset");
    }

    @Test
    void limitTenOffsetFiveWithoutOrderByUsesInsertionOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS LIMIT 10 OFFSET 5");
            assertEquals(10, rows.size(), "LIMIT 10 OFFSET 5 without ORDER BY must return exactly ten rows");
            assertEquals(6L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 5 in insertion order must have ID 6");
        }, "limitTenOffsetFiveWithoutOrderByUsesInsertionOrder");
    }

    @Test
    void limitOneOffsetNinetyNineReturnsNoRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID LIMIT 1 OFFSET 99");
            assertEquals(0, rows.size(), "LIMIT 1 OFFSET 99 (offset > total) must return an empty result, not an error");
        }, "limitOneOffsetNinetyNineReturnsNoRows");
    }

    @Test
    void limitHundredOffsetZeroReturnsAllRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID LIMIT 100 OFFSET 0");
            assertEquals(RECORD_COUNT, rows.size(), "LIMIT 100 OFFSET 0 must return all rows");
            assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row must have the smallest ID");
        }, "limitHundredOffsetZeroReturnsAllRows");
    }

    @Test
    void limitZeroWithOffsetReturnsNoRows() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID LIMIT 0 OFFSET 5");
            assertEquals(0, rows.size(), "LIMIT 0 OFFSET 5 must return no rows");
        }, "limitZeroWithOffsetReturnsNoRows");
    }

    @Test
    void limitOffsetSumExceedingTotalReturnsRemainder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS ORDER BY ID LIMIT 20 OFFSET 25");
            assertEquals(5, rows.size(), "LIMIT 20 OFFSET 25 must return the remaining five rows, not twenty");
            assertEquals(26L, ((Number) rows.get(0).get("ID")).longValue(),
                    "first row after OFFSET 25 must have ID 26");
            assertEquals(30L, ((Number) rows.get(4).get("ID")).longValue(),
                    "last remaining row must have ID 30");
        }, "limitOffsetSumExceedingTotalReturnsRemainder");
    }

    @Test
    void limitOffsetAppliedAfterWhereAndOrderBy() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT ID, AGE FROM USERS WHERE AGE > 20 ORDER BY AGE DESC LIMIT 5 OFFSET 3");
            assertEquals(5, rows.size(), "WHERE AGE > 20 (10 rows) ORDER BY AGE DESC LIMIT 5 OFFSET 3 must return exactly five rows");
            assertEquals(4L, ((Number) rows.get(0).get("ID")).longValue(),
                    "after OFFSET 3 of the AGE DESC ordering (IDs 1,2,3,4,5 have the top five ages) the first row must have ID 4");
        }, "limitOffsetAppliedAfterWhereAndOrderBy");
    }

    @Test
    void limitOffsetAfterGroupByOrder() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT AGE, COUNT(*) AS CNT FROM USERS GROUP BY AGE ORDER BY AGE LIMIT 5 OFFSET 10");
            assertEquals(5, rows.size(), "GROUP BY AGE (30 groups) ORDER BY AGE LIMIT 5 OFFSET 10 must return exactly five groups");
            assertEquals(11L, ((Number) rows.get(0).get("AGE")).longValue(),
                    "first group after OFFSET 10 must have AGE 11");
        }, "limitOffsetAfterGroupByOrder");
    }

    @Test
    void aggregateWithoutGroupByLimitOffset() {
        assertDoesNotThrow(() -> {
            List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) AS CNT FROM USERS LIMIT 1 OFFSET 0");
            assertEquals(1, rows.size(), "aggregate with LIMIT 1 OFFSET 0 must return exactly one row");
            assertEquals(RECORD_COUNT, ((Number) rows.get(0).get("CNT")).longValue(),
                    "COUNT(*) must be computed over all rows, not limited rows");
        }, "aggregateWithoutGroupByLimitOffset");
    }
}
