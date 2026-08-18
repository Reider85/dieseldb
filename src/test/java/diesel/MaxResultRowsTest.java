package diesel;

import diesel.Database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaxResultRowsTest {

    private static final int LEFT_COUNT = 100;
    private static final int RIGHT_COUNT = 100;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTables();
        insertRecords();
    }

    @AfterEach
    void tearDown() {
        dropTables();
        SelectQuery.loadHashJoinConfig();
    }

    private void createTables() {
        database.executeQuery("CREATE TABLE MRT_LEFT (ID LONG PRIMARY KEY SEQUENCE(mrt_lseq 1 1), VAL INTEGER)", null);
        database.executeQuery("CREATE TABLE MRT_RIGHT (ID LONG PRIMARY KEY SEQUENCE(mrt_rseq 1 1), VAL INTEGER)", null);
    }

    private void dropTables() {
        try {
            database.dropTable("MRT_LEFT");
        } catch (TableNotFoundException ignored) {
        }
        try {
            database.dropTable("MRT_RIGHT");
        } catch (TableNotFoundException ignored) {
        }
    }

    private void insertRecords() {
        for (int i = 1; i <= LEFT_COUNT; i++) {
            database.executeQuery("INSERT INTO MRT_LEFT (VAL) VALUES (" + i + ")", null);
        }
        for (int i = 1; i <= RIGHT_COUNT; i++) {
            database.executeQuery("INSERT INTO MRT_RIGHT (VAL) VALUES (" + i + ")", null);
        }
    }

    private static final String CROSS_JOIN_SQL =
            "SELECT MRT_LEFT.ID, MRT_RIGHT.ID FROM MRT_LEFT CROSS JOIN MRT_RIGHT";

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    private String resultLimitMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }

    @Test
    void defaultLimitIsOneMillion() {
        assertEquals(1_000_000L, SelectQuery.getMaxResultRows(),
                "max.result.rows must default to 1,000,000 rows");
    }

    @Test
    void crossJoinOverLimitThrows() {
        SelectQuery.setMaxResultRowsForTest(5000);
        RuntimeException e = assertThrows(RuntimeException.class, () -> database.executeQuery(CROSS_JOIN_SQL, null),
                "a cross join producing more rows than the limit must abort");
        assertTrue(resultLimitMessage(e).contains("maximum allowed row limit"),
                "the error must explain the row limit, was: " + resultLimitMessage(e));
    }

    @Test
    void exactLimitIsAllowed() {
        SelectQuery.setMaxResultRowsForTest(LEFT_COUNT * RIGHT_COUNT);
        List<Map<String, Object>> result = runSelect(CROSS_JOIN_SQL);
        assertEquals(LEFT_COUNT * RIGHT_COUNT, result.size(),
                "a result of exactly the limit rows must succeed");
    }

    @Test
    void simpleSelectOverLimitThrows() {
        SelectQuery.setMaxResultRowsForTest(2);
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> database.executeQuery("SELECT * FROM MRT_LEFT", null),
                "a plain SELECT returning more rows than the limit must abort");
        assertTrue(resultLimitMessage(e).contains("maximum allowed row limit"),
                "the error must explain the row limit, was: " + resultLimitMessage(e));
    }

    @Test
    void hintOverridesLowLimit() {
        SelectQuery.setMaxResultRowsForTest(100);
        List<Map<String, Object>> result = runSelect("SELECT /* MAX_ROWS=20000 */ MRT_LEFT.ID, MRT_RIGHT.ID "
                + "FROM MRT_LEFT CROSS JOIN MRT_RIGHT");
        assertEquals(LEFT_COUNT * RIGHT_COUNT, result.size(),
                "the MAX_ROWS hint must raise the per-query limit above the configured default");
    }

    @Test
    void hintBelowResultSizeThrows() {
        SelectQuery.setMaxResultRowsForTest(1_000_000);
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> database.executeQuery("SELECT /* MAX_ROWS=100 */ MRT_LEFT.ID, MRT_RIGHT.ID "
                        + "FROM MRT_LEFT CROSS JOIN MRT_RIGHT", null),
                "a hint below the produced row count must abort the query");
        assertTrue(resultLimitMessage(e).contains("maximum allowed row limit"),
                "the error must explain the row limit, was: " + resultLimitMessage(e));
    }

    @Test
    void hintZeroDisablesLimit() {
        SelectQuery.setMaxResultRowsForTest(100);
        List<Map<String, Object>> result = runSelect("SELECT /* MAX_ROWS=0 */ MRT_LEFT.ID, MRT_RIGHT.ID "
                + "FROM MRT_LEFT CROSS JOIN MRT_RIGHT");
        assertEquals(LEFT_COUNT * RIGHT_COUNT, result.size(),
                "MAX_ROWS=0 must disable the limit for the query");
    }

    @Test
    void lowercaseHintIsHonored() {
        SelectQuery.setMaxResultRowsForTest(100);
        List<Map<String, Object>> result = runSelect("SELECT /* max_rows=20000 */ MRT_LEFT.ID, MRT_RIGHT.ID "
                + "FROM MRT_LEFT CROSS JOIN MRT_RIGHT");
        assertEquals(LEFT_COUNT * RIGHT_COUNT, result.size(),
                "the MAX_ROWS hint must be matched case-insensitively");
    }

    @Test
    void explainAnalyzeHonorsHint() {
        SelectQuery.setMaxResultRowsForTest(1_000_000);
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> database.executeQuery("EXPLAIN ANALYZE /* MAX_ROWS=2 */ SELECT * FROM MRT_LEFT", null),
                "EXPLAIN ANALYZE must execute the inner SELECT under the hinted limit");
        assertTrue(resultLimitMessage(e).contains("maximum allowed row limit"),
                "the error must explain the row limit, was: " + resultLimitMessage(e));
    }

    @Test
    void warningLoggedAt80PercentOfLimit() {
        Logger logger = Logger.getLogger("diesel.SelectQuery");
        List<LogRecord> captured = new java.util.ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                captured.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(handler);
        try {
            SelectQuery.setMaxResultRowsForTest(100);
            List<Map<String, Object>> result = runSelect("SELECT * FROM MRT_LEFT");
            assertEquals(LEFT_COUNT, result.size());
            assertTrue(captured.stream().anyMatch(record ->
                            record.getLevel() == Level.WARNING
                                    && record.getMessage() != null
                                    && record.getMessage().contains("approaching the maximum allowed row limit")),
                    "a query reaching 80% of the limit must log a single warning");
        } finally {
            logger.removeHandler(handler);
        }
    }
}
