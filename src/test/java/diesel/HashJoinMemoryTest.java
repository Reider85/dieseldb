package diesel;

import diesel.Database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashJoinMemoryTest {

    private static final int RECORD_COUNT = 200;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTables();
        insertRecords();
    }

    @AfterEach
    void tearDown() {
        SelectQuery.loadHashJoinConfig();
    }

    private void createTables() {
        dropTables();
        String createUsersTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        String createDetailsTableQuery = "CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)";
        database.executeQuery(createUsersTableQuery, null);
        database.executeQuery(createDetailsTableQuery, null);
    }

    private void dropTables() {
        try {
            database.dropTable("USERS");
        } catch (IllegalArgumentException ignored) {
        }
        try {
            database.dropTable("USER_DETAILS");
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void insertRecords() {
        for (int i = 1; i <= RECORD_COUNT; i++) {
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);
            String userQuery = String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82), balance
            );
            String detailQuery = String.format(
                    "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO, BALANCE) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d', %s)",
                    i, i, i, 18 + (i % 82), i, balance
            );
            database.executeQuery(userQuery, null);
            database.executeQuery(detailQuery, null);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    private SelectQuery runJoin() {
        String sql = "SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID";
        SelectQuery selectQuery = (SelectQuery) new QueryParser().parse(sql, database);
        selectQuery.execute(database.getTable("USERS"));
        return selectQuery;
    }

    @Test
    void inMemoryHashJoinProducesCorrectResultsAndMetrics() {
        SelectQuery.loadHashJoinConfig();
        List<Map<String, Object>> result = runSelect(
                "SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID");
        assertEquals(RECORD_COUNT, result.size(), "in-memory hash join must return exactly RECORD_COUNT rows");

        SelectQuery selectQuery = runJoin();
        assertFalse(selectQuery.isLastJoinUsedPartitioning(), "default config must use the in-memory hash join");
        assertEquals(RECORD_COUNT, selectQuery.getLastHashJoinTableSize(), "hash table must hold one entry per distinct key");
        assertTrue(selectQuery.getLastHashJoinBuildTimeMs() >= 0, "build time metric must be recorded");
        assertTrue(selectQuery.getLastHashJoinProbeTimeMs() >= 0, "probe time metric must be recorded");
    }

    @Test
    void partitionedHashJoinUsedWhenEstimatedSizeExceedsMemoryBudget() {
        SelectQuery.setHashJoinConfigForTest(100, 0);
        List<Map<String, Object>> result = runSelect(
                "SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID");
        assertEquals(RECORD_COUNT, result.size(), "partitioned hash join must return the same rows as the in-memory hash join");

        SelectQuery selectQuery = runJoin();
        assertTrue(selectQuery.isLastJoinUsedPartitioning(), "zero-byte memory budget must force the partitioned hash join");
        assertEquals(RECORD_COUNT, selectQuery.getLastHashJoinTableSize(), "partitioned hash join must cover every distinct key");
        assertTrue(selectQuery.getLastHashJoinBuildTimeMs() >= 0, "partitioned build time metric must be recorded");
        assertTrue(selectQuery.getLastHashJoinProbeTimeMs() >= 0, "partitioned probe time metric must be recorded");
    }

    @Test
    void partitionedHashJoinUsedWhenRowsExceedMaxInMemory() {
        SelectQuery.setHashJoinConfigForTest(5, 1024);
        List<Map<String, Object>> result = runSelect(
                "SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID");
        assertEquals(RECORD_COUNT, result.size(), "partitioned hash join must return the same rows as the in-memory hash join");

        SelectQuery selectQuery = runJoin();
        assertTrue(selectQuery.isLastJoinUsedPartitioning(), "row budget overflow must route to the partitioned hash join, not the O(n x m) nested loop");
        assertEquals(RECORD_COUNT, selectQuery.getLastHashJoinTableSize(), "partitioned hash join must cover every distinct key");
        assertTrue(selectQuery.getLastHashJoinBuildTimeMs() >= 0, "partitioned build time metric must be recorded");
        assertTrue(selectQuery.getLastHashJoinProbeTimeMs() >= 0, "partitioned probe time metric must be recorded");
    }
}
