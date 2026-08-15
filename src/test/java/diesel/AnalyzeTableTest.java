package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnalyzeTableTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
    }

    @AfterEach
    void tearDown() {
        SelectQuery.loadHashJoinConfig();
    }

    private void createUsersTable() {
        database.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
    }

    private void createDetailsTable() {
        database.executeQuery(
                "CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)", null);
    }

    private void insertUsers(int count) {
        for (int i = 1; i <= count; i++) {
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);
            database.executeQuery(String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82), balance), null);
        }
    }

    private void insertDetails(int count) {
        for (int i = 1; i <= count; i++) {
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);
            database.executeQuery(String.format(
                    "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO, BALANCE) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d', %s)",
                    i, i, i, 18 + (i % 82), i, balance), null);
        }
    }

    private SelectQuery runJoin() {
        String sql = "SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID";
        SelectQuery selectQuery = (SelectQuery) new QueryParser().parse(sql, database);
        selectQuery.execute(database.getTable("USERS"));
        return selectQuery;
    }

    @Test
    void analyzeTableReturnsStatisticsMessage() {
        createUsersTable();
        insertUsers(5);
        Object result = database.executeQuery("ANALYZE TABLE USERS", null);
        assertTrue(result instanceof String, "ANALYZE TABLE must return a status message");
        String message = (String) result;
        assertTrue(message.contains("5 rows"), "message must report the row count: " + message);
        assertTrue(message.contains("avg row size"), "message must report the average row size: " + message);
        assertTrue(message.contains("last analyzed"), "message must report the last-analyzed timestamp: " + message);
        Table.TableStatistics stats = database.getTable("USERS").getStatistics();
        assertEquals(5, stats.getRowCount(), "ANALYZE TABLE must report the exact row count");
        assertTrue(stats.getAvgRowSizeBytes() > 0, "average row size must be positive");
        assertTrue(stats.getLastAnalyzedMillis() > 0, "last-analyzed timestamp must be set after ANALYZE");
    }

    @Test
    void analyzeTableIsCaseInsensitiveAndAcceptsSemicolon() {
        createUsersTable();
        insertUsers(2);
        Object result = database.executeQuery("analyze table users;", null);
        assertTrue(result instanceof String, "lowercase ANALYZE TABLE must parse");
        assertTrue(((String) result).contains("2 rows"), ((String) result));
    }

    @Test
    void analyzeTableMissingTableThrows() {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> database.executeQuery("ANALYZE TABLE NOPE", null));
        assertTrue(exception.getMessage().contains("Table NOPE does not exist"), exception.getMessage());
    }

    @Test
    void analyzeTableMalformedSyntaxThrows() {
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery("ANALYZE TABLE", null));
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery("ANALYZE TABLE A B", null));
    }

    @Test
    void rowCountUpdatesSynchronouslyOnInsert() {
        createUsersTable();
        Table table = database.getTable("USERS");
        assertEquals(0, table.getStatistics().getRowCount(), "a fresh table must report 0 rows");
        insertUsers(3);
        assertEquals(3, table.getStatistics().getRowCount(), "statistics must track INSERT");
        insertUsers(2);
        assertEquals(5, table.getStatistics().getRowCount(), "statistics must track further INSERTs");
    }

    @Test
    void rowCountUpdatesOnDelete() {
        createUsersTable();
        insertUsers(5);
        database.executeQuery("DELETE FROM USERS WHERE ID = 2", null);
        database.executeQuery("DELETE FROM USERS WHERE ID = 4", null);
        assertEquals(3, database.getTable("USERS").getStatistics().getRowCount(), "statistics must track DELETE");
        assertEquals(3, database.getTable("USERS").rowCount(), "rowCount() and statistics must agree");
    }

    @Test
    void avgRowSizeReflectsRowContent() {
        database.executeQuery("CREATE TABLE DOCS (ID LONG PRIMARY KEY SEQUENCE(doc_seq 1 1), TITLE STRING)", null);
        Table table = database.getTable("DOCS");
        database.executeQuery("INSERT INTO DOCS (TITLE) VALUES ('abc')", null);
        database.executeQuery("INSERT INTO DOCS (TITLE) VALUES ('def')", null);
        database.executeQuery("ANALYZE TABLE DOCS", null);
        long smallAvg = table.getStatistics().getAvgRowSizeBytes();
        database.executeQuery("DELETE FROM DOCS WHERE ID = 1", null);
        database.executeQuery("DELETE FROM DOCS WHERE ID = 2", null);
        database.executeQuery("INSERT INTO DOCS (TITLE) VALUES ('a very long title that makes each row much bigger')", null);
        database.executeQuery("INSERT INTO DOCS (TITLE) VALUES ('another very long title, again much bigger than before')", null);
        database.executeQuery("ANALYZE TABLE DOCS", null);
        long bigAvg = table.getStatistics().getAvgRowSizeBytes();
        assertTrue(bigAvg > smallAvg,
                "longer string rows must produce a larger average row size: " + smallAvg + " -> " + bigAvg);
    }

    @Test
    void asyncRefreshUpdatesStatisticsAfterInsert() throws InterruptedException {
        createUsersTable();
        insertUsers(5);
        Table table = database.getTable("USERS");
        long deadline = System.currentTimeMillis() + 3000;
        boolean refreshed = false;
        while (System.currentTimeMillis() < deadline) {
            if (table.getStatistics().getLastAnalyzedMillis() > 0) {
                refreshed = true;
                break;
            }
            Thread.sleep(20);
        }
        assertTrue(refreshed, "the asynchronous statistics refresh must update lastAnalyzed after INSERT");
    }

    @Test
    void smallTablesUseNestedLoopByStatistics() {
        createUsersTable();
        createDetailsTable();
        insertUsers(10);
        insertDetails(10);
        SelectQuery selectQuery = runJoin();
        assertFalse(selectQuery.isLastJoinUsedPartitioning());
        assertEquals(0, selectQuery.getLastHashJoinTableSize(),
                "10-row tables must be joined with a nested loop, not a hash join");
        String plan = (String) database.executeQuery(
                "EXPLAIN SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID", null);
        assertTrue(plan.contains("Nested Loop (chosen by statistics)"), plan);
    }

    @Test
    void largerTablesUseInMemoryHashJoinByStatistics() {
        createUsersTable();
        createDetailsTable();
        insertUsers(200);
        insertDetails(200);
        SelectQuery selectQuery = runJoin();
        assertFalse(selectQuery.isLastJoinUsedPartitioning());
        assertEquals(200, selectQuery.getLastHashJoinTableSize(),
                "200-row tables must use the in-memory hash join");
        String plan = (String) database.executeQuery(
                "EXPLAIN SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID", null);
        assertTrue(plan.contains("In-Memory Hash Join"), plan);
    }

    @Test
    void statisticsPersistAcrossSerializedSaveLoad() {
        String tableName = "ANALYZE_PERSIST";
        database.executeQuery("CREATE TABLE " + tableName + " (ID LONG PRIMARY KEY SEQUENCE(p_seq 1 1), NAME STRING)", null);
        for (int i = 1; i <= 4; i++) {
            database.executeQuery("INSERT INTO " + tableName + " (NAME) VALUES ('name" + i + "')", null);
        }
        database.executeQuery("ANALYZE TABLE " + tableName, null);
        database.getTable(tableName).saveToSerializedFile(tableName);
        try {
            Table loaded = Table.loadFromFile(new Database(), tableName);
            assertTrue(loaded != null, "table must load from its serialized file");
            Table.TableStatistics stats = loaded.getStatistics();
            assertEquals(4, stats.getRowCount(), "row count must survive save/load");
            assertTrue(stats.getAvgRowSizeBytes() > 0, "average row size must survive save/load");
            assertTrue(stats.getLastAnalyzedMillis() > 0, "last-analyzed timestamp must survive save/load");
        } finally {
            new File(tableName + ".table").delete();
        }
    }
}
