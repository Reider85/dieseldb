package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class AliasesTest {

    private static final int RECORD_COUNT = 10;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createUsersTable();
        createTransactionsTable();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertUsersRecords();
        insertTransactionsRecords();
    }

    private void createUsersTable() {
        dropTable("USERS");
        String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        database.executeQuery(createTableQuery, null);
    }

    private void createTransactionsTable() {
        dropTable("TRANSACTIONS");
        String createTableQuery = "CREATE TABLE TRANSACTIONS (TRANS_ID LONG PRIMARY KEY SEQUENCE(trans_seq 1 1), USER_ID LONG, TRANS_DATE DATE, AMOUNT BIGDECIMAL)";
        database.executeQuery(createTableQuery, null);
    }

    private void dropTable(String tableName) {
        try {
            database.dropTable(tableName);
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void createUniqueIndex() {
        String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createBTreeIndex() {
        String createIndexQuery = "CREATE INDEX ON USERS (AGE)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createHashIndex() {
        String createIndexQuery = "CREATE HASH INDEX ON USERS (NAME)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createUniqueClusteredIndex() {
        String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (USER_CODE)";
        database.executeQuery(createIndexQuery, null);
    }

    private void insertUsersRecords() {
        Table table = database.getTable("USERS");
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP)
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("USERS");
    }

    private void insertTransactionsRecords() {
        Table table = database.getTable("TRANSACTIONS");
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String transDate = String.format("2024-%02d-%02d", 1 + (i % 12), 1 + (i % 28));
            String query = String.format(
                    "INSERT INTO TRANSACTIONS (USER_ID, TRANS_DATE, AMOUNT) VALUES (%d, '%s', %s)",
                    i, transDate, new BigDecimal(50 + (i % 500)).setScale(2, RoundingMode.HALF_UP)
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("TRANSACTIONS");
    }

    @Test
    void selectWithOrderByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName", null), "selectWithOrderByString");
    }

    @Test
    void selectWithGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME userName, COUNT(*) userCount FROM USERS u GROUP BY userName ORDER BY userName", null), "selectWithGroupByString");
    }

    @Test
    void selectMinMaxAvgWithJoinAndGroupBy() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC", null), "selectMinMaxAvgWithJoinAndGroupBy");
    }

    @Test
    void selectWithMultipleInnerJoins() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5", null), "selectWithMultipleInnerJoins");
    }

    @Test
    void selectWithInCondition() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME userName, USER_CODE code FROM USERS u WHERE userName IN ('User500', 'User501', 'User502')", null), "selectWithInCondition");
    }

    @Test
    void selectWithLikeCondition() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME userName, USER_CODE code FROM USERS u WHERE userName LIKE 'User50%'", null), "selectWithLikeCondition");
    }

    @Test
    void selectWithOrderByStringAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName", null), "selectWithOrderByStringAs");
    }

    @Test
    void selectWithGroupByStringAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME AS userName, COUNT(*) AS userCount FROM USERS u GROUP BY userName ORDER BY userName", null), "selectWithGroupByStringAs");
    }

    @Test
    void selectMinMaxAvgWithJoinAndGroupByAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME AS userName, t.TRANS_DATE AS transDate, MIN(u.AGE) AS minAge, MAX(u.AGE) AS maxAge, AVG(u.AGE) AS avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC", null), "selectMinMaxAvgWithJoinAndGroupByAs");
    }

    @Test
    void selectWithMultipleInnerJoinsAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME AS userName, t.AMOUNT AS transAmount, u2.NAME AS refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5", null), "selectWithMultipleInnerJoinsAs");
    }

    @Test
    void selectWithInConditionAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME AS userName, u.USER_CODE AS code FROM USERS u WHERE u.userName IN ('User500', 'User501', 'User502')", null), "selectWithInConditionAs");
    }

    @Test
    void selectWithLikeConditionAs() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.NAME AS userName, u.USER_CODE AS code FROM USERS u WHERE u.userName LIKE 'User50%'", null), "selectWithLikeConditionAs");
    }
}
