package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class AliasesTest {
    private static final Logger LOGGER = Logger.getLogger(AliasesTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;

    public AliasesTest() {
        this.database = new Database();
    }

    public void runTests() {
        try {
            // Step 1: Create tables and indexes, insert records
            createUsersTable();
            createTransactionsTable();
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();
            insertUsersRecords();
            insertTransactionsRecords();

            // Step 2: Run SELECT queries with aliases (without AS)
            selectWithOrderByString();
            selectWithGroupByString();
            selectMinMaxAvgWithJoinAndGroupBy();
            selectWithMultipleInnerJoins();
            selectWithInCondition();
            selectWithLikeCondition();

            // Step 3: Run SELECT queries with aliases (with AS)
            selectWithOrderByStringAs();
            selectWithGroupByStringAs();
            selectMinMaxAvgWithJoinAndGroupByAs();
            selectWithMultipleInnerJoinsAs();
            selectWithInConditionAs();
            selectWithLikeConditionAs();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createUsersTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUsersTable");
            dropTable("USERS");
            String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Test createUsersTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createUsersTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createTransactionsTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createTransactionsTable");
            dropTable("TRANSACTIONS");
            String createTableQuery = "CREATE TABLE TRANSACTIONS (TRANS_ID LONG PRIMARY KEY SEQUENCE(trans_seq 1 1), USER_ID LONG, TRANS_DATE DATE, AMOUNT BIGDECIMAL)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Test createTransactionsTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createTransactionsTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void dropTable(String tableName) {
        try {
            database.dropTable(tableName);
            LOGGER.log(Level.INFO, "Table {0} dropped", tableName);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table {0} not found for dropping", tableName);
        }
    }

    private void createUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUniqueIndex");
            String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createBTreeIndex");
            String createIndexQuery = "CREATE INDEX ON USERS (AGE)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createHashIndex");
            String createIndexQuery = "CREATE HASH INDEX ON USERS (NAME)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUniqueClusteredIndex");
            String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (USER_CODE)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertUsersRecords() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertUsersRecords");
            LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
            Table table = database.getTable("USERS");

            long startTime = System.nanoTime();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                String query = String.format(
                        "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                        i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP)
                );
                database.executeQuery(query, null);
            }
            table.saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
            LOGGER.log(Level.INFO, "Test insertUsersRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertUsersRecords: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertTransactionsRecords() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertTransactionsRecords");
            LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
            Table table = database.getTable("TRANSACTIONS");

            long startTime = System.nanoTime();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                String transDate = String.format("2024-%02d-%02d", 1 + (i % 12), 1 + (i % 28));
                String query = String.format(
                        "INSERT INTO TRANSACTIONS (USER_ID, TRANS_DATE, AMOUNT) VALUES (%d, '%s', %s)",
                        i, transDate, new BigDecimal(50 + (i % 500)).setScale(2, BigDecimal.ROUND_HALF_UP)
                );
                database.executeQuery(query, null);
            }
            table.saveToFile("TRANSACTIONS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
            LOGGER.log(Level.INFO, "Test insertTransactionsRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertTransactionsRecords: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private Object executeSelectQuery(String query) {
        LOGGER.log(Level.INFO, "Executing SELECT: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Result: {0} rows, Time: {1} ms", new Object[]{((List<?>) result).size(), String.format("%.3f", durationMs)});
        return result;
    }

    private void selectWithOrderByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithOrderByString");
            String query = "SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithOrderByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithOrderByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithGroupByString");
            String query = "SELECT NAME userName, COUNT(*) userCount FROM USERS u GROUP BY userName ORDER BY userName";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectMinMaxAvgWithJoinAndGroupBy() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgWithJoinAndGroupBy");
            String query = "SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge " +
                    "FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                    "GROUP BY userName, transDate ORDER BY transDate DESC";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgWithJoinAndGroupBy: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgWithJoinAndGroupBy: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithMultipleInnerJoins() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithMultipleInnerJoins");
            String query = "SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName " +
                    "FROM USERS u " +
                    "INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                    "INNER JOIN USERS u2 ON u.ID = u2.ID " +
                    "LIMIT 10 OFFSET 5";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithMultipleInnerJoins: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithMultipleInnerJoins: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInCondition() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInCondition");
            String query = "SELECT NAME userName, USER_CODE code FROM USERS u WHERE userName IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInCondition: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInCondition: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeCondition() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeCondition");
            String query = "SELECT NAME userName, USER_CODE code FROM USERS u WHERE userName LIKE 'User50%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeCondition: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeCondition: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithOrderByStringAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithOrderByStringAs");
            String query = "SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithOrderByStringAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithOrderByStringAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithGroupByStringAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithGroupByStringAs");
            String query = "SELECT NAME AS userName, COUNT(*) AS userCount FROM USERS u GROUP BY userName ORDER BY userName";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithGroupByStringAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithGroupByStringAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectMinMaxAvgWithJoinAndGroupByAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgWithJoinAndGroupByAs");
            String query = "SELECT u.NAME AS userName, t.TRANS_DATE AS transDate, MIN(u.AGE) AS minAge, MAX(u.AGE) AS maxAge, AVG(u.AGE) AS avgAge " +
                    "FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                    "GROUP BY userName, transDate ORDER BY transDate DESC";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgWithJoinAndGroupByAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgWithJoinAndGroupByAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithMultipleInnerJoinsAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithMultipleInnerJoinsAs");
            String query = "SELECT u.NAME AS userName, t.AMOUNT AS transAmount, u2.NAME AS refName " +
                    "FROM USERS u " +
                    "INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                    "INNER JOIN USERS u2 ON u.ID = u2.ID " +
                    "LIMIT 10 OFFSET 5";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithMultipleInnerJoinsAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithMultipleInnerJoinsAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInConditionAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInConditionAs");
            String query = "SELECT u.NAME AS userName, u.USER_CODE AS code FROM USERS u WHERE u.userName IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInConditionAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInConditionAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeConditionAs() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeConditionAs");
            String query = "SELECT u.NAME AS userName, u.USER_CODE AS code FROM USERS u WHERE u.userName LIKE 'User50%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeConditionAs: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeConditionAs: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void restoreTableState() {
        createUsersTable();
        createTransactionsTable();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertUsersRecords();
        insertTransactionsRecords();
    }

    public static void main(String[] args) {
        AliasesTest test = new AliasesTest();
        test.runTests();
    }
}