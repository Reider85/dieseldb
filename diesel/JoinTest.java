package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class JoinTest {
    private static final Logger LOGGER = Logger.getLogger(JoinTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;

    public JoinTest() {
        this.database = new Database();
    }

    public void runTests() {
        try {
            // Step 1: Create tables and indexes, insert records
            createTables();
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();
            insertRecords();

            // Step 2: Run SELECT queries with JOIN
            selectWithJoinOnPrimaryKey();
            selectWithJoinOnBTreeIndex();
            selectWithJoinOnHashIndex();
            selectWithJoinOnUniqueIndex();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createTables() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createTables");
            dropTables();
            String createUsersTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
            String createDetailsTableQuery = "CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING)";
            LOGGER.log(Level.INFO, "Executing: {0}", createUsersTableQuery);
            database.executeQuery(createUsersTableQuery, null);
            LOGGER.log(Level.INFO, "Executing: {0}", createDetailsTableQuery);
            database.executeQuery(createDetailsTableQuery, null);
            LOGGER.log(Level.INFO, "Test createTables: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createTables: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void dropTables() {
        try {
            database.dropTable("USERS");
            database.dropTable("USER_DETAILS");
            LOGGER.log(Level.INFO, "Tables USERS and USER_DETAILS dropped");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Tables USERS or USER_DETAILS not found for dropping");
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

    private void insertRecords() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertRecords");
            LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
            Table usersTable = database.getTable("USERS");
            Table detailsTable = database.getTable("USER_DETAILS");

            long startTime = System.nanoTime();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                String userQuery = String.format(
                        "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                        i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP)
                );
                String detailQuery = String.format(
                        "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d')",
                        i, i, i, 18 + (i % 82), i
                );
                database.executeQuery(userQuery, null);
                database.executeQuery(detailQuery, null);
            }
            usersTable.saveToFile("USERS");
            detailsTable.saveToFile("USER_DETAILS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
            LOGGER.log(Level.INFO, "Test insertRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertRecords: FAIL - {0}", e.getMessage());
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

    private void restoreTableState() {
        createTables();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertRecords();
    }

    private void selectWithJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        JoinTest test = new JoinTest();
        test.runTests();
    }
}