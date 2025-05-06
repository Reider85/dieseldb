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

            // Step 2: Run SELECT queries with various JOIN types
            // INNER JOIN
            selectWithInnerJoinOnPrimaryKey();
            selectWithInnerJoinOnBTreeIndex();
            selectWithInnerJoinOnHashIndex();
            selectWithInnerJoinOnUniqueIndex();
            selectWithInnerJoinOnNonIndexedField();

            // LEFT INNER JOIN
            selectWithLeftInnerJoinOnPrimaryKey();
            selectWithLeftInnerJoinOnBTreeIndex();
            selectWithLeftInnerJoinOnHashIndex();
            selectWithLeftInnerJoinOnUniqueIndex();
            selectWithLeftInnerJoinOnNonIndexedField();

            // RIGHT INNER JOIN
            selectWithRightInnerJoinOnPrimaryKey();
            selectWithRightInnerJoinOnBTreeIndex();
            selectWithRightInnerJoinOnHashIndex();
            selectWithRightInnerJoinOnUniqueIndex();
            selectWithRightInnerJoinOnNonIndexedField();

            // LEFT OUTER JOIN
            selectWithLeftOuterJoinOnPrimaryKey();
            selectWithLeftOuterJoinOnBTreeIndex();
            selectWithLeftOuterJoinOnHashIndex();
            selectWithLeftOuterJoinOnUniqueIndex();
            selectWithLeftOuterJoinOnNonIndexedField();

            // RIGHT OUTER JOIN
            selectWithRightOuterJoinOnPrimaryKey();
            selectWithRightOuterJoinOnBTreeIndex();
            selectWithRightOuterJoinOnHashIndex();
            selectWithRightOuterJoinOnUniqueIndex();
            selectWithRightOuterJoinOnNonIndexedField();

            // LEFT JOIN
            selectWithLeftJoinOnPrimaryKey();
            selectWithLeftJoinOnBTreeIndex();
            selectWithLeftJoinOnHashIndex();
            selectWithLeftJoinOnUniqueIndex();
            selectWithLeftJoinOnNonIndexedField();

            // RIGHT JOIN
            selectWithRightJoinOnPrimaryKey();
            selectWithRightJoinOnBTreeIndex();
            selectWithRightJoinOnHashIndex();
            selectWithRightJoinOnUniqueIndex();
            selectWithRightJoinOnNonIndexedField();

            // FULL JOIN
            selectWithFullJoinOnPrimaryKey();
            selectWithFullJoinOnBTreeIndex();
            selectWithFullJoinOnHashIndex();
            selectWithFullJoinOnUniqueIndex();
            selectWithFullJoinOnNonIndexedField();

            // FULL OUTER JOIN
            selectWithFullOuterJoinOnPrimaryKey();
            selectWithFullOuterJoinOnBTreeIndex();
            selectWithFullOuterJoinOnHashIndex();
            selectWithFullOuterJoinOnUniqueIndex();
            selectWithFullOuterJoinOnNonIndexedField();

            // CROSS JOIN
            selectWithCrossJoinOnPrimaryKey();
            selectWithCrossJoinOnBTreeIndex();
            selectWithCrossJoinOnHashIndex();
            selectWithCrossJoinOnUniqueIndex();
            selectWithCrossJoinOnNonIndexedField();

            // New JOIN tests with AND and OR in ON clause
            selectWithInnerJoinOnPrimaryKeyWithAndOr();
            selectWithInnerJoinOnBTreeIndexWithAndOr();
            selectWithInnerJoinOnHashIndexWithAndOr();
            selectWithInnerJoinOnUniqueIndexWithAndOr();
            selectWithInnerJoinOnNonIndexedFieldWithAndOr();

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
            String createDetailsTableQuery = "CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)";
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
                BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP);
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

    // INNER JOIN Tests
    private void selectWithInnerJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // LEFT INNER JOIN Tests
    private void selectWithLeftInnerJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftInnerJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftInnerJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftInnerJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftInnerJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftInnerJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftInnerJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftInnerJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftInnerJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftInnerJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftInnerJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftInnerJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftInnerJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftInnerJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftInnerJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftInnerJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftInnerJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftInnerJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftInnerJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftInnerJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // RIGHT INNER JOIN Tests
    private void selectWithRightInnerJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightInnerJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightInnerJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightInnerJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightInnerJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightInnerJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightInnerJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightInnerJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightInnerJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightInnerJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightInnerJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightInnerJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightInnerJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightInnerJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightInnerJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightInnerJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightInnerJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightInnerJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightInnerJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightInnerJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // LEFT OUTER JOIN Tests
    private void selectWithLeftOuterJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftOuterJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftOuterJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftOuterJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftOuterJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftOuterJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftOuterJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftOuterJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftOuterJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftOuterJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftOuterJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftOuterJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftOuterJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftOuterJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftOuterJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftOuterJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftOuterJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftOuterJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftOuterJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftOuterJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // RIGHT OUTER JOIN Tests
    private void selectWithRightOuterJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightOuterJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightOuterJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightOuterJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightOuterJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightOuterJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightOuterJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightOuterJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightOuterJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightOuterJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightOuterJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightOuterJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightOuterJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightOuterJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightOuterJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightOuterJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightOuterJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightOuterJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightOuterJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightOuterJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // LEFT JOIN Tests
    private void selectWithLeftJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLeftJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLeftJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS LEFT JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLeftJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLeftJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // RIGHT JOIN Tests
    private void selectWithRightJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithRightJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithRightJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS RIGHT JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithRightJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithRightJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // FULL JOIN Tests
    private void selectWithFullJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // FULL OUTER JOIN Tests
    private void selectWithFullOuterJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullOuterJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullOuterJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullOuterJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullOuterJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullOuterJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullOuterJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullOuterJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullOuterJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullOuterJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullOuterJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullOuterJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullOuterJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullOuterJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullOuterJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullOuterJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithFullOuterJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithFullOuterJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithFullOuterJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithFullOuterJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // CROSS JOIN Tests
    private void selectWithCrossJoinOnPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithCrossJoinOnPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS CROSS JOIN USER_DETAILS " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithCrossJoinOnPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithCrossJoinOnPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithCrossJoinOnBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithCrossJoinOnBTreeIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS CROSS JOIN USER_DETAILS " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithCrossJoinOnBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithCrossJoinOnBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithCrossJoinOnHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithCrossJoinOnHashIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS CROSS JOIN USER_DETAILS " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithCrossJoinOnHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithCrossJoinOnHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithCrossJoinOnUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithCrossJoinOnUniqueIndex");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS CROSS JOIN USER_DETAILS " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithCrossJoinOnUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithCrossJoinOnUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithCrossJoinOnNonIndexedField() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithCrossJoinOnNonIndexedField");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS CROSS JOIN USER_DETAILS " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithCrossJoinOnNonIndexedField: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithCrossJoinOnNonIndexedField: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // New JOIN Tests with AND and OR in ON clause
    private void selectWithInnerJoinOnPrimaryKeyWithAndOr() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnPrimaryKeyWithAndOr");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME OR USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnPrimaryKeyWithAndOr: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnPrimaryKeyWithAndOr: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnBTreeIndexWithAndOr() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnBTreeIndexWithAndOr");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE AND USERS.NAME = USER_DETAILS.NAME OR USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnBTreeIndexWithAndOr: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnBTreeIndexWithAndOr: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnHashIndexWithAndOr() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnHashIndexWithAndOr");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME AND USERS.AGE = USER_DETAILS.AGE OR USERS.USER_CODE = USER_DETAILS.USER_CODE " +
                    "WHERE USERS.NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnHashIndexWithAndOr: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnHashIndexWithAndOr: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnUniqueIndexWithAndOr() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnUniqueIndexWithAndOr");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.AGE = USER_DETAILS.AGE OR USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnUniqueIndexWithAndOr: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnUniqueIndexWithAndOr: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithInnerJoinOnNonIndexedFieldWithAndOr() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithInnerJoinOnNonIndexedFieldWithAndOr");
            String query = "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                    "FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.AGE = USER_DETAILS.AGE OR USERS.NAME = USER_DETAILS.NAME " +
                    "WHERE USERS.BALANCE = 5100.00";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithInnerJoinOnNonIndexedFieldWithAndOr: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithInnerJoinOnNonIndexedFieldWithAndOr: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        JoinTest test = new JoinTest();
        test.runTests();
    }
}