package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class AdvancedTest {
    private static final Logger LOGGER = Logger.getLogger(AdvancedTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;

    public AdvancedTest() {
        this.database = new Database();
    }

    public void runTests() {
        try {
            // Step 1: Create table
            createTable();

            // Step 2: Create indexes
            createBTreeIndex();
            createHashIndex();

            // Step 3: Insert records
            insertRecords();

            // Step 4: Run SELECT queries
            selectWithoutWhere();
            selectWithWhereNoIndex();
            selectWithWhereHashIndex();
            selectWithWhereBTreeIndex();
            selectWithWhereIndexedAndNonIndexed();
            selectWithWhereIndexedAndNonIndexedInParentheses();
            selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();
            selectWithWhereTwoIndexed();
            selectWithWhereIndexedOrIndexed();
            selectWithWhereIndexedOrNonIndexed();

            // Step 5: Run UPDATE queries
            updateWithWhereNoIndex();
            updateWithWhereHashIndex();
            updateWithWhereBTreeIndex();
            updateWithWhereIndexedAndNonIndexed();
            updateWithWhereTwoIndexed();
            updateWithWhereIndexedOrIndexed();
            updateWithWhereIndexedOrNonIndexed();
            updateWithWhereIndexedAndNonIndexedInParentheses();
            updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();

            // Step 6: Run DELETE queries
            deleteWithWhereNoIndex();
            deleteWithWhereHashIndex();
            deleteWithWhereBTreeIndex();
            deleteWithWhereIndexedAndNonIndexed();
            deleteWithWhereIndexedAndNonIndexedInParentheses();
            deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();
            deleteWithWhereTwoIndexed();
            deleteWithWhereIndexedOrIndexed();
            deleteWithWhereIndexedOrNonIndexed();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createTable() {
        dropTable();
        String createTableQuery = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
        database.executeQuery(createTableQuery, null);
        LOGGER.log(Level.INFO, "Table USERS created");
    }

    private void createBTreeIndex() {
        String createIndexQuery = "CREATE INDEX ON USERS (AGE)";
        LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.log(Level.INFO, "B-tree index created on AGE");
    }

    private void createHashIndex() {
        String createIndexQuery = "CREATE HASH INDEX ON USERS (NAME)";
        LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.log(Level.INFO, "Hash index created on NAME");
    }

    private void insertRecords() {
        LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
        List<String> columns = Arrays.asList("ID", "NAME", "AGE", "BALANCE");
        Table table = database.getTable("USERS");
        Random random = new Random();

        long startTime = System.nanoTime();
        for (int i = 1; i <= RECORD_COUNT; i++) {
            List<Object> values = new ArrayList<>();
            values.add(String.valueOf(i));
            values.add("User" + i);
            values.add(18 + (i % 82)); // Ages between 18 and 99
            values.add(new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP));

            InsertQuery insertQuery = new InsertQuery(columns, values);
            insertQuery.execute(table);
            table.saveToFile("USERS");
        }
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
            LOGGER.log(Level.INFO, "Table USERS dropped");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table USERS not found for dropping");
        }
    }

    private void executeSelectQuery(String query) {
        LOGGER.log(Level.INFO, "Executing SELECT: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Result: {0} rows, Time: {1} ms", new Object[]{((List<?>) result).size(), String.format("%.3f", durationMs)});
    }

    private void executeUpdateQuery(String query) {
        LOGGER.log(Level.INFO, "Executing UPDATE: {0}", query);
        long startTime = System.nanoTime();
        database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Update completed in {0} ms", String.format("%.3f", durationMs));
    }

    private void executeDeleteQuery(String query) {
        LOGGER.log(Level.INFO, "Executing DELETE: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        int deletedRows = (result instanceof Integer) ? (Integer) result : 0;
        LOGGER.log(Level.INFO, "Deleted {0} rows in {1} ms", new Object[]{deletedRows, String.format("%.3f", durationMs)});
    }

    private void selectWithoutWhere() {
        String query = "SELECT ID, NAME, AGE, BALANCE FROM USERS";
        executeSelectQuery(query);
    }

    private void selectWithWhereNoIndex() {
        String query = "SELECT ID, NAME FROM USERS WHERE BALANCE > 5000";
        executeSelectQuery(query);
    }

    private void selectWithWhereHashIndex() {
        String query = "SELECT ID, NAME FROM USERS WHERE NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereBTreeIndex() {
        String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50";
        executeSelectQuery(query);
    }

    private void selectWithWhereIndexedAndNonIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 AND BALANCE > 5000";
        executeSelectQuery(query);
    }

    private void selectWithWhereIndexedAndNonIndexedInParentheses() {
        String query = "SELECT ID, NAME FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)";
        executeSelectQuery(query);
    }

    private void selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        String query = "SELECT ID, NAME FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
        executeSelectQuery(query);
    }

    private void selectWithWhereTwoIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 AND NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereIndexedOrIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereIndexedOrNonIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000";
        executeSelectQuery(query);
    }

    private void updateWithWhereNoIndex() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE BALANCE > 5000";
        executeUpdateQuery(query);
    }

    private void updateWithWhereHashIndex() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereBTreeIndex() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50";
        executeUpdateQuery(query);
    }

    private void updateWithWhereIndexedAndNonIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND BALANCE > 5000";
        executeUpdateQuery(query);
    }

    private void updateWithWhereTwoIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereIndexedOrIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereIndexedOrNonIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR BALANCE > 5000";
        executeUpdateQuery(query);
    }

    private void updateWithWhereIndexedAndNonIndexedInParentheses() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE (AGE = 50) AND (BALANCE > 5000)";
        executeUpdateQuery(query);
    }

    private void updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
        executeUpdateQuery(query);
    }

    private void deleteWithWhereNoIndex() {
        String query = "DELETE FROM USERS WHERE BALANCE > 5000";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereHashIndex() {
        String query = "DELETE FROM USERS WHERE NAME = 'User500'";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereBTreeIndex() {
        String query = "DELETE FROM USERS WHERE AGE = 50";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereIndexedAndNonIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 AND BALANCE > 5000";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereIndexedAndNonIndexedInParentheses() {
        String query = "DELETE FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        String query = "DELETE FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereTwoIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 AND NAME = 'User500'";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereIndexedOrIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 OR NAME = 'User500'";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    private void deleteWithWhereIndexedOrNonIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 OR BALANCE > 5000";
        executeDeleteQuery(query);
        // Re-insert records to ensure table state for subsequent tests
        insertRecords();
    }

    public static void main(String[] args) {
        AdvancedTest test = new AdvancedTest();
        test.runTests();
    }
}