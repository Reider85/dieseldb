package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.File;

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
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();

            // Step 3: Insert records
            insertRecords();

            // Step 4: Run INSERT queries
            insertWithSequencePrimaryKey();
            insertWithDuplicateSequencePrimaryKey();
            insertWithUniqueIndex();
            insertWithDuplicateUniqueIndex();
            insertWithUniqueClusteredIndex();
            insertWithDuplicateUniqueClusteredIndex();
            insertWithPrimaryKey();
            insertWithDuplicatePrimaryKey();

            // Step 5: Run SELECT queries
            selectWithWhereSequencePrimaryKey();
            selectWithWhereSequencePrimaryKeyBTreeHashIndexed();
            selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses();
            selectWithoutWhere();
            selectWithWhereNoIndex();
            selectWithWhereHashIndex();
            selectWithWhereBTreeIndex();
            selectWithWhereUniqueIndex();
            selectWithWhereUniqueClusteredIndex();
            selectWithWherePrimaryKey();
            selectWithWhereIndexedAndNonIndexed();
            selectWithWhereIndexedAndNonIndexedInParentheses();
            selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();
            selectWithWhereTwoIndexed();
            selectWithWhereUniqueBTreeHashIndexed();
            selectWithWhereUniqueClusteredBTreeHashIndexed();
            selectWithWherePrimaryKeyBTreeHashIndexed();
            selectWithWhereUniqueBTreeHashIndexedInParentheses();
            selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses();
            selectWithWherePrimaryKeyBTreeHashIndexedInParentheses();
            selectWithWhereIndexedOrIndexed();
            selectWithWhereIndexedOrNonIndexed();

            // Step 6: Run UPDATE queries
            updateWithWhereSequencePrimaryKey();
            updateWithWhereSequencePrimaryKeyBTreeHashIndexed();
            updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses();
            updateWithWhereNoIndex();
            updateWithWhereHashIndex();
            updateWithWhereBTreeIndex();
            updateWithWhereUniqueIndex();
            updateWithWhereUniqueClusteredIndex();
            updateWithWherePrimaryKey();
            updateWithWhereIndexedAndNonIndexed();
            updateWithWhereTwoIndexed();
            updateWithWhereUniqueBTreeHashIndexed();
            updateWithWhereUniqueClusteredBTreeHashIndexed();
            updateWithWherePrimaryKeyBTreeHashIndexed();
            updateWithWhereUniqueBTreeHashIndexedInParentheses();
            updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses();
            updateWithWherePrimaryKeyBTreeHashIndexedInParentheses();
            updateWithWhereIndexedOrIndexed();
            updateWithWhereIndexedOrNonIndexed();
            updateWithWhereIndexedAndNonIndexedInParentheses();
            updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();

            // Step 7: Run DELETE queries
            deleteWithWhereSequencePrimaryKey();
            deleteWithWhereSequencePrimaryKeyBTreeHashIndexed();
            deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses();
            deleteWithWhereNoIndex();
            deleteWithWhereHashIndex();
            deleteWithWhereBTreeIndex();
            deleteWithWhereUniqueIndex();
            deleteWithWhereUniqueClusteredIndex();
            deleteWithWherePrimaryKey();
            deleteWithWhereIndexedAndNonIndexed();
            deleteWithWhereIndexedAndNonIndexedInParentheses();
            deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces();
            deleteWithWhereTwoIndexed();
            deleteWithWhereUniqueBTreeHashIndexed();
            deleteWithWhereUniqueClusteredBTreeHashIndexed();
            deleteWithWherePrimaryKeyBTreeHashIndexed();
            deleteWithWhereUniqueBTreeHashIndexedInParentheses();
            deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses();
            deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses();
            deleteWithWhereIndexedOrIndexed();
            deleteWithWhereIndexedOrNonIndexed();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createTable");
            dropTable();
            String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Table USERS created with SEQUENCE on ID");
            LOGGER.log(Level.INFO, "Test createTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUniqueIndex");
            String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Unique index created on ID");
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
            LOGGER.log(Level.INFO, "B-tree index created on AGE");
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
            LOGGER.log(Level.INFO, "Hash index created on NAME");
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
            LOGGER.log(Level.INFO, "Unique index created on USER_CODE");
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
            Table table = database.getTable("USERS");
            Random random = new Random();

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
            LOGGER.log(Level.INFO, "Test insertRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertRecords: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithSequencePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertWithSequencePrimaryKey");
            String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1001', 'User1001', 25, 1500.00)";
            LOGGER.log(Level.INFO, "Executing INSERT with SEQUENCE primary key: {0}", query);
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with SEQUENCE primary key completed in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.INFO, "Test insertWithSequencePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertWithSequencePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicateSequencePrimaryKey() {
        LOGGER.log(Level.INFO, "Starting test: insertWithDuplicateSequencePrimaryKey");
        LOGGER.log(Level.INFO, "Note: Sequence on ID ensures unique values, preventing duplicate primary key insertion");
        String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1001DUP', 'User1001Duplicate', 25, 1500.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with SEQUENCE primary key: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with SEQUENCE primary key completed in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.INFO, "Test insertWithDuplicateSequencePrimaryKey: OK");
        } catch (Exception e) {
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.SEVERE, "Test insertWithDuplicateSequencePrimaryKey: FAIL - Unexpected error: {0} in {1} ms", new Object[]{e.getMessage(), String.format("%.3f", durationMs)});
            throw new RuntimeException("Unexpected error in insertWithDuplicateSequencePrimaryKey: " + e.getMessage(), e);
        }
    }

    private void insertWithUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertWithUniqueIndex");
            String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1002', 'User1002', 25, 1500.00)";
            LOGGER.log(Level.INFO, "Executing INSERT with unique index: {0}", query);
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with unique index completed in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.INFO, "Test insertWithUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertWithUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicateUniqueIndex() {
        LOGGER.log(Level.INFO, "Starting test: insertWithDuplicateUniqueIndex");
        // Test duplicate USER_CODE instead of ID, as ID is sequence-based
        String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1002', 'User1002Duplicate', 25, 1500.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate USER_CODE: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.WARNING, "Insert with duplicate USER_CODE succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.SEVERE, "Test insertWithDuplicateUniqueIndex: FAIL - insert succeeded unexpectedly");
            throw new RuntimeException("insertWithDuplicateUniqueIndex succeeded unexpectedly, test failed");
        } catch (RuntimeException e) {
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            if (e.getCause() != null && e.getCause() instanceof IllegalStateException) {
                LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicateUniqueIndex: {0} in {1} ms", new Object[]{e.getCause().getMessage(), String.format("%.3f", durationMs)});
                LOGGER.log(Level.INFO, "Test insertWithDuplicateUniqueIndex: OK - Expected failure: {0}", e.getCause().getMessage());
            } else {
                LOGGER.log(Level.SEVERE, "Test insertWithDuplicateUniqueIndex: FAIL - Unexpected error: {0} in {1} ms", new Object[]{e.getMessage(), String.format("%.3f", durationMs)});
                throw e;
            }
        }
    }

    private void insertWithUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertWithUniqueClusteredIndex");
            String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1003', 'User1003', 26, 1600.00)";
            LOGGER.log(Level.INFO, "Executing INSERT with unique index: {0}", query);
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with unique index completed in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.INFO, "Test insertWithUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertWithUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicateUniqueClusteredIndex() {
        LOGGER.log(Level.INFO, "Starting test: insertWithDuplicateUniqueClusteredIndex");
        String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1003', 'User1003Duplicate', 26, 1600.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate USER_CODE: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.WARNING, "Insert with duplicate USER_CODE succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.SEVERE, "Test insertWithDuplicateUniqueClusteredIndex: FAIL - insert succeeded unexpectedly");
            throw new RuntimeException("insertWithDuplicateUniqueClusteredIndex succeeded unexpectedly, test failed");
        } catch (RuntimeException e) {
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            if (e.getCause() != null && e.getCause() instanceof IllegalStateException) {
                LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicateUniqueClusteredIndex: {0} in {1} ms", new Object[]{e.getCause().getMessage(), String.format("%.3f", durationMs)});
                LOGGER.log(Level.INFO, "Test insertWithDuplicateUniqueClusteredIndex: OK - Expected failure: {0}", e.getCause().getMessage());
            } else {
                LOGGER.log(Level.SEVERE, "Test insertWithDuplicateUniqueClusteredIndex: FAIL - Unexpected error: {0} in {1} ms", new Object[]{e.getMessage(), String.format("%.3f", durationMs)});
                throw e;
            }
        }
    }

    private void insertWithPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertWithPrimaryKey");
            String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1004', 'User1004', 27, 1700.00)";
            LOGGER.log(Level.INFO, "Executing INSERT with SEQUENCE primary key: {0}", query);
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with SEQUENCE primary key completed in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.INFO, "Test insertWithPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertWithPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicatePrimaryKey() {
        LOGGER.log(Level.INFO, "Starting test: insertWithDuplicatePrimaryKey");
        // Test duplicate USER_CODE instead of ID, as ID is sequence-based
        String query = "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1004', 'User1004Duplicate', 27, 1700.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate USER_CODE: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.WARNING, "Insert with duplicate USER_CODE succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
            LOGGER.log(Level.SEVERE, "Test insertWithDuplicatePrimaryKey: FAIL - insert succeeded unexpectedly");
            throw new RuntimeException("insertWithDuplicatePrimaryKey succeeded unexpectedly, test failed");
        } catch (RuntimeException e) {
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            if (e.getCause() != null && e.getCause() instanceof IllegalStateException) {
                LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicatePrimaryKey: {0} in {1} ms", new Object[]{e.getCause().getMessage(), String.format("%.3f", durationMs)});
                LOGGER.log(Level.INFO, "Test insertWithDuplicatePrimaryKey: OK - Expected failure: {0}", e.getCause().getMessage());
            } else {
                LOGGER.log(Level.SEVERE, "Test insertWithDuplicatePrimaryKey: FAIL - Unexpected error: {0} in {1} ms", new Object[]{e.getMessage(), String.format("%.3f", durationMs)});
                throw e;
            }
        }
    }
    private void dropTable() {
        try {
            database.dropTable("USERS");
            // Delete the associated CSV file
            String fileName = "USERS.csv";
            File file = new File(fileName);
            if (file.exists() && !file.delete()) {
                LOGGER.log(Level.WARNING, "Failed to delete table file: {0}", fileName);
                throw new RuntimeException("Failed to delete table file: " + fileName);
            }
            // Delete the serialized table file if it exists
            String tableFileName = "USERS.table";
            File tableFile = new File(tableFileName);
            if (tableFile.exists() && !tableFile.delete()) {
                LOGGER.log(Level.WARNING, "Failed to delete serialized table file: {0}", tableFileName);
                throw new RuntimeException("Failed to delete serialized table file: " + tableFileName);
            }
            LOGGER.log(Level.INFO, "Table USERS dropped and files deleted");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table USERS not found for dropping");
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

    private void executeUpdateQuery(String query) {
        LOGGER.log(Level.INFO, "Executing UPDATE: {0}", query);
        long startTime = System.nanoTime();
        database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Update completed in {0} ms", String.format("%.3f", durationMs));
        database.getTable("USERS").saveToFile("USERS");
    }

    private Object executeDeleteQuery(String query) {
        LOGGER.log(Level.INFO, "Executing DELETE: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        int deletedRows = (result instanceof Integer) ? (Integer) result : 0;
        LOGGER.log(Level.INFO, "Deleted {0} rows in {1} ms", new Object[]{deletedRows, String.format("%.3f", durationMs)});
        database.getTable("USERS").saveToFile("USERS");
        return result;
    }

    private void restoreTableState() {
        createTable(); // This calls dropTable() internally
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertRecords();
    }

    private void selectWithWhereSequencePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereSequencePrimaryKey");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereSequencePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereSequencePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereSequencePrimaryKeyBTreeHashIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereSequencePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereSequencePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithoutWhere() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithoutWhere");
            String query = "SELECT ID, USER_CODE, NAME, AGE, BALANCE FROM USERS";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithoutWhere: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithoutWhere: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereNoIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereNoIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereNoIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereNoIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereHashIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereBTreeIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueClusteredIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWherePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWherePrimaryKey");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWherePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWherePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereIndexedAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereIndexedAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereIndexedAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereIndexedAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereIndexedAndNonIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereIndexedAndNonIndexedInParentheses");
            String query = "SELECT ID, NAME FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereIndexedAndNonIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereIndexedAndNonIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
            String query = "SELECT ID, NAME FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereTwoIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereTwoIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 AND NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereTwoIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereTwoIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueBTreeHashIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueClusteredBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueClusteredBTreeHashIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueClusteredBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueClusteredBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWherePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWherePrimaryKeyBTreeHashIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWherePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWherePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueBTreeHashIndexedInParentheses");
            String query = "SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
            String query = "SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWherePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWherePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWherePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereIndexedOrIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereIndexedOrIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR NAME = 'User500'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereIndexedOrIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereIndexedOrIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereIndexedOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereIndexedOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereIndexedOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereIndexedOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereSequencePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereSequencePrimaryKey");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereSequencePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereSequencePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereSequencePrimaryKeyBTreeHashIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereSequencePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereSequencePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereNoIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereNoIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereNoIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereNoIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereHashIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereBTreeIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueClusteredIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWherePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWherePrimaryKey");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWherePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWherePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereIndexedAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereIndexedAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereIndexedAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereIndexedAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereTwoIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereTwoIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereTwoIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereTwoIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueBTreeHashIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueClusteredBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueClusteredBTreeHashIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueClusteredBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueClusteredBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWherePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWherePrimaryKeyBTreeHashIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWherePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWherePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueBTreeHashIndexedInParentheses");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWherePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWherePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWherePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereIndexedOrIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereIndexedOrIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR NAME = 'User500'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereIndexedOrIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereIndexedOrIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereIndexedOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereIndexedOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereIndexedOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereIndexedOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereIndexedAndNonIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereIndexedAndNonIndexedInParentheses");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (AGE = 50) AND (BALANCE > 5000)";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereIndexedAndNonIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereIndexedAndNonIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereSequencePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereSequencePrimaryKey");
            String query = "DELETE FROM USERS WHERE ID = 500";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereSequencePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereSequencePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereSequencePrimaryKeyBTreeHashIndexed");
            String query = "DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereSequencePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereSequencePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereNoIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereNoIndex");
            String query = "DELETE FROM USERS WHERE BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereNoIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereNoIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereHashIndex");
            String query = "DELETE FROM USERS WHERE NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereBTreeIndex");
            String query = "DELETE FROM USERS WHERE AGE = 50";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueIndex");
            String query = "DELETE FROM USERS WHERE ID = 500";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueClusteredIndex");
            String query = "DELETE FROM USERS WHERE USER_CODE = 'CODE500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWherePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWherePrimaryKey");
            String query = "DELETE FROM USERS WHERE ID = 500";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWherePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWherePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereIndexedAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereIndexedAndNonIndexed");
            String query = "DELETE FROM USERS WHERE AGE = 50 AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereIndexedAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereIndexedAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereIndexedAndNonIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereIndexedAndNonIndexedInParentheses");
            String query = "DELETE FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereIndexedAndNonIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereIndexedAndNonIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
            String query = "DELETE FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereTwoIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereTwoIndexed");
            String query = "DELETE FROM USERS WHERE AGE = 50 AND NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereTwoIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereTwoIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueBTreeHashIndexed");
            String query = "DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueClusteredBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueClusteredBTreeHashIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueClusteredBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueClusteredBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWherePrimaryKeyBTreeHashIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWherePrimaryKeyBTreeHashIndexed");
            String query = "DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWherePrimaryKeyBTreeHashIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWherePrimaryKeyBTreeHashIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueBTreeHashIndexedInParentheses");
            String query = "DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
            String query = "DELETE FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses");
            String query = "DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereIndexedOrIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereIndexedOrIndexed");
            String query = "DELETE FROM USERS WHERE AGE = 50 OR NAME = 'User500'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereIndexedOrIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereIndexedOrIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereIndexedOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereIndexedOrNonIndexed");
            String query = "DELETE FROM USERS WHERE AGE = 50 OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereIndexedOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereIndexedOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        AdvancedTest test = new AdvancedTest();
        test.runTests();
    }
}