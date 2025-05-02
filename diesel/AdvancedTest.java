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
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();

            // Step 3: Insert records
            insertRecords();

            // Step 4: Run INSERT queries
            insertWithUniqueIndex();
            try {
                insertWithDuplicateUniqueIndex();
                throw new RuntimeException("insertWithDuplicateUniqueIndex succeeded unexpectedly, test failed");
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IllegalStateException) {
                    LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicateUniqueIndex: {0}", e.getCause().getMessage());
                } else {
                    LOGGER.log(Level.SEVERE, "Unexpected error in insertWithDuplicateUniqueIndex: {0}", e.getMessage());
                    throw e;
                }
            }
            insertWithUniqueClusteredIndex();
            try {
                insertWithDuplicateUniqueClusteredIndex();
                throw new RuntimeException("insertWithDuplicateUniqueClusteredIndex succeeded unexpectedly, test failed");
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IllegalStateException) {
                    LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicateUniqueClusteredIndex: {0}", e.getCause().getMessage());
                } else {
                    LOGGER.log(Level.SEVERE, "Unexpected error in insertWithDuplicateUniqueClusteredIndex: {0}", e.getMessage());
                    throw e;
                }
            }
            insertWithPrimaryKey();
            try {
                insertWithDuplicatePrimaryKey();
                throw new RuntimeException("insertWithDuplicatePrimaryKey succeeded unexpectedly, test failed");
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IllegalStateException) {
                    LOGGER.log(Level.INFO, "Expected failure in insertWithDuplicatePrimaryKey: {0}", e.getCause().getMessage());
                } else {
                    LOGGER.log(Level.SEVERE, "Unexpected error in insertWithDuplicatePrimaryKey: {0}", e.getMessage());
                    throw e;
                }
            }

            // Step 5: Run SELECT queries
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
        dropTable();
        String createTableQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
        database.executeQuery(createTableQuery, null);
        LOGGER.log(Level.INFO, "Table USERS created");
    }

    private void createUniqueIndex() {
        String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
        LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.log(Level.INFO, "Unique index created on ID");
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

    private void createUniqueClusteredIndex() {
        String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
        LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.log(Level.INFO, "Unique clustered index created on USER_CODE");
    }

    private void insertRecords() {
        LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
        List<String> columns = Arrays.asList("ID", "USER_CODE", "NAME", "AGE", "BALANCE");
        Table table = database.getTable("USERS");
        Random random = new Random();

        long startTime = System.nanoTime();
        for (int i = 1; i <= RECORD_COUNT; i++) {
            List<Object> values = new ArrayList<>();
            values.add(String.valueOf(i)); // ID
            values.add("CODE" + i); // USER_CODE
            values.add("User" + i); // NAME
            values.add(18 + (i % 82)); // AGE (18-99)
            values.add(new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP)); // BALANCE

            InsertQuery insertQuery = new InsertQuery(columns, values);
            insertQuery.execute(table);
            table.saveToFile("USERS");
        }
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
    }

    private void insertWithUniqueIndex() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1001', 'CODE1001', 'User1001', 25, 1500.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with unique index: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with unique index completed in {0} ms", String.format("%.3f", durationMs));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert with unique index failed: {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicateUniqueIndex() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1001', 'CODE1001DUP', 'User1001Duplicate', 25, 1500.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate unique index: {0}", query);
        long startTime = System.nanoTime();
        database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.WARNING, "Insert with duplicate unique index succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
    }

    private void insertWithUniqueClusteredIndex() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1002', 'CODE1002', 'User1002', 26, 1600.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with unique clustered index: {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with unique clustered index completed in {0} ms", String.format("%.3f", durationMs));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert with unique clustered index failed: {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicateUniqueClusteredIndex() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1003', 'CODE1002', 'User1002Duplicate', 26, 1600.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate unique clustered index: {0}", query);
        long startTime = System.nanoTime();
        database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.WARNING, "Insert with duplicate unique clustered index succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
    }

    private void insertWithPrimaryKey() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1004', 'CODE1004', 'User1004', 27, 1700.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with unique index (ID): {0}", query);
        long startTime = System.nanoTime();
        try {
            database.executeQuery(query, null);
            database.getTable("USERS").saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Insert with unique index (ID) completed in {0} ms", String.format("%.3f", durationMs));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert with unique index (ID) failed: {0}", e.getMessage());
            throw e;
        }
    }

    private void insertWithDuplicatePrimaryKey() {
        String query = "INSERT INTO USERS (ID, USER_CODE, NAME, AGE, BALANCE) VALUES ('1004', 'CODE1004DUP', 'User1004Duplicate', 27, 1700.00)";
        LOGGER.log(Level.INFO, "Executing INSERT with duplicate unique index (ID): {0}", query);
        long startTime = System.nanoTime();
        database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        LOGGER.log(Level.WARNING, "Insert with duplicate unique index (ID) succeeded unexpectedly in {0} ms", String.format("%.3f", durationMs));
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
        database.getTable("USERS").saveToFile("USERS");
    }

    private void executeDeleteQuery(String query) {
        LOGGER.log(Level.INFO, "Executing DELETE: {0}", query);
        long startTime = System.nanoTime();
        try {
            Object result = database.executeQuery(query, null);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            int deletedRows = (result instanceof Integer) ? (Integer) result : 0;
            LOGGER.log(Level.INFO, "Deleted {0} rows in {1} ms", new Object[]{deletedRows, String.format("%.3f", durationMs)});
            database.getTable("USERS").saveToFile("USERS");
            // Drop and recreate table to ensure clean state before re-inserting records
            try {
                createTable(); // This calls dropTable() internally
                createUniqueIndex();
                createBTreeIndex();
                createHashIndex();
                createUniqueClusteredIndex();
                insertRecords();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to restore table state after DELETE: {0}", e.getMessage());
                // Continue execution to avoid stopping tests
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "DELETE query failed: {0}", e.getMessage());
            // Continue execution to avoid stopping tests
        }
    }

    private void selectWithoutWhere() {
        String query = "SELECT ID, USER_CODE, NAME, AGE, BALANCE FROM USERS";
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

    private void selectWithWhereUniqueIndex() {
        String query = "SELECT ID, NAME FROM USERS WHERE ID = '500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereUniqueClusteredIndex() {
        String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500'";
        executeSelectQuery(query);
    }

    private void selectWithWherePrimaryKey() {
        String query = "SELECT ID, NAME FROM USERS WHERE ID = '500'";
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

    private void selectWithWhereUniqueBTreeHashIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereUniqueClusteredBTreeHashIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWherePrimaryKeyBTreeHashIndexed() {
        String query = "SELECT ID, NAME FROM USERS WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeSelectQuery(query);
    }

    private void selectWithWhereUniqueBTreeHashIndexedInParentheses() {
        String query = "SELECT ID, NAME FROM USERS WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
        executeSelectQuery(query);
    }

    private void selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        String query = "SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
        executeSelectQuery(query);
    }

    private void selectWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        String query = "SELECT ID, NAME FROM USERS WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
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

    private void updateWithWhereUniqueIndex() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = '500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereUniqueClusteredIndex() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500'";
        executeUpdateQuery(query);
    }

    private void updateWithWherePrimaryKey() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = '500'";
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

    private void updateWithWhereUniqueBTreeHashIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereUniqueClusteredBTreeHashIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWherePrimaryKeyBTreeHashIndexed() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeUpdateQuery(query);
    }

    private void updateWithWhereUniqueBTreeHashIndexedInParentheses() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
        executeUpdateQuery(query);
    }

    private void updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
        executeUpdateQuery(query);
    }

    private void updateWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        String query = "UPDATE USERS SET BALANCE = 6000 WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
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
    }

    private void deleteWithWhereHashIndex() {
        String query = "DELETE FROM USERS WHERE NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereBTreeIndex() {
        String query = "DELETE FROM USERS WHERE AGE = 50";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueIndex() {
        String query = "DELETE FROM USERS WHERE ID = '500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueClusteredIndex() {
        String query = "DELETE FROM USERS WHERE USER_CODE = 'CODE500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWherePrimaryKey() {
        String query = "DELETE FROM USERS WHERE ID = '500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereIndexedAndNonIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 AND BALANCE > 5000";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereIndexedAndNonIndexedInParentheses() {
        String query = "DELETE FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        String query = "DELETE FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereTwoIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 AND NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueBTreeHashIndexed() {
        String query = "DELETE FROM USERS WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueClusteredBTreeHashIndexed() {
        String query = "DELETE FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWherePrimaryKeyBTreeHashIndexed() {
        String query = "DELETE FROM USERS WHERE ID = '500' AND AGE = 50 AND NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueBTreeHashIndexedInParentheses() {
        String query = "DELETE FROM USERS WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        String query = "DELETE FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')";
        executeDeleteQuery(query);
    }

    private void deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        String query = "DELETE FROM USERS WHERE (ID = '500') AND (AGE = 50) AND (NAME = 'User500')";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereIndexedOrIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 OR NAME = 'User500'";
        executeDeleteQuery(query);
    }

    private void deleteWithWhereIndexedOrNonIndexed() {
        String query = "DELETE FROM USERS WHERE AGE = 50 OR BALANCE > 5000";
        executeDeleteQuery(query);
    }

    public static void main(String[] args) {
        AdvancedTest test = new AdvancedTest();
        test.runTests();
    }
}