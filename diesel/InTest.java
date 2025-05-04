package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class InTest {
    private static final Logger LOGGER = Logger.getLogger(InTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;

    public InTest() {
        this.database = new Database();
    }

    public void runTests() {
        try {
            // Step 1: Create table and indexes, insert records
            createTable();
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();
            insertRecords();

            // Step 2: Run SELECT queries with IN
            selectWithWhereInBTreeIndex();
            selectWithWhereInHashIndex();
            selectWithWhereInUniqueIndex();
            selectWithWhereInUniqueClusteredIndex();
            selectWithWhereInPrimaryKey();
            selectWithWhereInBTreeIndexAndNonIndexed();
            selectWithWhereInHashIndexAndNonIndexed();
            selectWithWhereInUniqueIndexAndNonIndexed();
            selectWithWhereInUniqueClusteredIndexAndNonIndexed();
            selectWithWhereInPrimaryKeyAndNonIndexed();
            selectWithWhereInBTreeIndexOrNonIndexed();
            selectWithWhereInHashIndexOrNonIndexed();
            selectWithWhereInUniqueIndexOrNonIndexed();
            selectWithWhereInUniqueClusteredIndexOrNonIndexed();
            selectWithWhereInPrimaryKeyOrNonIndexed();

            // Step 3: Run UPDATE queries with IN
            updateWithWhereInBTreeIndex();
            updateWithWhereInHashIndex();
            updateWithWhereInUniqueIndex();
            updateWithWhereInUniqueClusteredIndex();
            updateWithWhereInPrimaryKey();
            updateWithWhereInBTreeIndexAndNonIndexed();
            updateWithWhereInHashIndexAndNonIndexed();
            updateWithWhereInUniqueIndexAndNonIndexed();
            updateWithWhereInUniqueClusteredIndexAndNonIndexed();
            updateWithWhereInPrimaryKeyAndNonIndexed();
            updateWithWhereInBTreeIndexOrNonIndexed();
            updateWithWhereInHashIndexOrNonIndexed();
            updateWithWhereInUniqueIndexOrNonIndexed();
            updateWithWhereInUniqueClusteredIndexOrNonIndexed();
            updateWithWhereInPrimaryKeyOrNonIndexed();

            // Step 4: Run DELETE queries with IN
            deleteWithWhereInBTreeIndex();
            deleteWithWhereInHashIndex();
            deleteWithWhereInUniqueIndex();
            deleteWithWhereInUniqueClusteredIndex();
            deleteWithWhereInPrimaryKey();
            deleteWithWhereInBTreeIndexAndNonIndexed();
            deleteWithWhereInHashIndexAndNonIndexed();
            deleteWithWhereInUniqueIndexAndNonIndexed();
            deleteWithWhereInUniqueClusteredIndexAndNonIndexed();
            deleteWithWhereInPrimaryKeyAndNonIndexed();
            deleteWithWhereInBTreeIndexOrNonIndexed();
            deleteWithWhereInHashIndexOrNonIndexed();
            deleteWithWhereInUniqueIndexOrNonIndexed();
            deleteWithWhereInUniqueClusteredIndexOrNonIndexed();
            deleteWithWhereInPrimaryKeyOrNonIndexed();

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
            LOGGER.log(Level.INFO, "Test createTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
            LOGGER.log(Level.INFO, "Table USERS dropped");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table USERS not found for dropping");
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
        createTable();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertRecords();
    }

    private void selectWithWhereInBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInBTreeIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInHashIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueClusteredIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInPrimaryKey");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502)";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInBTreeIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInHashIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueClusteredIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInPrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInPrimaryKeyAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInPrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInPrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInBTreeIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInHashIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInUniqueClusteredIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithWhereInPrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithWhereInPrimaryKeyOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithWhereInPrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithWhereInPrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInBTreeIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52)";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInHashIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502)";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueClusteredIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInPrimaryKey");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502)";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInBTreeIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52) AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInHashIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueClusteredIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInPrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInPrimaryKeyAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInPrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInPrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInBTreeIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52) OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInHashIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInUniqueClusteredIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithWhereInPrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithWhereInPrimaryKeyOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithWhereInPrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithWhereInPrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInBTreeIndex");
            String query = "DELETE FROM USERS WHERE AGE IN (50, 51, 52)";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInHashIndex");
            String query = "DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueIndex");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502)";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueClusteredIndex");
            String query = "DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInPrimaryKey");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502)";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInBTreeIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE AGE IN (50, 51, 52) AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInHashIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueClusteredIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInPrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInPrimaryKeyAndNonIndexed");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInPrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInPrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInBTreeIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE AGE IN (50, 51, 52) OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInHashIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInUniqueClusteredIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithWhereInPrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithWhereInPrimaryKeyOrNonIndexed");
            String query = "DELETE FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithWhereInPrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithWhereInPrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        InTest test = new InTest();
        test.runTests();
    }
}