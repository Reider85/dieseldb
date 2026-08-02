package diesel;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

public class LikeTest {

    private static final int RECORD_COUNT = 1000;
    private static final Logger LOGGER = Logger.getLogger(LikeTest.class.getName());
    private final Database database;

    public LikeTest() {
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

            // Step 2: Run SELECT queries with LIKE
            selectWithLikeBTreeIndex();
            selectWithLikeHashIndex();
            selectWithLikeUniqueIndex();
            selectWithLikeUniqueClusteredIndex();
            selectWithLikePrimaryKey();
            selectWithLikeBTreeIndexAndNonIndexed();
            selectWithLikeHashIndexAndNonIndexed();
            selectWithLikeUniqueIndexAndNonIndexed();
            selectWithLikeUniqueClusteredIndexAndNonIndexed();
            selectWithLikePrimaryKeyAndNonIndexed();
            selectWithLikeBTreeIndexOrNonIndexed();
            selectWithLikeHashIndexOrNonIndexed();
            selectWithLikeUniqueIndexOrNonIndexed();
            selectWithLikeUniqueClusteredIndexOrNonIndexed();
            selectWithLikePrimaryKeyOrNonIndexed();

            // Step 3: Run UPDATE queries with LIKE
            updateWithLikeBTreeIndex();
            updateWithLikeHashIndex();
            updateWithLikeUniqueIndex();
            updateWithLikeUniqueClusteredIndex();
            updateWithLikePrimaryKey();
            updateWithLikeBTreeIndexAndNonIndexed();
            updateWithLikeHashIndexAndNonIndexed();
            updateWithLikeUniqueIndexAndNonIndexed();
            updateWithLikeUniqueClusteredIndexAndNonIndexed();
            updateWithLikePrimaryKeyAndNonIndexed();
            updateWithLikeBTreeIndexOrNonIndexed();
            updateWithLikeHashIndexOrNonIndexed();
            updateWithLikeUniqueIndexOrNonIndexed();
            updateWithLikeUniqueClusteredIndexOrNonIndexed();
            updateWithLikePrimaryKeyOrNonIndexed();

            // Step 4: Run DELETE queries with LIKE
            deleteWithLikeBTreeIndex();
            deleteWithLikeHashIndex();
            deleteWithLikeUniqueIndex();
            deleteWithLikeUniqueClusteredIndex();
            deleteWithLikePrimaryKey();
            deleteWithLikeBTreeIndexAndNonIndexed();
            deleteWithLikeHashIndexAndNonIndexed();
            deleteWithLikeUniqueIndexAndNonIndexed();
            deleteWithLikeUniqueClusteredIndexAndNonIndexed();
            deleteWithLikePrimaryKeyAndNonIndexed();
            deleteWithLikeBTreeIndexOrNonIndexed();
            deleteWithLikeHashIndexOrNonIndexed();
            deleteWithLikeUniqueIndexOrNonIndexed();
            deleteWithLikeUniqueClusteredIndexOrNonIndexed();
            deleteWithLikePrimaryKeyOrNonIndexed();

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
            LOGGER.log(Level.INFO, "Inserting {0} records", LikeTest.RECORD_COUNT);
            Table table = database.getTable("USERS");

            long startTime = System.nanoTime();
            for (int i = 1; i <= LikeTest.RECORD_COUNT; i++) {
                String query = String.format(
                        "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                        i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP)
                );
                database.executeQuery(query, null);
            }
            table.saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{LikeTest.RECORD_COUNT, String.format("%.3f", durationMs)});
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

    private void selectWithLikeBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeBTreeIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeHashIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueClusteredIndex");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikePrimaryKey");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeBTreeIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeHashIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueClusteredIndexAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikePrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikePrimaryKeyAndNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikePrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikePrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeBTreeIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeHashIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikeUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikeUniqueClusteredIndexOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikeUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikeUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithLikePrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithLikePrimaryKeyOrNonIndexed");
            String query = "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithLikePrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithLikePrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeBTreeIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeHashIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueClusteredIndex");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikePrimaryKey");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeBTreeIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeHashIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueClusteredIndexAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikePrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikePrimaryKeyAndNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikePrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikePrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeBTreeIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeHashIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikeUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikeUniqueClusteredIndexOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikeUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikeUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void updateWithLikePrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: updateWithLikePrimaryKeyOrNonIndexed");
            String query = "UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeUpdateQuery(query);
            LOGGER.log(Level.INFO, "Test updateWithLikePrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test updateWithLikePrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeBTreeIndex");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeHashIndex");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueIndex");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueClusteredIndex");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikePrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikePrimaryKey");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikePrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikePrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeBTreeIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeBTreeIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeBTreeIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeBTreeIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeHashIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeHashIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeHashIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeHashIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueClusteredIndexAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueClusteredIndexAndNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueClusteredIndexAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueClusteredIndexAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikePrimaryKeyAndNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikePrimaryKeyAndNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikePrimaryKeyAndNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikePrimaryKeyAndNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeBTreeIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeBTreeIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeBTreeIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeBTreeIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeHashIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeHashIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeHashIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeHashIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikeUniqueClusteredIndexOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikeUniqueClusteredIndexOrNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikeUniqueClusteredIndexOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikeUniqueClusteredIndexOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void deleteWithLikePrimaryKeyOrNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: deleteWithLikePrimaryKeyOrNonIndexed");
            String query = "DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000";
            executeDeleteQuery(query);
            restoreTableState();
            LOGGER.log(Level.INFO, "Test deleteWithLikePrimaryKeyOrNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test deleteWithLikePrimaryKeyOrNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        LikeTest test = new LikeTest();
        test.runTests();
    }
}