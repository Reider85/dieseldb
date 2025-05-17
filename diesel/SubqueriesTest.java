package diesel;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

public class SubqueriesTest {
    private static final Logger LOGGER = Logger.getLogger(SubqueriesTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;

    public SubqueriesTest() {
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

            // Step 2: Run SELECT queries with subqueries
            selectWithSubqueryInColumn();
            selectWithSubqueryInWhere();
            selectWithSubqueryInInClause();
            selectWithSubqueryInColumnWhereOrderBy();
            selectWithSubqueryInColumnWhereGroupBy();
            selectWithSubqueryInColumnWhereHaving();
            selectWithSubqueryInColumnInnerJoinSubqueryInWhere();
            selectWithSubqueryInColumnInnerJoinSubqueryInOn();
            selectWithSubqueryInColumnWithAlias();
            selectWithSubqueryInInClauseWithAlias();
            selectWithSubqueryInColumnWhereOrderByWithAliases();
            selectWithSubqueryInColumnWhereGroupByWithAliases();
            selectWithSubqueryInColumnWhereHavingWithAliases();
            selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases();
            selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases();
            selectWithSubqueryInColumnWhereOrderByWithAggregates();

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
        try {
            LOGGER.log(Level.INFO, "Executing SELECT: {0}", query);
            long startTime = System.nanoTime();
            Object result = database.executeQuery(query, null);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Result: {0} rows, Time: {1} ms", new Object[]{((List<?>) result).size(), String.format("%.3f", durationMs)});
            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "executeSelectQuery: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumn() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumn");
            String query = "SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u WHERE AGE > 50 LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumn: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumn: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInWhere() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInWhere");
            String query = "SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInWhere: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInWhere: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInInClause() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInInClause");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInInClause: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInInClause: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereOrderBy() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereOrderBy");
            String query = "SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) ORDER BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereOrderBy: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereOrderBy: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereGroupBy() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereGroupBy");
            String query = "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereGroupBy: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereGroupBy: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereHaving");
            String query = "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnInnerJoinSubqueryInWhere() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnInnerJoinSubqueryInWhere");
            String query = "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID WHERE u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnInnerJoinSubqueryInWhere: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnInnerJoinSubqueryInWhere: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnInnerJoinSubqueryInOn() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnInnerJoinSubqueryInOn");
            String query = "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnInnerJoinSubqueryInOn: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnInnerJoinSubqueryInOn: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWithAlias() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWithAlias");
            String query = "SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u WHERE AGE > 50 LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWithAlias: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWithAlias: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInInClauseWithAlias() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInInClauseWithAlias");
            String query = "SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) AS id_subquery LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInInClauseWithAlias: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInInClauseWithAlias: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereOrderByWithAliases() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereOrderByWithAliases");
            String query = "SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery ORDER BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereOrderByWithAliases: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereOrderByWithAliases: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereGroupByWithAliases() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereGroupByWithAliases");
            String query = "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereGroupByWithAliases: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereGroupByWithAliases: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereHavingWithAliases() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereHavingWithAliases");
            String query = "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) AS count_subquery LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereHavingWithAliases: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereHavingWithAliases: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases");
            String query = "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID WHERE u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases");
            String query = "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectWithSubqueryInColumnWhereOrderByWithAggregates() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectWithSubqueryInColumnWhereOrderByWithAggregates");
            String query = "SELECT ID, (SELECT MAX(NAME) FROM USERS WHERE ID = u.ID LIMIT 1) AS max_name FROM USERS u WHERE AGE > (SELECT AVG(AGE) FROM USERS LIMIT 1) ORDER BY (SELECT MAX(NAME) FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10";
            executeSelectQuery(query);
            LOGGER.log(Level.INFO, "Test selectWithSubqueryInColumnWhereOrderByWithAggregates: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectWithSubqueryInColumnWhereOrderByWithAggregates: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        SubqueriesTest test = new SubqueriesTest();
        test.runTests();
    }
}