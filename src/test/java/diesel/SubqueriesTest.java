package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class SubqueriesTest {

    private static final int RECORD_COUNT = 10;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTable();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertRecords();
    }

    private void createTable() {
        dropTable();
        String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        database.executeQuery(createTableQuery, null);
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
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

    private void insertRecords() {
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

    @Test
    void selectWithSubqueryInColumn() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u WHERE AGE > 50 LIMIT 10", null), "selectWithSubqueryInColumn");
    }

    @Test
    void selectWithSubqueryInWhere() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10", null), "selectWithSubqueryInWhere");
    }

    @Test
    void selectWithSubqueryInInClause() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10", null), "selectWithSubqueryInInClause");
    }

    @Test
    void selectWithSubqueryInColumnWhereOrderBy() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) ORDER BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereOrderBy");
    }

    @Test
    void selectWithSubqueryInColumnWhereGroupBy() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereGroupBy");
    }

    @Test
    void selectWithSubqueryInColumnWhereHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereHaving");
    }

    @Test
    void selectWithSubqueryInColumnInnerJoinSubqueryInWhere() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID WHERE u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnInnerJoinSubqueryInWhere");
    }

    @Test
    void selectWithSubqueryInColumnInnerJoinSubqueryInOn() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnInnerJoinSubqueryInOn");
    }

    @Test
    void selectWithSubqueryInColumnWithAlias() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u WHERE AGE > 50 LIMIT 10", null), "selectWithSubqueryInColumnWithAlias");
    }

    @Test
    void selectWithSubqueryInInClauseWithAlias() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) AS id_subquery LIMIT 10", null), "selectWithSubqueryInInClauseWithAlias");
    }

    @Test
    void selectWithSubqueryInColumnWhereOrderByWithAliases() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery ORDER BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereOrderByWithAliases");
    }

    @Test
    void selectWithSubqueryInColumnWhereGroupByWithAliases() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereGroupByWithAliases");
    }

    @Test
    void selectWithSubqueryInColumnWhereHavingWithAliases() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) AS count_subquery LIMIT 10", null), "selectWithSubqueryInColumnWhereHavingWithAliases");
    }

    @Test
    void selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID WHERE u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery LIMIT 10", null), "selectWithSubqueryInColumnInnerJoinSubqueryInWhereWithAliases");
    }

    @Test
    void selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name_alias FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) AS age_subquery LIMIT 10", null), "selectWithSubqueryInColumnInnerJoinSubqueryInOnWithAliases");
    }

    @Test
    void selectWithSubqueryInColumnWhereOrderByWithAggregates() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, (SELECT MAX(NAME) FROM USERS WHERE ID = u.ID LIMIT 1) AS max_name FROM USERS u WHERE AGE > (SELECT AVG(AGE) FROM USERS LIMIT 1) ORDER BY (SELECT MAX(NAME) FROM USERS WHERE ID = u.ID LIMIT 1) LIMIT 10", null), "selectWithSubqueryInColumnWhereOrderByWithAggregates");
    }
}
