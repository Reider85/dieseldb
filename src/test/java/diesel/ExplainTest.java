package diesel;

import diesel.Database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExplainTest {

    private static final int RECORD_COUNT = 200;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTables();
        insertRecords();
    }

    @AfterEach
    void tearDown() {
        SelectQuery.loadHashJoinConfig();
    }

    private void createTables() {
        dropTables();
        String createUsersTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)";
        String createDetailsTableQuery = "CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)";
        database.executeQuery(createUsersTableQuery, null);
        database.executeQuery(createDetailsTableQuery, null);
    }

    private void dropTables() {
        try {
            database.dropTable("USERS");
        } catch (TableNotFoundException ignored) {
        }
        try {
            database.dropTable("USER_DETAILS");
        } catch (TableNotFoundException ignored) {
        }
    }

    private void insertRecords() {
        for (int i = 1; i <= RECORD_COUNT; i++) {
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);
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
    }

    private String explain(String sql) {
        return (String) database.executeQuery(sql, null);
    }

    @Test
    void explainSelectShowsOperationScanTableAndEstimatedRows() {
        String plan = explain("EXPLAIN SELECT * FROM USERS");
        assertTrue(plan.contains("Execution Plan"), "plan must start with the Execution Plan header");
        assertTrue(plan.contains("Operation: SELECT"), "plan must name the SELECT operation");
        assertTrue(plan.contains("Scan USERS (estimated rows: 200)"), "plan must show the scan and estimated rows");
        assertTrue(plan.contains("Index: none (full scan)"), "plan must show the full-scan index line");
    }

    @Test
    void explainSelectWithEqualityJoinShowsHashJoinAlgorithm() {
        String plan = explain("EXPLAIN SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID");
        assertTrue(plan.contains("Join INNER"), "plan must list the INNER join");
        assertTrue(plan.contains("In-Memory Hash Join"), "equality join on 200 rows must be planned as an in-memory hash join");
        assertTrue(plan.contains("USERS.ID = USER_DETAILS.USER_ID"), "plan must show the hash join keys");
        assertTrue(plan.contains("estimated rows: 200"), "plan must estimate the joined table size");
    }

    @Test
    void explainSelectWithOrOnConditionShowsNestedLoop() {
        String plan = explain("EXPLAIN SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID OR USERS.AGE = 25");
        assertTrue(plan.contains("Nested Loop (OR condition may produce a large result set)"),
                "an OR in the ON clause must be planned as a nested loop");
    }

    @Test
    void explainSelectWithWhereShowsFilter() {
        String plan = explain("EXPLAIN SELECT * FROM USERS WHERE AGE > 25");
        assertTrue(plan.contains("Filter (WHERE): AGE GREATER_THAN 25"), "plan must show the WHERE filter");
    }

    @Test
    void explainSelectWithGroupByOrderByLimitShowsClauses() {
        String plan = explain("EXPLAIN SELECT AGE, COUNT(*) FROM USERS GROUP BY AGE ORDER BY AGE LIMIT 5");
        assertTrue(plan.contains("Group By: USERS.AGE"), "plan must show the GROUP BY clause");
        assertTrue(plan.contains("Order By: AGE ASC"), "plan must show the ORDER BY clause");
        assertTrue(plan.contains("Limit: 5, Offset: none"), "plan must show the LIMIT clause");
    }

    @Test
    void explainSelectWithIndexShowsUsedIndex() {
        database.executeQuery("CREATE HASH INDEX ON USERS (AGE)", null);
        String plan = explain("EXPLAIN SELECT * FROM USERS WHERE AGE = 25");
        assertTrue(plan.contains("Hash index on USERS.AGE"), "plan must name the hash index used for the equality condition");
    }

    @Test
    void explainSelectUsesClusteredIndexOnPrimaryKey() {
        String plan = explain("EXPLAIN SELECT * FROM USERS WHERE ID = 5");
        assertTrue(plan.contains("Clustered index on USERS.ID"), "primary-key equality must use the clustered index");
    }

    @Test
    void explainInsertShowsTableAndColumns() {
        String plan = explain("EXPLAIN INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE201', 'User201', 19, 100.00)");
        assertTrue(plan.contains("Operation: INSERT"), "plan must name the INSERT operation");
        assertTrue(plan.contains("Table: USERS (estimated rows: 200)"), "plan must show the target table");
        assertTrue(plan.contains("Columns: [USER_CODE, NAME, AGE, BALANCE]"), "plan must show the inserted columns");
    }

    @Test
    void explainUpdateShowsTableConditionsAndFullScan() {
        String plan = explain("EXPLAIN UPDATE USERS SET AGE = 30 WHERE USER_CODE = 'CODE1'");
        assertTrue(plan.contains("Operation: UPDATE"), "plan must name the UPDATE operation");
        assertTrue(plan.contains("Table: USERS (estimated rows: 200)"), "plan must show the target table");
        assertTrue(plan.contains("Columns: [AGE]"), "plan must show the updated columns");
        assertTrue(plan.contains("Conditions: USER_CODE EQUALS 'CODE1'"), "plan must show the WHERE conditions");
        assertTrue(plan.contains("Index: none (full scan)"), "UPDATE must be planned as a full scan");
    }

    @Test
    void explainDeleteShowsTableConditionsAndUsedIndex() {
        database.executeQuery("CREATE HASH INDEX ON USERS (AGE)", null);
        String plan = explain("EXPLAIN DELETE FROM USERS WHERE AGE = 25");
        assertTrue(plan.contains("Operation: DELETE"), "plan must name the DELETE operation");
        assertTrue(plan.contains("Conditions: AGE EQUALS 25"), "plan must show the WHERE conditions");
        assertTrue(plan.contains("Index: Hash index on USERS.AGE"), "DELETE must reuse the secondary hash index");
    }

    @Test
    void explainDeleteUsesFullScanWhenOnlyClusteredIndexMatches() {
        String plan = explain("EXPLAIN DELETE FROM USERS WHERE ID = 5");
        assertTrue(plan.contains("Index: none (full scan)"),
                "DELETE must not use the clustered index, mirroring the runtime");
    }

    @Test
    void explainAnalyzeSelectShowsActualMetrics() {
        String plan = explain("EXPLAIN ANALYZE SELECT USERS.ID, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID");
        assertTrue(plan.contains("Actual metrics (ANALYZE):"), "ANALYZE must append the actual metrics block");
        assertTrue(plan.contains("rows: 200"), "ANALYZE must report the actual row count");
        assertTrue(plan.contains("elapsed:"), "ANALYZE must report the elapsed time");
        assertTrue(plan.contains("hash join table size: 200"), "ANALYZE must report the hash table size");
        assertTrue(plan.contains("hash join partitioned: false"), "default config must run the in-memory hash join");
    }

    @Test
    void explainAnalyzeSelectWithoutJoinOmitsJoinMetrics() {
        String plan = explain("EXPLAIN ANALYZE SELECT * FROM USERS");
        assertTrue(plan.contains("rows: 200"), "ANALYZE must report the actual row count");
        assertFalse(plan.contains("hash join"), "a scan-only plan must not show hash-join metrics");
    }

    @Test
    void explainAnalyzeInsertShowsAffectedRows() {
        String plan = explain("EXPLAIN ANALYZE INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE201', 'User201', 19, 100.00)");
        assertTrue(plan.contains("affected rows: 1"), "ANALYZE must report the inserted row");
        assertTrue(plan.contains("elapsed:"), "ANALYZE must report the elapsed time");
    }

    @Test
    void explainAnalyzeUpdateShowsAffectedRows() {
        String plan = explain("EXPLAIN ANALYZE UPDATE USERS SET AGE = 99 WHERE USER_CODE = 'CODE1'");
        assertTrue(plan.contains("affected rows: 1"), "ANALYZE must report the updated rows");
    }

    @Test
    void explainAnalyzeDeleteShowsAffectedRows() {
        String plan = explain("EXPLAIN ANALYZE DELETE FROM USERS WHERE AGE = 25");
        assertTrue(plan.matches("(?s).*affected rows: [1-9][0-9]*.*"), "ANALYZE must report a positive affected-row count");
    }

    @Test
    void explainAnalyzeDeleteAllShowsFullCount() {
        String plan = explain("EXPLAIN ANALYZE DELETE FROM USERS");
        assertTrue(plan.contains("affected rows: 200"), "ANALYZE DELETE without conditions must affect every row");
    }

    @Test
    void explainUnsupportedStatementThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> database.executeQuery("EXPLAIN CREATE TABLE OTHER (A INTEGER)", null),
                "EXPLAIN must reject DDL statements");
    }

    @Test
    void explainMissingTableThrows() {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> database.executeQuery("EXPLAIN SELECT * FROM NO_SUCH_TABLE", null),
                "EXPLAIN on a missing table must fail at execution time");
        assertTrue(exception.getMessage().contains("does not exist"), "error must name the missing table");
    }

    @Test
    void explainSelectWithDerivedTableShowsPlan() {
        String plan = explain("EXPLAIN SELECT t.ID FROM (SELECT * FROM USERS) t");
        assertTrue(plan.contains("Operation: SELECT"), "derived-table SELECT must be planned");
        assertTrue(plan.contains("estimated rows: 200"), "plan must estimate the derived table size");
    }

    @Test
    void explainWithSubqueryInWhereShowsFilter() {
        String plan = explain("EXPLAIN SELECT * FROM USERS WHERE AGE IN (SELECT AGE FROM USER_DETAILS WHERE USER_ID = 1)");
        assertTrue(plan.contains("Filter (WHERE):"), "plan must show the subquery-based WHERE filter");
        assertTrue(plan.contains("estimated rows: 200"), "plan must estimate the main table size");
    }

    @Test
    void explainLowerCaseKeywordWorks() {
        String plan = explain("explain select * from users");
        assertTrue(plan.contains("Operation: SELECT"), "lower-case EXPLAIN must be recognized");
    }
}
