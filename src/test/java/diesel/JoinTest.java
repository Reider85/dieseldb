package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class JoinTest {

    private static final int RECORD_COUNT = 10;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTables();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        insertRecords();
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
            database.dropTable("USER_DETAILS");
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
        Table usersTable = database.getTable("USERS");
        Table detailsTable = database.getTable("USER_DETAILS");
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
        usersTable.saveToFile("USERS");
        detailsTable.saveToFile("USER_DETAILS");
    }

    @Test
    void selectWithInnerJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithInnerJoinOnPrimaryKey");
    }

    @Test
    void selectWithInnerJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithInnerJoinOnBTreeIndex");
    }

    @Test
    void selectWithInnerJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndex");
    }

    @Test
    void selectWithInnerJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithInnerJoinOnUniqueIndex");
    }

    @Test
    void selectWithInnerJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithInnerJoinOnNonIndexedField");
    }

    @Test
    void selectWithLeftInnerJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithLeftInnerJoinOnPrimaryKey");
    }

    @Test
    void selectWithLeftInnerJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithLeftInnerJoinOnBTreeIndex");
    }

    @Test
    void selectWithLeftInnerJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithLeftInnerJoinOnHashIndex");
    }

    @Test
    void selectWithLeftInnerJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithLeftInnerJoinOnUniqueIndex");
    }

    @Test
    void selectWithLeftInnerJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithLeftInnerJoinOnNonIndexedField");
    }

    @Test
    void selectWithRightInnerJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithRightInnerJoinOnPrimaryKey");
    }

    @Test
    void selectWithRightInnerJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithRightInnerJoinOnBTreeIndex");
    }

    @Test
    void selectWithRightInnerJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithRightInnerJoinOnHashIndex");
    }

    @Test
    void selectWithRightInnerJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithRightInnerJoinOnUniqueIndex");
    }

    @Test
    void selectWithRightInnerJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithRightInnerJoinOnNonIndexedField");
    }

    @Test
    void selectWithLeftOuterJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithLeftOuterJoinOnPrimaryKey");
    }

    @Test
    void selectWithLeftOuterJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithLeftOuterJoinOnBTreeIndex");
    }

    @Test
    void selectWithLeftOuterJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithLeftOuterJoinOnHashIndex");
    }

    @Test
    void selectWithLeftOuterJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithLeftOuterJoinOnUniqueIndex");
    }

    @Test
    void selectWithLeftOuterJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithLeftOuterJoinOnNonIndexedField");
    }

    @Test
    void selectWithRightOuterJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithRightOuterJoinOnPrimaryKey");
    }

    @Test
    void selectWithRightOuterJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithRightOuterJoinOnBTreeIndex");
    }

    @Test
    void selectWithRightOuterJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithRightOuterJoinOnHashIndex");
    }

    @Test
    void selectWithRightOuterJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithRightOuterJoinOnUniqueIndex");
    }

    @Test
    void selectWithRightOuterJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithRightOuterJoinOnNonIndexedField");
    }

    @Test
    void selectWithLeftJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithLeftJoinOnPrimaryKey");
    }

    @Test
    void selectWithLeftJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithLeftJoinOnBTreeIndex");
    }

    @Test
    void selectWithLeftJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithLeftJoinOnHashIndex");
    }

    @Test
    void selectWithLeftJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithLeftJoinOnUniqueIndex");
    }

    @Test
    void selectWithLeftJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithLeftJoinOnNonIndexedField");
    }

    @Test
    void selectWithRightJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithRightJoinOnPrimaryKey");
    }

    @Test
    void selectWithRightJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithRightJoinOnBTreeIndex");
    }

    @Test
    void selectWithRightJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithRightJoinOnHashIndex");
    }

    @Test
    void selectWithRightJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithRightJoinOnUniqueIndex");
    }

    @Test
    void selectWithRightJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS RIGHT JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithRightJoinOnNonIndexedField");
    }

    @Test
    void selectWithFullJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithFullJoinOnPrimaryKey");
    }

    @Test
    void selectWithFullJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithFullJoinOnBTreeIndex");
    }

    @Test
    void selectWithFullJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithFullJoinOnHashIndex");
    }

    @Test
    void selectWithFullJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithFullJoinOnUniqueIndex");
    }

    @Test
    void selectWithFullJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithFullJoinOnNonIndexedField");
    }

    @Test
    void selectWithFullOuterJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502)", null), "selectWithFullOuterJoinOnPrimaryKey");
    }

    @Test
    void selectWithFullOuterJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithFullOuterJoinOnBTreeIndex");
    }

    @Test
    void selectWithFullOuterJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithFullOuterJoinOnHashIndex");
    }

    @Test
    void selectWithFullOuterJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithFullOuterJoinOnUniqueIndex");
    }

    @Test
    void selectWithFullOuterJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00", null), "selectWithFullOuterJoinOnNonIndexedField");
    }

    @Test
    void selectWithCrossJoinOnPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS CROSS JOIN USER_DETAILS WHERE USERS.ID IN (500, 501, 502)", null), "selectWithCrossJoinOnPrimaryKey");
    }

    @Test
    void selectWithCrossJoinOnBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS CROSS JOIN USER_DETAILS WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithCrossJoinOnBTreeIndex");
    }

    @Test
    void selectWithCrossJoinOnHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS CROSS JOIN USER_DETAILS WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithCrossJoinOnHashIndex");
    }

    @Test
    void selectWithCrossJoinOnUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS CROSS JOIN USER_DETAILS WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithCrossJoinOnUniqueIndex");
    }

    @Test
    void selectWithCrossJoinOnNonIndexedField() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS CROSS JOIN USER_DETAILS WHERE USERS.BALANCE = 5100.00", null), "selectWithCrossJoinOnNonIndexedField");
    }

    @Test
    void selectWithInnerJoinOnPrimaryKeyWithAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.ID IN (500, 501, 502)", null), "selectWithInnerJoinOnPrimaryKeyWithAndOr");
    }

    @Test
    void selectWithInnerJoinOnBTreeIndexWithAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE AND USERS.NAME = USER_DETAILS.NAME OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithInnerJoinOnBTreeIndexWithAndOr");
    }

    @Test
    void selectWithInnerJoinOnHashIndexWithAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME AND USERS.AGE = USER_DETAILS.AGE OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndexWithAndOr");
    }

    @Test
    void selectWithInnerJoinOnUniqueIndexWithAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.AGE = USER_DETAILS.AGE OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithInnerJoinOnUniqueIndexWithAndOr");
    }

    @Test
    void selectWithInnerJoinOnNonIndexedFieldWithAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.AGE = USER_DETAILS.AGE OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.BALANCE = 5100.00", null), "selectWithInnerJoinOnNonIndexedFieldWithAndOr");
    }

    @Test
    void selectWithInnerJoinOnPrimaryKeyWithAndOrInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502)", null), "selectWithInnerJoinOnPrimaryKeyWithAndOrInParentheses");
    }

    @Test
    void selectWithInnerJoinOnBTreeIndexWithAndOrInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.AGE = USER_DETAILS.AGE AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithInnerJoinOnBTreeIndexWithAndOrInParentheses");
    }

    @Test
    void selectWithInnerJoinOnHashIndexWithAndOrInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.NAME = USER_DETAILS.NAME AND USERS.AGE = USER_DETAILS.AGE) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndexWithAndOrInParentheses");
    }

    @Test
    void selectWithInnerJoinOnUniqueIndexWithAndOrInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.AGE = USER_DETAILS.AGE) OR (USERS.NAME = USER_DETAILS.NAME) WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithInnerJoinOnUniqueIndexWithAndOrInParentheses");
    }

    @Test
    void selectWithInnerJoinOnNonIndexedFieldWithAndOrInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.AGE = USER_DETAILS.AGE) OR (USERS.NAME = USER_DETAILS.NAME) WHERE USERS.BALANCE = 5100.00", null), "selectWithInnerJoinOnNonIndexedFieldWithAndOrInParentheses");
    }

    @Test
    void selectWithInnerJoinOnPrimaryKeyWithNestedAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON ((USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME)) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502)", null), "selectWithInnerJoinOnPrimaryKeyWithNestedAndOr");
    }

    @Test
    void selectWithInnerJoinOnBTreeIndexWithNestedAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON ((USERS.AGE = USER_DETAILS.AGE AND USERS.NAME = USER_DETAILS.NAME)) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithInnerJoinOnBTreeIndexWithNestedAndOr");
    }

    @Test
    void selectWithInnerJoinOnHashIndexWithNestedAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON ((USERS.NAME = USER_DETAILS.NAME AND USERS.AGE = USER_DETAILS.AGE)) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndexWithNestedAndOr");
    }

    @Test
    void selectWithInnerJoinOnUniqueIndexWithNestedAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON ((USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.AGE = USER_DETAILS.AGE)) OR (USERS.NAME = USER_DETAILS.NAME) WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithInnerJoinOnUniqueIndexWithNestedAndOr");
    }

    @Test
    void selectWithInnerJoinOnNonIndexedFieldWithNestedAndOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON ((USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.AGE = USER_DETAILS.AGE)) OR (USERS.NAME = USER_DETAILS.NAME) WHERE USERS.BALANCE = 5100.00", null), "selectWithInnerJoinOnNonIndexedFieldWithNestedAndOr");
    }

    @Test
    void selectWithInnerJoinOnPrimaryKeyWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.ID IN (500, 501, 502)", null), "selectWithInnerJoinOnPrimaryKeyWithAndLikeInOr");
    }

    @Test
    void selectWithInnerJoinOnBTreeIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithInnerJoinOnBTreeIndexWithAndLikeInOr");
    }

    @Test
    void selectWithInnerJoinOnHashIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndexWithAndLikeInOr");
    }

    @Test
    void selectWithInnerJoinOnUniqueIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.NAME LIKE 'User%' OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithInnerJoinOnUniqueIndexWithAndLikeInOr");
    }

    @Test
    void selectWithInnerJoinOnNonIndexedFieldWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.NAME LIKE 'User%' OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.BALANCE = 5100.00", null), "selectWithInnerJoinOnNonIndexedFieldWithAndLikeInOr");
    }

    @Test
    void selectWithLeftOuterJoinOnPrimaryKeyWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.ID IN (500, 501, 502)", null), "selectWithLeftOuterJoinOnPrimaryKeyWithAndLikeInOr");
    }

    @Test
    void selectWithLeftOuterJoinOnBTreeIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.AGE = USER_DETAILS.AGE AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.AGE IN (50, 51, 52)", null), "selectWithLeftOuterJoinOnBTreeIndexWithAndLikeInOr");
    }

    @Test
    void selectWithLeftOuterJoinOnHashIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME AND USERS.NAME LIKE 'User%' OR USERS.USER_CODE = USER_DETAILS.USER_CODE WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithLeftOuterJoinOnHashIndexWithAndLikeInOr");
    }

    @Test
    void selectWithLeftOuterJoinOnUniqueIndexWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.USER_CODE = USER_DETAILS.USER_CODE AND USERS.NAME LIKE 'User%' OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithLeftOuterJoinOnUniqueIndexWithAndLikeInOr");
    }

    @Test
    void selectWithLeftOuterJoinOnNonIndexedFieldWithAndLikeInOr() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE AND USERS.NAME LIKE 'User%' OR USERS.NAME = USER_DETAILS.NAME WHERE USERS.BALANCE = 5100.00", null), "selectWithLeftOuterJoinOnNonIndexedFieldWithAndLikeInOr");
    }

    @Test
    void selectWithInnerJoinOnHashIndexWithIsNullAndIsNotNull() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.NAME = USER_DETAILS.NAME AND USERS.NAME IS NOT NULL AND USER_DETAILS.INFO IS NULL WHERE USERS.NAME IN ('User500', 'User501', 'User502')", null), "selectWithInnerJoinOnHashIndexWithIsNullAndIsNotNull");
    }

    @Test
    void selectWithLeftOuterJoinOnPrimaryKeyWithIsNullAndIsNotNull() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS LEFT OUTER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID AND USERS.ID IS NOT NULL AND USER_DETAILS.INFO IS NULL WHERE USERS.ID IN (500, 501, 502)", null), "selectWithLeftOuterJoinOnPrimaryKeyWithIsNullAndIsNotNull");
    }
}
