package diesel;

import diesel.Database;

import java.math.BigDecimal;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdvancedTest {

    private static final int RECORD_COUNT = 100;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);
        insertRecords();
    }

    private void insertRecords() {
        Table table = database.getTable("USERS");
        Random random = new Random();
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP)
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("USERS");
    }

    @Test
    void insertWithSequencePrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1001', 'User1001', 25, 1500.00)", null), "insertWithSequencePrimaryKey");
    }

    @Test
    void insertWithDuplicateSequencePrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1001DUP', 'User1001Duplicate', 25, 1500.00)", null), "insertWithDuplicateSequencePrimaryKey");
    }

    @Test
    void insertWithUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1002', 'User1002', 25, 1500.00)", null), "insertWithUniqueIndex");
    }

    @Test
    void insertWithDuplicateUniqueIndex() {
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1002', 'User1002', 25, 1500.00)", null);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1002', 'User1002Duplicate', 25, 1500.00)", null), "insertWithDuplicateUniqueIndex");
        assertTrue(exception.getCause() instanceof IllegalStateException, "insertWithDuplicateUniqueIndex cause");
    }

    @Test
    void insertWithUniqueClusteredIndex() {
        assertDoesNotThrow(() -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1003', 'User1003', 26, 1600.00)", null), "insertWithUniqueClusteredIndex");
    }

    @Test
    void insertWithDuplicateUniqueClusteredIndex() {
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1003', 'User1003', 26, 1600.00)", null);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1003', 'User1003Duplicate', 26, 1600.00)", null), "insertWithDuplicateUniqueClusteredIndex");
        assertTrue(exception.getCause() instanceof IllegalStateException, "insertWithDuplicateUniqueClusteredIndex cause");
    }

    @Test
    void insertWithPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1004', 'User1004', 27, 1700.00)", null), "insertWithPrimaryKey");
    }

    @Test
    void insertWithDuplicatePrimaryKey() {
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1004', 'User1004', 27, 1700.00)", null);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE1004', 'User1004Duplicate', 27, 1700.00)", null), "insertWithDuplicatePrimaryKey");
        assertTrue(exception.getCause() instanceof IllegalStateException, "insertWithDuplicatePrimaryKey cause");
    }

    @Test
    void selectWithWhereSequencePrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500", null), "selectWithWhereSequencePrimaryKey");
    }

    @Test
    void selectWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null), "selectWithWhereSequencePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null), "selectWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void selectWithoutWhere() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, USER_CODE, NAME, AGE, BALANCE FROM USERS", null), "selectWithoutWhere");
    }

    @Test
    void selectWithWhereNoIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE BALANCE > 5000", null), "selectWithWhereNoIndex");
    }

    @Test
    void selectWithWhereHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME = 'User500'", null), "selectWithWhereHashIndex");
    }

    @Test
    void selectWithWhereBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50", null), "selectWithWhereBTreeIndex");
    }

    @Test
    void selectWithWhereUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500", null), "selectWithWhereUniqueIndex");
    }

    @Test
    void selectWithWhereUniqueClusteredIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500'", null), "selectWithWhereUniqueClusteredIndex");
    }

    @Test
    void selectWithWherePrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500", null), "selectWithWherePrimaryKey");
    }

    @Test
    void selectWithWhereIndexedAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 AND BALANCE > 5000", null), "selectWithWhereIndexedAndNonIndexed");
    }

    @Test
    void selectWithWhereIndexedAndNonIndexedInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)", null), "selectWithWhereIndexedAndNonIndexedInParentheses");
    }

    @Test
    void selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )", null), "selectWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
    }

    @Test
    void selectWithWhereTwoIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 AND NAME = 'User500'", null), "selectWithWhereTwoIndexed");
    }

    @Test
    void selectWithWhereUniqueBTreeHashIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null), "selectWithWhereUniqueBTreeHashIndexed");
    }

    @Test
    void selectWithWhereUniqueClusteredBTreeHashIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'", null), "selectWithWhereUniqueClusteredBTreeHashIndexed");
    }

    @Test
    void selectWithWherePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null), "selectWithWherePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void selectWithWhereUniqueBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null), "selectWithWhereUniqueBTreeHashIndexedInParentheses");
    }

    @Test
    void selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')", null), "selectWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
    }

    @Test
    void selectWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null), "selectWithWherePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void selectWithWhereIndexedOrIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 OR NAME = 'User500'", null), "selectWithWhereIndexedOrIndexed");
    }

    @Test
    void selectWithWhereIndexedOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000", null), "selectWithWhereIndexedOrNonIndexed");
    }

    @Test
    void selectWithWhereIndexedOrNonIndexedWithLimit() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10", null), "selectWithWhereIndexedOrNonIndexedWithLimit");
    }

    @Test
    void selectWithWhereIndexedOrNonIndexedWithLimitAndOffset() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5", null), "selectWithWhereIndexedOrNonIndexedWithLimitAndOffset");
    }

    @Test
    void updateWithWhereSequencePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereSequencePrimaryKey");
    }

    @Test
    void updateWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereSequencePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void updateWithWhereNoIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereNoIndex");
    }

    @Test
    void updateWithWhereHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereHashIndex");
    }

    @Test
    void updateWithWhereBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereBTreeIndex");
    }

    @Test
    void updateWithWhereUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueIndex");
    }

    @Test
    void updateWithWhereUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueClusteredIndex");
    }

    @Test
    void updateWithWherePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWherePrimaryKey");
    }

    @Test
    void updateWithWhereIndexedAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereIndexedAndNonIndexed");
    }

    @Test
    void updateWithWhereTwoIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereTwoIndexed");
    }

    @Test
    void updateWithWhereUniqueBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueBTreeHashIndexed");
    }

    @Test
    void updateWithWhereUniqueClusteredBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueClusteredBTreeHashIndexed");
    }

    @Test
    void updateWithWherePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWherePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void updateWithWhereUniqueBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueBTreeHashIndexedInParentheses");
    }

    @Test
    void updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
    }

    @Test
    void updateWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWherePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void updateWithWhereIndexedOrIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereIndexedOrIndexed");
    }

    @Test
    void updateWithWhereIndexedOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE = 50 OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereIndexedOrNonIndexed");
    }

    @Test
    void updateWithWhereIndexedAndNonIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (AGE = 50) AND (BALANCE > 5000)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereIndexedAndNonIndexedInParentheses");
    }

    @Test
    void updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
    }

    @Test
    void deleteWithWhereSequencePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereSequencePrimaryKey");
    }

    @Test
    void deleteWithWhereSequencePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereSequencePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereSequencePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void deleteWithWhereNoIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereNoIndex");
    }

    @Test
    void deleteWithWhereHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereHashIndex");
    }

    @Test
    void deleteWithWhereBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE = 50", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereBTreeIndex");
    }

    @Test
    void deleteWithWhereUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueIndex");
    }

    @Test
    void deleteWithWhereUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE = 'CODE500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueClusteredIndex");
    }

    @Test
    void deleteWithWherePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWherePrimaryKey");
    }

    @Test
    void deleteWithWhereIndexedAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE = 50 AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereIndexedAndNonIndexed");
    }

    @Test
    void deleteWithWhereIndexedAndNonIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (AGE = 50) AND (BALANCE > 5000)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereIndexedAndNonIndexedInParentheses");
    }

    @Test
    void deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (  AGE  =  50  ) AND (  BALANCE  >  5000  )", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereIndexedAndNonIndexedInParenthesesWithSpaces");
    }

    @Test
    void deleteWithWhereTwoIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereTwoIndexed");
    }

    @Test
    void deleteWithWhereUniqueBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueBTreeHashIndexed");
    }

    @Test
    void deleteWithWhereUniqueClusteredBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE = 'CODE500' AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueClusteredBTreeHashIndexed");
    }

    @Test
    void deleteWithWherePrimaryKeyBTreeHashIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID = 500 AND AGE = 50 AND NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWherePrimaryKeyBTreeHashIndexed");
    }

    @Test
    void deleteWithWhereUniqueBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueBTreeHashIndexedInParentheses");
    }

    @Test
    void deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereUniqueClusteredBTreeHashIndexedInParentheses");
    }

    @Test
    void deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE (ID = 500) AND (AGE = 50) AND (NAME = 'User500')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWherePrimaryKeyBTreeHashIndexedInParentheses");
    }

    @Test
    void deleteWithWhereIndexedOrIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE = 50 OR NAME = 'User500'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereIndexedOrIndexed");
    }

    @Test
    void deleteWithWhereIndexedOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE = 50 OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereIndexedOrNonIndexed");
    }
}
