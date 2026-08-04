package diesel;

import diesel.Database;

import java.math.BigDecimal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InTest {

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
    void selectWithWhereInBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)", null), "selectWithWhereInBTreeIndex");
    }

    @Test
    void selectWithWhereInHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502')", null), "selectWithWhereInHashIndex");
    }

    @Test
    void selectWithWhereInUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502)", null), "selectWithWhereInUniqueIndex");
    }

    @Test
    void selectWithWhereInUniqueClusteredIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null), "selectWithWhereInUniqueClusteredIndex");
    }

    @Test
    void selectWithWhereInPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502)", null), "selectWithWhereInPrimaryKey");
    }

    @Test
    void selectWithWhereInBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) AND BALANCE > 5000", null), "selectWithWhereInBTreeIndexAndNonIndexed");
    }

    @Test
    void selectWithWhereInHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000", null), "selectWithWhereInHashIndexAndNonIndexed");
    }

    @Test
    void selectWithWhereInUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null), "selectWithWhereInUniqueIndexAndNonIndexed");
    }

    @Test
    void selectWithWhereInUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000", null), "selectWithWhereInUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void selectWithWhereInPrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null), "selectWithWhereInPrimaryKeyAndNonIndexed");
    }

    @Test
    void selectWithWhereInBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) OR BALANCE > 5000", null), "selectWithWhereInBTreeIndexOrNonIndexed");
    }

    @Test
    void selectWithWhereInHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000", null), "selectWithWhereInHashIndexOrNonIndexed");
    }

    @Test
    void selectWithWhereInUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null), "selectWithWhereInUniqueIndexOrNonIndexed");
    }

    @Test
    void selectWithWhereInUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000", null), "selectWithWhereInUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void selectWithWhereInPrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null), "selectWithWhereInPrimaryKeyOrNonIndexed");
    }

    @Test
    void updateWithWhereInBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInBTreeIndex");
    }

    @Test
    void updateWithWhereInHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInHashIndex");
    }

    @Test
    void updateWithWhereInUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueIndex");
    }

    @Test
    void updateWithWhereInUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueClusteredIndex");
    }

    @Test
    void updateWithWhereInPrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInPrimaryKey");
    }

    @Test
    void updateWithWhereInBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInBTreeIndexAndNonIndexed");
    }

    @Test
    void updateWithWhereInHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInHashIndexAndNonIndexed");
    }

    @Test
    void updateWithWhereInUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueIndexAndNonIndexed");
    }

    @Test
    void updateWithWhereInUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void updateWithWhereInPrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInPrimaryKeyAndNonIndexed");
    }

    @Test
    void updateWithWhereInBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE AGE IN (50, 51, 52) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInBTreeIndexOrNonIndexed");
    }

    @Test
    void updateWithWhereInHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInHashIndexOrNonIndexed");
    }

    @Test
    void updateWithWhereInUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueIndexOrNonIndexed");
    }

    @Test
    void updateWithWhereInUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void updateWithWhereInPrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithWhereInPrimaryKeyOrNonIndexed");
    }

    @Test
    void deleteWithWhereInBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE IN (50, 51, 52)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInBTreeIndex");
    }

    @Test
    void deleteWithWhereInHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInHashIndex");
    }

    @Test
    void deleteWithWhereInUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueIndex");
    }

    @Test
    void deleteWithWhereInUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502')", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueClusteredIndex");
    }

    @Test
    void deleteWithWhereInPrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502)", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInPrimaryKey");
    }

    @Test
    void deleteWithWhereInBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE IN (50, 51, 52) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInBTreeIndexAndNonIndexed");
    }

    @Test
    void deleteWithWhereInHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInHashIndexAndNonIndexed");
    }

    @Test
    void deleteWithWhereInUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueIndexAndNonIndexed");
    }

    @Test
    void deleteWithWhereInUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void deleteWithWhereInPrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502) AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInPrimaryKeyAndNonIndexed");
    }

    @Test
    void deleteWithWhereInBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE AGE IN (50, 51, 52) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInBTreeIndexOrNonIndexed");
    }

    @Test
    void deleteWithWhereInHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInHashIndexOrNonIndexed");
    }

    @Test
    void deleteWithWhereInUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueIndexOrNonIndexed");
    }

    @Test
    void deleteWithWhereInUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void deleteWithWhereInPrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE ID IN (500, 501, 502) OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithWhereInPrimaryKeyOrNonIndexed");
    }
}
