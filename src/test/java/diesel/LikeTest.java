package diesel;

import diesel.Database;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LikeTest {

    private static final int RECORD_COUNT = 10;

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
                    i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP)
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("USERS");
    }

    @Test
    void selectWithLikeBTreeIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null), "selectWithLikeBTreeIndex");
    }

    @Test
    void selectWithLikeHashIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null), "selectWithLikeHashIndex");
    }

    @Test
    void selectWithLikeUniqueIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null), "selectWithLikeUniqueIndex");
    }

    @Test
    void selectWithLikeUniqueClusteredIndex() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null), "selectWithLikeUniqueClusteredIndex");
    }

    @Test
    void selectWithLikePrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null), "selectWithLikePrimaryKey");
    }

    @Test
    void selectWithLikeBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null), "selectWithLikeBTreeIndexAndNonIndexed");
    }

    @Test
    void selectWithLikeHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null), "selectWithLikeHashIndexAndNonIndexed");
    }

    @Test
    void selectWithLikeUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null), "selectWithLikeUniqueIndexAndNonIndexed");
    }

    @Test
    void selectWithLikeUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null), "selectWithLikeUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void selectWithLikePrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null), "selectWithLikePrimaryKeyAndNonIndexed");
    }

    @Test
    void selectWithLikeBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null), "selectWithLikeBTreeIndexOrNonIndexed");
    }

    @Test
    void selectWithLikeHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null), "selectWithLikeHashIndexOrNonIndexed");
    }

    @Test
    void selectWithLikeUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null), "selectWithLikeUniqueIndexOrNonIndexed");
    }

    @Test
    void selectWithLikeUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null), "selectWithLikeUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void selectWithLikePrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null), "selectWithLikePrimaryKeyOrNonIndexed");
    }

    @Test
    void updateWithLikeBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeBTreeIndex");
    }

    @Test
    void updateWithLikeHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeHashIndex");
    }

    @Test
    void updateWithLikeUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueIndex");
    }

    @Test
    void updateWithLikeUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueClusteredIndex");
    }

    @Test
    void updateWithLikePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikePrimaryKey");
    }

    @Test
    void updateWithLikeBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeBTreeIndexAndNonIndexed");
    }

    @Test
    void updateWithLikeHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeHashIndexAndNonIndexed");
    }

    @Test
    void updateWithLikeUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueIndexAndNonIndexed");
    }

    @Test
    void updateWithLikeUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void updateWithLikePrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikePrimaryKeyAndNonIndexed");
    }

    @Test
    void updateWithLikeBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeBTreeIndexOrNonIndexed");
    }

    @Test
    void updateWithLikeHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeHashIndexOrNonIndexed");
    }

    @Test
    void updateWithLikeUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueIndexOrNonIndexed");
    }

    @Test
    void updateWithLikeUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikeUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void updateWithLikePrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("UPDATE USERS SET BALANCE = 6000 WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "updateWithLikePrimaryKeyOrNonIndexed");
    }

    @Test
    void deleteWithLikeBTreeIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeBTreeIndex");
    }

    @Test
    void deleteWithLikeHashIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeHashIndex");
    }

    @Test
    void deleteWithLikeUniqueIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueIndex");
    }

    @Test
    void deleteWithLikeUniqueClusteredIndex() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueClusteredIndex");
    }

    @Test
    void deleteWithLikePrimaryKey() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikePrimaryKey");
    }

    @Test
    void deleteWithLikeBTreeIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeBTreeIndexAndNonIndexed");
    }

    @Test
    void deleteWithLikeHashIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeHashIndexAndNonIndexed");
    }

    @Test
    void deleteWithLikeUniqueIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueIndexAndNonIndexed");
    }

    @Test
    void deleteWithLikeUniqueClusteredIndexAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueClusteredIndexAndNonIndexed");
    }

    @Test
    void deleteWithLikePrimaryKeyAndNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' AND BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikePrimaryKeyAndNonIndexed");
    }

    @Test
    void deleteWithLikeBTreeIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeBTreeIndexOrNonIndexed");
    }

    @Test
    void deleteWithLikeHashIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE NAME LIKE '%ser500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeHashIndexOrNonIndexed");
    }

    @Test
    void deleteWithLikeUniqueIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueIndexOrNonIndexed");
    }

    @Test
    void deleteWithLikeUniqueClusteredIndexOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikeUniqueClusteredIndexOrNonIndexed");
    }

    @Test
    void deleteWithLikePrimaryKeyOrNonIndexed() {
        assertDoesNotThrow(() -> {
            database.executeQuery("DELETE FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", null);
            database.getTable("USERS").saveToFile("USERS");
        }, "deleteWithLikePrimaryKeyOrNonIndexed");
    }
}
