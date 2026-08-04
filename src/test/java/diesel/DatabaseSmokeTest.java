package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseSmokeTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable("SMOKE");
        } catch (IllegalArgumentException ignored) {
            // table does not exist yet, nothing to drop
        }
        database.executeQuery("CREATE TABLE SMOKE (ID LONG PRIMARY KEY SEQUENCE(smoke_seq 1 1), NAME STRING, AGE INTEGER)", null);
        database.executeQuery("INSERT INTO SMOKE (NAME, AGE) VALUES ('Alice', 25)", null);
        database.executeQuery("INSERT INTO SMOKE (NAME, AGE) VALUES ('Bob', 30)", null);
    }

    @Test
    void selectAllReturnsAllInsertedRows() {
        Object result = database.executeQuery("SELECT ID, NAME FROM SMOKE", null);
        assertInstanceOf(List.class, result);
        assertEquals(2, ((List<?>) result).size());
    }

    @Test
    void selectWithWhereReturnsMatchingRows() {
        Object result = database.executeQuery("SELECT ID, NAME FROM SMOKE WHERE NAME = 'Alice'", null);
        assertInstanceOf(List.class, result);
        assertEquals(1, ((List<?>) result).size());
    }

    @Test
    void selectWithOrderBySortsRows() {
        Object result = database.executeQuery("SELECT NAME FROM SMOKE ORDER BY AGE DESC", null);
        assertInstanceOf(List.class, result);
        List<?> rows = (List<?>) result;
        assertEquals(2, rows.size());
        assertEquals("Bob", ((java.util.Map<?, ?>) rows.get(0)).get("NAME"));
    }
}
