package diesel;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Edge-case tests for the prompt-22 null-pointer hardening (Sonar java:S2259):
 * null query inputs must fail with a clear exception instead of a raw NPE
 * deep inside the engine, a {@link DatabaseClient} must not NPE before
 * {@code connect()}, {@link SelectQuery} must guard its public {@code table}
 * parameter and the documented-nullable {@code Table.getDatabase()}, and the
 * DML value converters must tolerate a {@code columnTypes} lookup that misses
 * the column.
 */
public class NullSafetyTest {

    private static final String TABLE = "NULLSAFE_T";
    private static final String TABLE_B = "NULLSAFE_B";

    private Database database;

    @BeforeEach
    void setUp() {
        new File(TABLE + ".csv").delete();
        new File(TABLE + ".table").delete();
        new File(TABLE_B + ".csv").delete();
        new File(TABLE_B + ".table").delete();
        database = new Database();
        database.executeQuery("CREATE TABLE " + TABLE
                + " (ID LONG PRIMARY KEY, NAME STRING, SCORE INTEGER)", null);
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, SCORE) VALUES (1, 'Alice', 10)", null);
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, SCORE) VALUES (2, 'Bob', 20)", null);
        database.executeQuery("CREATE TABLE " + TABLE_B + " (ID LONG PRIMARY KEY, Z STRING)", null);
        database.executeQuery("INSERT INTO " + TABLE_B + " (ID, Z) VALUES (1, 'z')", null);
    }

    @Test
    void databaseExecuteQueryWithNullQueryThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(null, null));
    }

    @Test
    void queryParserParseWithNullQueryThrowsIllegalArgumentException() {
        QueryParser parser = new QueryParser();
        assertThrows(IllegalArgumentException.class, () -> parser.parse(null, database));
    }

    @Test
    void subqueryParserParseWithNullQueryThrowsIllegalArgumentException() {
        SubqueryParser parser = new SubqueryParser();
        assertThrows(IllegalArgumentException.class, () -> parser.parse(null, database));
    }

    @Test
    void subqueryParserContainsSubqueryWithNullReturnsFalse() {
        assertFalse(new SubqueryParser().containsSubquery(null));
    }

    @Test
    void isExplainQueryWithNullReturnsFalse() {
        assertFalse(QueryParser.isExplainQuery(null));
    }

    @Test
    void clientExecuteQueryBeforeConnectThrows() {
        DatabaseClient client = new DatabaseClient("localhost", 1);
        assertThrows(IllegalStateException.class, () -> client.executeQuery("SELECT 1"));
    }

    @Test
    void clientDisconnectBeforeConnectDoesNotThrow() {
        DatabaseClient client = new DatabaseClient("localhost", 1);
        assertDoesNotThrow(client::disconnect);
    }

    @Test
    void selectQueryExecuteWithNullTableThrows() {
        Query<?> query = new QueryParser().parse("SELECT ID FROM " + TABLE + " WHERE ID = 1", database);
        SelectQuery selectQuery = (SelectQuery) query;
        assertThrows(NullPointerException.class, () -> selectQuery.execute(null));
    }

    @Test
    void selectQueryExecuteWithDetachedTableThrows() {
        Table detached = detachedTable();
        Query<?> query = new QueryParser().parse("SELECT ID FROM " + TABLE + " WHERE ID = 1", database);
        SelectQuery selectQuery = (SelectQuery) query;
        assertThrows(NullPointerException.class, () -> selectQuery.execute(detached));
    }

    @Test
    void describePlanWithDetachedTableThrows() {
        Table detached = detachedTable();
        Query<?> query = new QueryParser().parse("SELECT ID FROM " + TABLE + " WHERE ID = 1", database);
        SelectQuery selectQuery = (SelectQuery) query;
        assertThrows(NullPointerException.class, () -> selectQuery.describePlan(detached));
    }

    @Test
    void updateQueryAgainstTableMissingSetColumnDoesNotThrow() {
        Query<?> query = new QueryParser().parse(
                "UPDATE " + TABLE + " SET SCORE = 5 WHERE ID = 1", database);
        UpdateQuery updateQuery = (UpdateQuery) query;
        Table tableB = database.getTable(TABLE_B);
        // NULLSAFE_B has no SCORE column: columnTypes.get("SCORE") is null and
        // the converter must pass the value through instead of NPE-ing.
        assertDoesNotThrow(() -> updateQuery.execute(tableB));
    }

    @Test
    void deleteQueryAgainstTableMissingWhereColumnDoesNotThrow() {
        Query<?> query = new QueryParser().parse(
                "DELETE FROM " + TABLE + " WHERE SCORE = 10", database);
        DeleteQuery deleteQuery = (DeleteQuery) query;
        Table tableB = database.getTable(TABLE_B);
        assertDoesNotThrow(() -> deleteQuery.execute(tableB));
    }

    @Test
    void normalSelectJoinAndSubqueryStillWork() {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT " + TABLE + ".ID FROM " + TABLE + " JOIN " + TABLE_B + " ON "
                        + TABLE + ".ID = " + TABLE_B + ".ID", null);
        assertEquals(1, rows.size());
        assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue());

        rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT ID FROM " + TABLE + " WHERE ID IN (SELECT ID FROM " + TABLE_B + ")", null);
        assertEquals(1, rows.size());

        rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT ID FROM " + TABLE + " WHERE ID = (SELECT ID FROM " + TABLE_B + " WHERE ID = 1)", null);
        assertEquals(1, rows.size());
    }

    private Table detachedTable() {
        Map<String, Class<?>> columnTypes = new HashMap<>();
        columnTypes.put("ID", Long.class);
        columnTypes.put("NAME", String.class);
        columnTypes.put("SCORE", Integer.class);
        return new Table(null, TABLE, List.of("ID", "NAME", "SCORE"), columnTypes, "ID",
                new HashMap<>());
    }
}
