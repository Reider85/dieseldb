package diesel;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Regression tests for the prompt-23 dead-code removal (Sonar S2583, S108,
 * S1144, S1068): the deleted helpers (QueryParser.splitOrderByClause,
 * QueryParser.parseLimitClause, QueryParser.areSubQueriesEquivalent, the
 * QueryParser.OPERATORS/originalQuery fields and the SelectQuery.subQueries
 * field) were unreachable, so the public features they could have served must
 * still work end-to-end after their removal - multi-column ORDER BY, LIMIT and
 * LIMIT+OFFSET, scalar subqueries in SELECT/GROUP BY, and condition
 * tokenization that ends exactly at the end of the string (the previously
 * dead "<end>" ternary branch).
 */
public class DeadCodeRemovalTest {

    private static final String TABLE = "DEADCODE_T";

    private Database database;

    @BeforeEach
    void setUp() {
        new File(TABLE + ".csv").delete();
        new File(TABLE + ".table").delete();
        database = new Database();
        database.executeQuery("CREATE TABLE " + TABLE
                + " (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, AGE, BALANCE) VALUES (1, 'Alice', 30, 100.5)", null);
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, AGE, BALANCE) VALUES (2, 'Bob', 20, 200.5)", null);
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, AGE, BALANCE) VALUES (3, 'Carol', 25, 50.5)", null);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    @Test
    void multiColumnOrderByStillParsesAndSorts() {
        List<Map<String, Object>> rows = select("SELECT ID FROM " + TABLE
                + " ORDER BY AGE ASC, ID DESC");
        assertEquals(3, rows.size());
        assertEquals(2L, ((Number) rows.get(0).get("ID")).longValue());
        assertEquals(3L, ((Number) rows.get(1).get("ID")).longValue());
        assertEquals(1L, ((Number) rows.get(2).get("ID")).longValue());
    }

    @Test
    void limitWithoutOffsetStillWorks() {
        List<Map<String, Object>> rows = select("SELECT ID FROM " + TABLE + " ORDER BY ID ASC LIMIT 2");
        assertEquals(2, rows.size());
        assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue());
        assertEquals(2L, ((Number) rows.get(1).get("ID")).longValue());
    }

    @Test
    void limitWithOffsetStillWorks() {
        List<Map<String, Object>> rows = select("SELECT ID FROM " + TABLE + " ORDER BY ID ASC LIMIT 2 OFFSET 1");
        assertEquals(2, rows.size());
        assertEquals(2L, ((Number) rows.get(0).get("ID")).longValue());
        assertEquals(3L, ((Number) rows.get(1).get("ID")).longValue());
    }

    @Test
    void scalarSubqueryInSelectAndGroupByStillWorks() {
        assertDoesNotThrow(() -> select("SELECT (SELECT NAME FROM " + TABLE + " WHERE ID = u.ID LIMIT 1) AS user_name, "
                + "COUNT(*) AS user_count FROM " + TABLE + " u WHERE AGE > "
                + "(SELECT AGE FROM " + TABLE + " WHERE ID = 2 LIMIT 1) "
                + "GROUP BY (SELECT NAME FROM " + TABLE + " WHERE ID = u.ID LIMIT 1) LIMIT 10"),
                "scalar subquery in SELECT/GROUP BY");
        assertDoesNotThrow(() -> select("SELECT (SELECT NAME FROM " + TABLE + " WHERE ID = u.ID LIMIT 1) AS user_name_alias, "
                + "COUNT(*) AS user_count FROM " + TABLE + " u WHERE AGE > "
                + "(SELECT AGE FROM " + TABLE + " WHERE ID = 2 LIMIT 1) AS age_subquery "
                + "GROUP BY (SELECT NAME FROM " + TABLE + " WHERE ID = u.ID LIMIT 1) LIMIT 10"),
                "scalar subquery in SELECT/GROUP BY with aliases");
    }

    @Test
    void conditionTokenizedAtExactEndOfString() {
        List<Map<String, Object>> rows = select("SELECT ID FROM " + TABLE + " WHERE AGE > 20");
        assertEquals(2, rows.size());
        rows = select("SELECT ID FROM " + TABLE + " WHERE NAME = 'Bob'");
        assertEquals(1, rows.size());
        assertEquals(2L, ((Number) rows.get(0).get("ID")).longValue());
    }

    @Test
    void subqueryInWhereInAndLikeStillWork() {
        List<Map<String, Object>> rows = select("SELECT ID FROM " + TABLE
                + " WHERE AGE > (SELECT MIN(AGE) FROM " + TABLE + ")");
        assertEquals(2, rows.size());
        rows = select("SELECT ID FROM " + TABLE + " WHERE ID IN (1, 3) AND AGE < 30");
        assertEquals(1, rows.size());
        assertEquals(3L, ((Number) rows.get(0).get("ID")).longValue());
        rows = select("SELECT ID FROM " + TABLE + " WHERE NAME LIKE 'A%'");
        assertEquals(1, rows.size());
    }
}
