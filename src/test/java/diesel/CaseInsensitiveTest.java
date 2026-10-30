package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 4 #49 (R3-013c) acceptance: {@code COLLATE CASE_INSENSITIVE} makes
 * equality comparisons ({@code =}, {@code !=}, {@code IN}) on a STRING
 * column case-insensitive while a default column in the same table stays
 * case-sensitive; UPDATE/DELETE honor the collation; CREATE INDEX on a
 * case-insensitive column is rejected (index keys are case-sensitive); and
 * the collation survives a .table reopen (format v5 persistence).
 */
@Tag("query")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CaseInsensitiveTest {

    private static final String TABLE = "CI_T";

    private Database database;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery(
                "CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(ci_seq 1 1), "
                        + "CI_NAME STRING COLLATE CASE_INSENSITIVE, CS_NAME STRING)",
                null);
        database.executeQuery("INSERT INTO " + TABLE + " (CI_NAME, CS_NAME) VALUES ('Name2', 'Name2')", null);
        database.executeQuery("INSERT INTO " + TABLE + " (CI_NAME, CS_NAME) VALUES ('OTHER', 'OTHER')", null);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(String query) {
        Object result = database.executeQuery(query, null);
        assertTrue(result instanceof List, "query must return a result set: " + query);
        return (List<Map<String, Object>>) result;
    }

    @Test
    @Order(1)
    void ciColumnMatchesAnyCase() {
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME = 'Name2'").size(),
                "exact case must match");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME = 'name2'").size(),
                "lowercase literal must match the mixed-case row on a CI column");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME = 'NAME2'").size(),
                "uppercase literal must match the mixed-case row on a CI column");
        assertEquals(2, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME != 'zzz'").size(),
                "NOT EQUALS on a CI column must treat every non-matching row as unequal");
    }

    @Test
    @Order(2)
    void defaultColumnStaysCaseSensitive() {
        assertEquals(0, select("SELECT ID FROM " + TABLE + " WHERE CS_NAME = 'name2'").size(),
                "a column without COLLATE must keep case-sensitive equality");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CS_NAME = 'Name2'").size(),
                "exact case must still match on the default column");
    }

    @Test
    @Order(3)
    void inListOnCiColumnIsCaseInsensitive() {
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME IN ('name2', 'zzz')").size(),
                "IN literals must compare case-insensitively on a CI column");
        assertEquals(0, select("SELECT ID FROM " + TABLE + " WHERE CS_NAME IN ('name2')").size(),
                "IN on a default column must stay case-sensitive");
    }

    @Test
    @Order(4)
    void updateAndDeleteHonorCollation() {
        database.executeQuery(
                "CREATE TABLE CI_UPD (ID LONG PRIMARY KEY SEQUENCE(ci_upd_seq 1 1), "
                        + "NAME STRING COLLATE CASE_INSENSITIVE)", null);
        database.executeQuery("INSERT INTO CI_UPD (NAME) VALUES ('Target')", null);

        database.executeQuery("UPDATE CI_UPD SET NAME = 'Renamed' WHERE NAME = 'target'", null);
        assertEquals("Renamed", select("SELECT NAME FROM CI_UPD WHERE ID = 1").get(0).get("NAME"),
                "UPDATE WHERE with the wrong case must hit a CI column");

        database.executeQuery("DELETE FROM CI_UPD WHERE NAME = 'renamed'", null);
        assertEquals(0, select("SELECT ID FROM CI_UPD").size(),
                "DELETE WHERE with the wrong case must remove a CI row");
    }

    @Test
    @Order(5)
    void createIndexOnCiColumnRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery("CREATE INDEX ON " + TABLE + " (CI_NAME)", null),
                "CREATE INDEX on a CASE_INSENSITIVE column must be rejected");
        assertTrue(String.valueOf(e.getMessage()).contains("CASE_INSENSITIVE"),
                "rejection message must name the collation: " + e.getMessage());

        assertThrows(IllegalArgumentException.class, () ->
                        database.executeQuery("CREATE HASH INDEX ON " + TABLE + " (CI_NAME)", null),
                "CREATE HASH INDEX on a CASE_INSENSITIVE column must be rejected");
        assertThrows(IllegalArgumentException.class, () ->
                        database.executeQuery(
                                "CREATE INDEX ON " + TABLE + " (CI_NAME, CS_NAME)", null),
                "a composite index containing a CASE_INSENSITIVE column must be rejected");
    }

    @Test
    @Order(6)
    void collationSurvivesReopen() {
        Table before = database.getTable(TABLE);
        assertTrue(before.isCaseInsensitiveColumn("CI_NAME"),
                "CI_NAME must be case-insensitive before reopen");

        // close() persists every table (.table format v5 carries the collation map).
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.loadTablesFromDisk();

        Table table = database.getTable(TABLE);
        assertTrue(table.isCaseInsensitiveColumn("CI_NAME"),
                "CASE_INSENSITIVE collation must survive the .table reopen");

        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE CI_NAME = 'name2'").size(),
                "wrong-case lookup must still match after reopen");
    }
}
