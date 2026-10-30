package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 4 #49 (R3-013c) acceptance: the {@code compat.uppercase.string.literals}
 * flag restores the pre-#49 legacy behavior where the parser uppercased every
 * string literal - INSERT 'Name2' stores 'NAME2' and both WHERE 'name2' and
 * WHERE 'Name2' match (case-blind). The default (flag false) keeps
 * case-sensitive round-trips, covered by StringCaseSensitivityTest.
 *
 * <p>{@code @Isolated}: the flag is a JVM-wide system property read by the
 * parser on every query - a parallel class parsing at the same moment would
 * observe the flipped value and fail its own case assertions.
 */
@Tag("query")
@org.junit.jupiter.api.parallel.Isolated
public class CompatModeTest {

    private static final String FLAG = "compat.uppercase.string.literals";
    private static final String TABLE = "COMPAT_T";

    private Database database;
    private String previousFlag;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        previousFlag = System.getProperty(FLAG);
        database = new Database();
        database.setDataDir(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
        // Restore in finally-style so a failed assertion never leaks the
        // JVM-wide flag into other test classes.
        if (previousFlag == null) {
            System.clearProperty(FLAG);
        } else {
            System.setProperty(FLAG, previousFlag);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(String query) {
        Object result = database.executeQuery(query, null);
        assertTrue(result instanceof List, "query must return a result set: " + query);
        return (List<Map<String, Object>>) result;
    }

    @Test
    void compatTrueUppercasesStoredLiteralsAndMatchesAnyCase() {
        System.setProperty(FLAG, "true");
        database.executeQuery(
                "CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(compat_seq 1 1), NAME STRING)",
                null);
        database.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Name2')", null);

        // The literal was uppercased at parse time, so the stored value is NAME2.
        List<Map<String, Object>> stored = select("SELECT NAME FROM " + TABLE + " WHERE ID = 1");
        assertEquals(1, stored.size());
        assertEquals("NAME2", stored.get(0).get("NAME"),
                "compat mode must uppercase the literal before it is stored");

        // Both spellings match: the comparison literal is uppercased too.
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'name2'").size(),
                "compat mode must be case-blind for the lowercase spelling");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'Name2'").size(),
                "compat mode must be case-blind for the mixed-case spelling");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'NAME2'").size(),
                "compat mode must be case-blind for the uppercase spelling");
    }

    @Test
    void flagOffRestoresCaseSensitiveBehavior() {
        System.setProperty(FLAG, "false");
        database.executeQuery(
                "CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(compat_seq2 1 1), NAME STRING)",
                null);
        database.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Name2')", null);

        assertEquals("Name2", select("SELECT NAME FROM " + TABLE + " WHERE ID = 1").get(0).get("NAME"),
                "flag=false must store the literal with its original case");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'Name2'").size(),
                "flag=false must match the exact case");
        assertEquals(0, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'name2'").size(),
                "flag=false must not match the wrong case");
    }

    @Test
    void flagFlipBetweenRunsDoesNotServeStaleCachedPlan() {
        // Same SQL text, flag flipped between the two executions: the query
        // cache key carries the flag (Database.executeQueryInternal prefixes
        // COMPAT_UC), so the second run must re-parse under the new policy
        // instead of returning the AST cached under the old one.
        database.executeQuery(
                "CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(compat_seq3 1 1), NAME STRING)",
                null);

        System.setProperty(FLAG, "true");
        database.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Cached')", null);
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'cached'").size(),
                "compat=true run must match the uppercased literal");

        System.setProperty(FLAG, "false");
        // The stored value is 'CACHED' (uppercased by the first run); with the
        // flag off the literal stays 'cached' and must NOT match anymore.
        assertEquals(0, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'cached'").size(),
                "after the flag flips off, the same SQL must be re-parsed case-sensitively");
        assertEquals(1, select("SELECT ID FROM " + TABLE + " WHERE NAME = 'CACHED'").size(),
                "with the flag off the uppercase stored value matches the uppercase literal");
    }
}
