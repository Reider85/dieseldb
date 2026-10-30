package diesel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 4 #49 (R3-013c) acceptance: string literals keep their original
 * case end to end. Round-trips INSERT 'Name2' / SELECT WHERE col='Name2'
 * (hit) vs col='name2' (miss), UPDATE SET and DELETE preserve case, the
 * JOIN ON literal (the residual bug this prompt fixed - the ON clause used
 * to flow through the uppercasing normalizeQueryString) matches with its
 * original case, and literals containing SQL-looking words or operators
 * survive query normalization untouched.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("query")
@ResourceLock("diesel.AbstractDieselTest.tempDir")
public class StringCaseSensitivityTest extends AbstractDieselTest {

    @BeforeAll
    void setup() {
        dropTable("SCS_MAIN");
        dropTable("SCS_REF");
        database.executeQuery(
                "CREATE TABLE SCS_MAIN (ID LONG PRIMARY KEY SEQUENCE(scs_seq 1 1), NAME STRING, CODE STRING)",
                null);
        database.executeQuery(
                "CREATE TABLE SCS_REF (REF_ID LONG PRIMARY KEY SEQUENCE(scs_ref_seq 1 1), CODE STRING, INFO STRING)",
                null);
        database.executeQuery("INSERT INTO SCS_MAIN (NAME, CODE) VALUES ('Name2', 'MainA')", null);
        database.executeQuery("INSERT INTO SCS_MAIN (NAME, CODE) VALUES ('NAME2', 'MainB')", null);
        database.executeQuery("INSERT INTO SCS_REF (CODE, INFO) VALUES ('MainA', 'RefInfo')", null);
    }

    @AfterAll
    void tearDown() {
        dropTable("SCS_MAIN");
        dropTable("SCS_REF");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(String query) {
        Object result = database.executeQuery(query, null);
        assertTrue(result instanceof List, "query must return a result set: " + query);
        return (List<Map<String, Object>>) result;
    }

    @Test
    @Order(1)
    void insertSelectRoundTripPreservesCase() {
        List<Map<String, Object>> hit = select(
                "SELECT ID, NAME FROM SCS_MAIN WHERE NAME = 'Name2'");
        assertEquals(1, hit.size(), "WHERE NAME = 'Name2' must match the mixed-case row");
        assertEquals("Name2", hit.get(0).get("NAME"), "returned value must keep the inserted case");

        List<Map<String, Object>> miss = select(
                "SELECT ID, NAME FROM SCS_MAIN WHERE NAME = 'name2'");
        assertEquals(0, miss.size(), "WHERE NAME = 'name2' must not match 'Name2' (case-sensitive)");
    }

    @Test
    @Order(2)
    void uppercaseLiteralMatchesOnlyUppercaseRow() {
        List<Map<String, Object>> rows = select(
                "SELECT ID, NAME FROM SCS_MAIN WHERE NAME = 'NAME2'");
        assertEquals(1, rows.size(), "WHERE NAME = 'NAME2' must match only the uppercase row");
        assertEquals("NAME2", rows.get(0).get("NAME"));
    }

    @Test
    @Order(3)
    void updateSetPreservesLiteralCase() {
        dropTable("SCS_UPD");
        database.executeQuery(
                "CREATE TABLE SCS_UPD (ID LONG PRIMARY KEY SEQUENCE(scs_upd_seq 1 1), NAME STRING)", null);
        database.executeQuery("INSERT INTO SCS_UPD (NAME) VALUES ('before')", null);
        database.executeQuery("UPDATE SCS_UPD SET NAME = 'AfterCase' WHERE ID = 1", null);

        List<Map<String, Object>> stored = select("SELECT NAME FROM SCS_UPD WHERE ID = 1");
        assertEquals(1, stored.size());
        assertEquals("AfterCase", stored.get(0).get("NAME"),
                "UPDATE SET must store the literal with its original case");

        List<Map<String, Object>> wrongCase = select(
                "SELECT NAME FROM SCS_UPD WHERE NAME = 'aftercase'");
        assertEquals(0, wrongCase.size(), "post-UPDATE lookup with the wrong case must miss");
        dropTable("SCS_UPD");
    }

    @Test
    @Order(4)
    void deleteMatchesExactCaseOnly() {
        dropTable("SCS_DEL");
        database.executeQuery(
                "CREATE TABLE SCS_DEL (ID LONG PRIMARY KEY SEQUENCE(scs_del_seq 1 1), NAME STRING)", null);
        database.executeQuery("INSERT INTO SCS_DEL (NAME) VALUES ('KeepMe')", null);

        database.executeQuery("DELETE FROM SCS_DEL WHERE NAME = 'keepme'", null);
        assertEquals(1, countRows("SELECT ID FROM SCS_DEL"),
                "DELETE with the wrong case must not remove the row");

        database.executeQuery("DELETE FROM SCS_DEL WHERE NAME = 'KeepMe'", null);
        assertEquals(0, countRows("SELECT ID FROM SCS_DEL"),
                "DELETE with the exact case must remove the row");
        dropTable("SCS_DEL");
    }

    @Test
    @Order(5)
    void joinOnLiteralKeepsOriginalCase() {
        // Regression for the residual R3-013c bug: the JOIN ON clause is the
        // one condition path built from the normalized query text, which used
        // to uppercase single-quoted literals ('MainA' -> 'MAINA').
        List<Map<String, Object>> rows = select(
                "SELECT u.NAME, r.INFO FROM SCS_MAIN u "
                        + "INNER JOIN SCS_REF r ON r.CODE = u.CODE AND r.INFO = 'RefInfo' "
                        + "WHERE u.CODE = 'MainA'");
        assertEquals(1, rows.size(),
                "JOIN ON literal 'RefInfo' must keep its case and match exactly one row");
        assertEquals("RefInfo", rows.get(0).get("INFO"));

        List<Map<String, Object>> wrongCase = select(
                "SELECT u.NAME, r.INFO FROM SCS_MAIN u "
                        + "INNER JOIN SCS_REF r ON r.CODE = u.CODE AND r.INFO = 'refinfo' "
                        + "WHERE u.CODE = 'MainA'");
        assertEquals(0, wrongCase.size(),
                "JOIN ON with the wrong-case literal must not match");
    }

    @Test
    @Order(6)
    void literalContainingKeywordLikeSurvivesNormalization() {
        dropTable("SCS_WORDS");
        database.executeQuery(
                "CREATE TABLE SCS_WORDS (ID LONG PRIMARY KEY SEQUENCE(scs_w_seq 1 1), NAME STRING)", null);
        database.executeQuery("INSERT INTO SCS_WORDS (NAME) VALUES ('like equals greater_than')", null);
        database.executeQuery("INSERT INTO SCS_WORDS (NAME) VALUES ('plain')", null);

        // normalizeCondition rewrites the words EQUALS/LIKE/GREATER_THAN into
        // operators - but only outside single quotes since prompt4 #49.
        List<Map<String, Object>> rows = select(
                "SELECT ID FROM SCS_WORDS WHERE NAME = 'like equals greater_than'");
        assertEquals(1, rows.size(),
                "a literal containing SQL-looking words must survive normalization verbatim");

        // Whitespace inside a literal must not be collapsed either.
        database.executeQuery("INSERT INTO SCS_WORDS (NAME) VALUES ('a  =  b')", null);
        List<Map<String, Object>> spaced = select(
                "SELECT ID FROM SCS_WORDS WHERE NAME = 'a  =  b'");
        assertEquals(1, spaced.size(),
                "double spaces inside a literal must survive query normalization");
        dropTable("SCS_WORDS");
    }

    @Test
    @Order(7)
    void likePatternIsCaseSensitiveByDefault() {
        List<Map<String, Object>> rows = select(
                "SELECT ID FROM SCS_MAIN WHERE NAME LIKE 'name%'");
        assertEquals(0, rows.size(), "LIKE must stay case-sensitive on a default column");
    }

    @Test
    @Order(8)
    void quotedIdentifierCaseStillPreserved() {
        // Double-quoted identifiers kept their case before #49 and must keep it now.
        List<Map<String, Object>> rows = select(
                "SELECT \"NAME\" FROM SCS_MAIN WHERE \"NAME\" = 'Name2'");
        assertEquals(1, rows.size(), "double-quoted identifier path must stay intact");
    }

    @Test
    @Order(9)
    void inListPreservesLiteralCase() {
        List<Map<String, Object>> rows = select(
                "SELECT ID FROM SCS_MAIN WHERE NAME IN ('Name2', 'missing')");
        assertEquals(1, rows.size(), "IN list literals must keep their case");
        List<Map<String, Object>> wrongCase = select(
                "SELECT ID FROM SCS_MAIN WHERE NAME IN ('name2')");
        assertEquals(0, wrongCase.size(), "IN with the wrong case must not match");
    }

    @Test
    @Order(10)
    void unsupportedCollationRejected() {
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(
                "CREATE TABLE SCS_BAD (ID LONG PRIMARY KEY, NAME STRING COLLATE NOCASE)", null),
                "unknown collation names must be rejected");
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(
                "CREATE TABLE SCS_BAD (ID LONG PRIMARY KEY, AGE INTEGER COLLATE CASE_INSENSITIVE)", null),
                "COLLATE on a non-STRING column must be rejected");
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(
                "CREATE TABLE SCS_BAD (ID LONG PRIMARY KEY, NAME STRING PRIMARY KEY COLLATE CASE_INSENSITIVE)", null),
                "COLLATE on a PRIMARY KEY column must be rejected");
    }
}
