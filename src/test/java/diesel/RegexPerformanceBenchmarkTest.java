package diesel;

import diesel.Database;
import diesel.QueryParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 27 (java:S5842) — regression + performance benchmark for the
 * LIMIT/OFFSET/LIKE/subquery parsing regexes that were optimized with
 * possessive quantifiers and atomic groups.
 *
 * The benchmark parses ~10K SQL queries (only the parse phase, no execution)
 * and asserts the wall-clock cost stays within a generous bound so a
 * catastrophic-backtracking regression in the parsing regexes would be caught.
 */
public class RegexPerformanceBenchmarkTest {

    private static final String[] QUERIES = new String[]{
            "SELECT ID, NAME FROM USERS WHERE AGE > 20 AND NAME = 'Alice'",
            "SELECT ID, NAME FROM USERS WHERE NAME LIKE 'A%' OR AGE < 30",
            "SELECT ID FROM USERS WHERE ID IN (1, 2, 3, 4, 5)",
            "SELECT COUNT(*) FROM USERS GROUP BY AGE ORDER BY AGE DESC LIMIT 10 OFFSET 5",
            "SELECT ID, NAME FROM USERS ORDER BY AGE ASC, NAME DESC LIMIT 20",
            "SELECT ID FROM USERS WHERE NAME LIKE 'J%n_' AND BALANCE > 100.00",
            "SELECT ID FROM USERS WHERE (AGE > 20 AND AGE < 40) OR NAME = 'Bob'",
            "SELECT ID, NAME FROM USERS WHERE NAME = 'O''Brien' AND AGE >= 18",
            "SELECT ID FROM USERS WHERE NAME LIKE 'S%' AND AGE IN (20, 30, 40)",
            "SELECT ID, NAME FROM USERS WHERE AGE >= 18 AND AGE <= 65",
            "SELECT ID, NAME FROM USERS WHERE NAME <> 'Carol' AND BALANCE <= 500",
            "SELECT MAX(AGE), MIN(AGE) FROM USERS WHERE ID > 0"
    };

    private Database database;

    @BeforeEach
    void setUp() {
        java.util.logging.Logger.getLogger("diesel").setLevel(java.util.logging.Level.WARNING);
        database = new Database();
        database.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)",
                null);
    }

    @Test
    void benchmarkParse10kQueries() {
        QueryParser parser = new QueryParser();
        int total = 10_000;
        int iterations = Math.max(1, total / QUERIES.length);
        int parsed = 0;

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            for (String query : QUERIES) {
                assertNotNull(parser.parse(query, database), "parse returned null for: " + query);
                parsed++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        System.out.println("[RegexPerformanceBenchmark] parsed " + parsed
                + " SQL queries in " + elapsedMs + " ms");
        // Generous guard: the parse-only loop must stay well under this even on
        // a heavily loaded CI box; a backtracking blow-up would exceed it.
        assertTrue(elapsedMs < 30_000,
                "Parsing " + parsed + " queries took too long: " + elapsedMs + " ms");
    }

    @Test
    void likePatternParsesBasicAndEscapedQuotes() {
        QueryParser parser = new QueryParser();
        // Basic LIKE value (regression guard for the S5842 empty-alternative fix).
        assertDoesNotThrow(() ->
                parser.parse("SELECT ID FROM USERS WHERE NAME LIKE 'A%'", database));
        // SQL escaped quote '' inside the LIKE value now parses correctly instead of
        // being matched via the empty alternative.
        assertDoesNotThrow(() ->
                parser.parse("SELECT ID FROM USERS WHERE NAME LIKE 'O''Brien%'", database));
        // AND-combined LIKE still parses.
        assertDoesNotThrow(() ->
                parser.parse("SELECT ID FROM USERS WHERE NAME LIKE 'J%n_' AND AGE > 20", database));
    }
}
