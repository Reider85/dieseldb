package diesel;

import diesel.Database;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for small DieselDB test classes.  Each concrete subclass sets
 * {@link #recordCount()} (default 100) and calls {@link #setupCommonTables()}
 * from its {@code @BeforeAll}.  Helper methods replicate the ones that
 * lived inside the monolithic QuantitativeTest.
 */
public abstract class AbstractDieselTest {

    private static final Logger LOGGER = Logger.getLogger(AbstractDieselTest.class.getName());
    protected static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Committed CSV fixtures (TESTS-9), relative to the repo-root CWD. */
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");

    /**
     * Per-test-class data directory. JUnit injects a fresh, distinct directory
     * into a {@code static @TempDir} field for every test class, so the eleven
     * subclasses that share the USERS/PROFILES/TRANSACTIONS/USER_DETAILS table
     * names cannot see each other's files. A JVM-wide
     * {@code Files.createTempDirectory} let concurrent classes stomp on the
     * same {@code USERS.avro}, inflating it well past {@link #recordCount()}
     * rows and making the scan-heavy {@code PerformanceRegressionTest} queries
     * report 2-6x regressions.
     *
     * <p>The field is STATIC, i.e. one storage location shared by all eleven
     * subclasses — with JUnit 5 class-level parallelism
     * (junit-platform.properties) a second class could overwrite the injected
     * path while the first class was still running, and the first class's
     * cleanup of the other class's directory caused
     * {@code NoSuchFileException} on {@code *.table.tmp}. Every subclass
     * therefore declares {@code @ResourceLock("diesel.AbstractDieselTest.tempDir")}
     * so only one of them runs at a time (see AbstractDieselTestSubclass locking
     * note in TESTS-3). A non-static @TempDir field is NOT an option here:
     * with {@code @TestInstance(PER_CLASS)} the field is not yet injected when
     * the non-static {@code @BeforeAll} runs (null in setupCommonTables).
     */
    @TempDir
    protected static Path tempDir;

    protected final Database database = new Database();
    protected int passed = 0;
    protected int failed = 0;

    protected int recordCount() { return 100; }

    /* ------------------------------------------------------------------ */
    /*  Shared @BeforeAll setup – creates USERS/PROFILES/TRANSACTIONS/    */
    /*  USER_DETAILS with {@link #recordCount()} rows each.               */
    /* ------------------------------------------------------------------ */

    @BeforeAll
    void setupCommonTables() {
        database.setDataDir(tempDir.toString());
        dropTable("USERS");
        dropTable("PROFILES");
        dropTable("TRANSACTIONS");
        dropTable("USER_DETAILS");

        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, DATE_FIELD DATE, ACTIVE BOOLEAN, PRECISION DOUBLE)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);

        database.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)", null);
        database.executeQuery("CREATE TABLE TRANSACTIONS (TRANS_ID LONG PRIMARY KEY SEQUENCE(trans_seq 1 1), USER_ID LONG, TRANS_DATE DATE, AMOUNT BIGDECIMAL)", null);
        database.executeQuery("CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)", null);

        int n = recordCount();
        // Was: n x 4 INSERT via executeQuery (a full SQL parse per call, 4400
        // parses per fast run across the eleven subclasses). Now: 4 bulk loads
        // from committed fixture CSVs (TESTS-9) -- the rows are byte-identical
        // to what the old loop generated (see scripts/generate-fixtures.py and
        // BulkLoadEquivalenceTest).
        try {
            database.bulkLoadCSV("USERS", FIXTURES.resolve("users.csv"));
            database.bulkLoadCSV("PROFILES", FIXTURES.resolve("profiles.csv"));
            database.bulkLoadCSV("TRANSACTIONS", FIXTURES.resolve("transactions.csv"));
            database.bulkLoadCSV("USER_DETAILS", FIXTURES.resolve("user_details.csv"));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load test fixtures from " + FIXTURES, e);
        }

        // Flush tables to enable Avro pushdown for SELECT queries during measurement
        // This matches GroupByTest/LikeTest pattern and ensures consistent measurement path
        database.getTable("USERS").saveToFile("USERS");
        database.getTable("PROFILES").saveToFile("PROFILES");
        database.getTable("TRANSACTIONS").saveToFile("TRANSACTIONS");
        database.getTable("USER_DETAILS").saveToFile("USER_DETAILS");

        LOGGER.log(Level.INFO, "Setup completed: {0} records per table (bulk-loaded)", n);
    }

    /* ------------------------------------------------------------------ */
    /*  Assertion / helper methods                                        */
    /* ------------------------------------------------------------------ */

    protected void check(boolean condition, String message) {
        if (condition) {
            passed++;
            LOGGER.log(Level.INFO, "PASS: {0}", message);
        } else {
            failed++;
            LOGGER.log(Level.SEVERE, "FAIL: {0}", message);
        }
    }

    protected boolean isErrorResponse(Object response) {
        return response instanceof String && ((String) response).startsWith("Error:");
    }

    protected void runSelectCount(String group, String name, String query, int expected) {
        try {
            Object result = database.executeQuery(query, null);
            if (result instanceof List) {
                int actual = ((List<?>) result).size();
                check(actual == expected, group + " / " + name + " returned " + actual + " rows, expected " + expected);
            } else {
                check(false, group + " / " + name + " did not return a result set");
            }
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        }
    }

    protected void runExec(String group, String name, String query) {
        try {
            database.executeQuery(query, null);
            check(true, group + " / " + name + " executed");
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        }
    }

    protected void checkAggregate(String group, String name, String query, String key, Object expected) {
        try {
            Object result = database.executeQuery(query, null);
            if (result instanceof List) {
                List<?> rows = (List<?>) result;
                if (rows.size() == 1 && rows.get(0) instanceof java.util.Map) {
                    Object value = ((java.util.Map<?, ?>) rows.get(0)).get(key);
                    check(expected.equals(value), group + " / " + name + " aggregate " + key + " = " + value + ", expected " + expected);
                } else {
                    check(false, group + " / " + name + " expected a single aggregate row, got " + rows.size() + " rows");
                }
            } else {
                check(false, group + " / " + name + " did not return a result set");
            }
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        }
    }

    protected void dropTable(String name) {
        try {
            database.dropTable(name);
        } catch (TableNotFoundException e) {
            LOGGER.log(Level.WARNING, "Table {0} not found for dropping", name);
        }
    }

    protected int countRows(String query) {
        try {
            Object result = database.executeQuery(query, null);
            if (result instanceof List) {
                return ((List<?>) result).size();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "countRows failed for {0}: {1}", new Object[]{query, e.getMessage()});
        }
        return -1;
    }

    @AfterAll
    void closeDatabase() {
        database.close();
    }
}
