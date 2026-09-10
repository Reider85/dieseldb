package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Heavy ORDER BY + JOIN tests that require 600-row tables (360 k row cross-product).
 * These are marked {@link LargeTest} and skipped in CI by default.
 * <p>
 * For small, CI-friendly tests see the separate query test classes
 * ({@code AdvancedQueryTest}, {@code JoinQueryTest}, etc.) which use 100 rows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuantitativeTest {
    private static final Logger LOGGER = Logger.getLogger(QuantitativeTest.class.getName());
    private static final int RECORD_COUNT = 600;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    private final Database database;
    private int passed = 0;
    private int failed = 0;

    public QuantitativeTest() {
        this.database = new Database();
    }

    @BeforeAll
    void setup() {
        dropTable("USERS");
        dropTable("PROFILES");

        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, DATE_FIELD DATE, ACTIVE BOOLEAN, PRECISION DOUBLE)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);

        database.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)", null);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            String date = DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)));
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, DATE_FIELD, ACTIVE, PRECISION) VALUES ('CODE%d', 'User%d', %d, %s, '%s', %s, %f)",
                    i, i, 18 + (i % 82), balance, date, (i % 2 == 0) ? "TRUE" : "FALSE", (i % 100) / 10.0), null);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED, PROFILE_DATE) VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d', '%s')",
                    i, 18 + (i % 82), i, i, i, date), null);
        }
        LOGGER.log(Level.INFO, "Heavy setup completed: {0} records in USERS/PROFILES", RECORD_COUNT);
    }

    @LargeTest
    @Order(1)
    void heavyOrderByJoinPrimaryKey() {
        runSelectCount("OrderByHeavyTest", "complex join order by primary key",
                "SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID", RECORD_COUNT);
    }

    @LargeTest
    @Order(2)
    void heavyOrderByJoinNonIndexed() {
        runSelectCount("OrderByHeavyTest", "complex join order by non indexed",
                "SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE", RECORD_COUNT);
    }

    /* ------------------------------------------------------------------ */
    /*  Helpers                                                           */
    /* ------------------------------------------------------------------ */

    private void check(boolean condition, String message) {
        if (condition) {
            passed++;
            LOGGER.log(Level.INFO, "PASS: {0}", message);
        } else {
            failed++;
            LOGGER.log(Level.SEVERE, "FAIL: {0}", message);
        }
    }

    private void runSelectCount(String group, String name, String query, int expected) {
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

    private void dropTable(String name) {
        try {
            database.dropTable(name);
        } catch (TableNotFoundException e) {
            LOGGER.log(Level.WARNING, "Table {0} not found for dropping", name);
        }
    }

    public static void main(String[] args) {
        QuantitativeTest test = new QuantitativeTest();
        test.setup();
        test.heavyOrderByJoinPrimaryKey();
        test.heavyOrderByJoinNonIndexed();
        LOGGER.log(Level.INFO, "QuantitativeTest results: {0} passed, {1} failed", new Object[]{test.passed, test.failed});
    }
}
