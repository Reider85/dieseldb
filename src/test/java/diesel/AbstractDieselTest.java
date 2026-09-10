package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeAll;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
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
    protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

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
        for (int i = 1; i <= n; i++) {
            String date = DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)));
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, DATE_FIELD, ACTIVE, PRECISION) VALUES ('CODE%d', 'User%d', %d, %s, '%s', %s, %f)",
                    i, i, 18 + (i % 82), balance, date, (i % 2 == 0) ? "TRUE" : "FALSE", (i % 100) / 10.0), null);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED, PROFILE_DATE) VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d', '%s')",
                    i, 18 + (i % 82), i, i, i, date), null);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO TRANSACTIONS (USER_ID, TRANS_DATE, AMOUNT) VALUES (%d, '%s', %s)",
                    i, date, new BigDecimal(50 + (i % 500)).setScale(2, RoundingMode.HALF_UP)), null);

            database.executeQuery(String.format(Locale.US,
                    "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO, BALANCE) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d', %s)",
                    i, i, i, 18 + (i % 82), i, balance), null);
        }
        LOGGER.log(Level.INFO, "Setup completed: {0} records per table", n);
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
}
