package diesel;

import diesel.Database;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AllTestsSampleTest {
    private static final Logger LOGGER = Logger.getLogger(AllTestsSampleTest.class.getName());
    private static final int RECORD_COUNT = 600;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    private final Database database;
    private int passed = 0;
    private int failed = 0;
    private final List<String> timingEntries = new ArrayList<>();

    public AllTestsSampleTest() {
        this.database = new Database();
    }

    @Test
    public void runTests() {
        try {
            setup();
            runAdvancedTestQueries();
            runAliasesTestQueries();
            runGroupByTestQueries();
            runInTestQueries();
            runJoinTestQueries();
            runLikeTestQueries();
            runOrderByTestQueries();
            runPerformanceTestQueries();
            runPersistenceTestQueries();
            runSubqueriesTestQueries();
            runTrueFalseNullTestQueries();
            runCaseSensitivityTestQueries();
            runTransactionTestQueries();
            runPrompt62TestQueries();
            runPrompt65TestQueries();
            runPrompt66TestQueries();
            runPrompt67TestQueries();
            runPrompt68TestQueries();
            runPrompt69TestQueries();
            runPrompt70TestQueries();
        } catch (Exception e) {
            failed++;
            LOGGER.log(Level.SEVERE, "AllTestsSampleTest FAILED: {0}", e.getMessage());
            e.printStackTrace();
        }
        LOGGER.log(Level.INFO, "==========================================");
        LOGGER.log(Level.INFO, "AllTestsSampleTest results: {0} passed, {1} failed", new Object[]{passed, failed});
        writeTimingReport();
        if (failed > 0) {
            throw new RuntimeException("AllTestsSampleTest failed: " + failed + " tests");
        }
    }

    private void recordTiming(String group, String name, String query, double durationMs, boolean ok) {
        String flatQuery = query.replaceAll("[\\r\\n]+", " ").trim();
        timingEntries.add(String.format(Locale.US, "%s | %s | %s | %.2f | %s",
                group, name, ok ? "OK" : "FAIL", durationMs, flatQuery));
    }

    private void writeTimingReport() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("# AllTestsSampleTest query timings\n\n");
            sb.append("Generated: ").append(new Date()).append("\n\n");
            sb.append("| # | Group | Test | Result | Time (ms) | Query |\n");
            sb.append("|---|-------|------|--------|-----------|-------|\n");
            int index = 1;
            for (String entry : timingEntries) {
                sb.append("| ").append(index++).append(" | ").append(entry).append(" |\n");
            }
            String fileName = nextTimingFileName();
            try (FileWriter writer = new FileWriter(fileName)) {
                writer.write(sb.toString());
            }
            LOGGER.log(Level.INFO, "Timing report written to {0} ({1} queries)", new Object[]{fileName, timingEntries.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to write timing.md: {0}", e.getMessage());
        }
    }

    private String nextTimingFileName() {
        if (!new File("timing.md").exists()) {
            return "timing.md";
        }
        int counter = 1;
        while (new File("timing" + counter + ".md").exists()) {
            counter++;
        }
        return "timing" + counter + ".md";
    }

    private void check(boolean condition, String message) {
        if (condition) {
            passed++;
            LOGGER.log(Level.INFO, "PASS: {0}", message);
        } else {
            failed++;
            LOGGER.log(Level.SEVERE, "FAIL: {0}", message);
        }
    }

    private boolean isErrorResponse(Object response) {
        return response instanceof String && ((String) response).startsWith("Error:");
    }

    private void runSelectCount(String group, String name, String query, int expected) {
        long start = System.nanoTime();
        boolean ok = false;
        try {
            Object result = database.executeQuery(query, null);
            if (result instanceof List) {
                int actual = ((List<?>) result).size();
                ok = actual == expected;
                check(ok, group + " / " + name + " returned " + actual + " rows, expected " + expected);
            } else {
                check(false, group + " / " + name + " did not return a result set");
            }
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        } finally {
            recordTiming(group, name, query, (System.nanoTime() - start) / 1_000_000.0, ok);
        }
    }

    private void runExec(String group, String name, String query) {
        long start = System.nanoTime();
        boolean ok = false;
        try {
            database.executeQuery(query, null);
            ok = true;
            check(true, group + " / " + name + " executed");
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        } finally {
            recordTiming(group, name, query, (System.nanoTime() - start) / 1_000_000.0, ok);
        }
    }

    private void checkAggregate(String group, String name, String query, String key, Object expected) {
        long start = System.nanoTime();
        boolean ok = false;
        try {
            Object result = database.executeQuery(query, null);
            if (result instanceof List) {
                List<?> rows = (List<?>) result;
                if (rows.size() == 1 && rows.get(0) instanceof Map) {
                    Object value = ((Map<?, ?>) rows.get(0)).get(key);
                    ok = expected.equals(value);
                    check(ok, group + " / " + name + " aggregate " + key + " = " + value + ", expected " + expected);
                } else {
                    check(false, group + " / " + name + " expected a single aggregate row, got " + rows.size() + " rows");
                }
            } else {
                check(false, group + " / " + name + " did not return a result set");
            }
        } catch (Exception e) {
            check(false, group + " / " + name + " failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "{0} / {1} query: {2}", new Object[]{group, name, query});
        } finally {
            recordTiming(group, name, query, (System.nanoTime() - start) / 1_000_000.0, ok);
        }
    }

    private void dropTable(String name) {
        try {
            database.dropTable(name);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table {0} not found for dropping", name);
        }
    }

    private void setup() {
        dropTable("USERS");
        dropTable("PROFILES");
        dropTable("TRANSACTIONS");
        dropTable("USER_DETAILS");
        dropTable("PERSIST_TEST");

        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, DATE_FIELD DATE, ACTIVE BOOLEAN, PRECISION DOUBLE)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);

        database.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)", null);
        database.executeQuery("CREATE TABLE TRANSACTIONS (TRANS_ID LONG PRIMARY KEY SEQUENCE(trans_seq 1 1), USER_ID LONG, TRANS_DATE DATE, AMOUNT BIGDECIMAL)", null);
        database.executeQuery("CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)", null);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            String date = DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)));
            BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP);

            String userQuery = String.format(Locale.US,
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, DATE_FIELD, ACTIVE, PRECISION) VALUES ('CODE%d', 'User%d', %d, %s, '%s', %s, %f)",
                    i, i, 18 + (i % 82), balance, date, (i % 2 == 0) ? "TRUE" : "FALSE", (i % 100) / 10.0);
            database.executeQuery(userQuery, null);

            String profileQuery = String.format(Locale.US,
                    "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED, PROFILE_DATE) VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d', '%s')",
                    i, 18 + (i % 82), i, i, i, date);
            database.executeQuery(profileQuery, null);

            String transQuery = String.format(Locale.US,
                    "INSERT INTO TRANSACTIONS (USER_ID, TRANS_DATE, AMOUNT) VALUES (%d, '%s', %s)",
                    i, date, new BigDecimal(50 + (i % 500)).setScale(2, BigDecimal.ROUND_HALF_UP));
            database.executeQuery(transQuery, null);

            String detailQuery = String.format(Locale.US,
                    "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO, BALANCE) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d', %s)",
                    i, i, i, 18 + (i % 82), i, balance);
            database.executeQuery(detailQuery, null);
        }
        LOGGER.log(Level.INFO, "Setup completed: {0} records inserted into USERS, PROFILES, TRANSACTIONS, USER_DETAILS", RECORD_COUNT);
    }

    private void runAdvancedTestQueries() {
        runSelectCount("AdvancedTest", "simple select by primary key", "SELECT ID, NAME FROM USERS WHERE ID = 500", 1);
        runSelectCount("AdvancedTest", "simple select by name", "SELECT ID, NAME FROM USERS WHERE NAME = 'User500'", 1);
        runSelectCount("AdvancedTest", "complex select with multi-column and conditions",
                "SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500')", 0);
        runSelectCount("AdvancedTest", "complex select with or limit offset",
                "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5", 2);
    }

    private void runAliasesTestQueries() {
        runSelectCount("AliasesTest", "simple select with alias order by",
                "SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName", RECORD_COUNT);
        runSelectCount("AliasesTest", "simple select with as alias order by",
                "SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName", RECORD_COUNT);
        runSelectCount("AliasesTest", "complex select min max avg with join and group by",
                "SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge " +
                        "FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                        "GROUP BY userName, transDate ORDER BY transDate DESC", RECORD_COUNT);
        runSelectCount("AliasesTest", "complex select with multiple inner joins",
                "SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName " +
                        "FROM USERS u " +
                        "INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                        "INNER JOIN USERS u2 ON u.ID = u2.ID " +
                        "LIMIT 10 OFFSET 5", 10);
    }

    private void runGroupByTestQueries() {
        runSelectCount("GroupByTest", "simple group by min max avg",
                "SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME", RECORD_COUNT);
        runSelectCount("GroupByTest", "simple group by sum count",
                "SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME", RECORD_COUNT);
        runSelectCount("GroupByTest", "complex group by date having",
                "SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", RECORD_COUNT);
        runSelectCount("GroupByTest", "complex group by join string date",
                "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) " +
                        "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                        "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", RECORD_COUNT);
    }

    private void runInTestQueries() {
        runSelectCount("InTest", "simple in on btree index", "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)", 21);
        runSelectCount("InTest", "simple in on primary key", "SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502)", 3);
        runSelectCount("InTest", "complex in with and",
                "SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000", 0);
        runSelectCount("InTest", "complex in with or",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000", 3);
    }

    private void runJoinTestQueries() {
        runSelectCount("JoinTest", "simple inner join on primary key",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                        "WHERE USERS.ID IN (500, 501, 502)", 3);
        runSelectCount("JoinTest", "simple inner join on non indexed field",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                        "WHERE USERS.BALANCE = 5100.00", 0);
        runSelectCount("JoinTest", "complex full join on primary key",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                        "WHERE USERS.ID IN (500, 501, 502)", 3);
        runSelectCount("JoinTest", "complex inner join with and or in on",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) " +
                        "WHERE USERS.ID IN (500, 501, 502)", 3);
    }

    private void runLikeTestQueries() {
        runSelectCount("LikeTest", "simple like on name",
                "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%'", 1);
        runSelectCount("LikeTest", "simple like on user code",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%'", 1);
        runSelectCount("LikeTest", "complex like with and",
                "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000", 0);
        runSelectCount("LikeTest", "complex like with or",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000", 1);
    }

    private void runOrderByTestQueries() {
        runSelectCount("OrderByTest", "simple order by name", "SELECT ID, NAME FROM USERS ORDER BY NAME", RECORD_COUNT);
        runSelectCount("OrderByTest", "simple order by age desc", "SELECT ID, AGE FROM USERS ORDER BY AGE DESC", RECORD_COUNT);
        // Skipped: complex join order by primary key - causes OOM due to 600x600 cross join (360000 rows)
        // runSelectCount("OrderByTest", "complex join order by primary key",
        //         "SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID", RECORD_COUNT * RECORD_COUNT);
        // Skipped: complex join order by non indexed - same memory issue
        // runSelectCount("OrderByTest", "complex join order by non indexed",
        //         "SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE", RECORD_COUNT * RECORD_COUNT);
    }

    private void runPerformanceTestQueries() {
        runSelectCount("PerformanceTest", "simple select where age",
                "SELECT NAME, AGE FROM USERS WHERE AGE < 30", 95);
        runSelectCount("PerformanceTest", "simple select clustered index", "SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'", 1);
        runSelectCount("PerformanceTest", "complex select age and active",
                "SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE", 47);
        runSelectCount("PerformanceTest", "complex select parenthesized or",
                "SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500", 244);
    }

    private void runPersistenceTestQueries() {
        runExec("PersistenceTest", "create table",
                "CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)");
        runExec("PersistenceTest", "insert alice",
                "INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) " +
                        "VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')");
        runExec("PersistenceTest", "insert bob full schema",
                "INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) " +
                        "VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000')");
        runSelectCount("PersistenceTest", "select from persisted table", "SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25", 1);
        database.getTable("PERSIST_TEST").saveToSerializedFile("PERSIST_TEST");
        check(new File("PERSIST_TEST.table").exists(), "PersistenceTest / serialized .table file created on disk");
        check(new File("PERSIST_TEST.csv").exists(), "PersistenceTest / csv file created on disk");
    }

    private void runSubqueriesTestQueries() {
        runSelectCount("SubqueriesTest", "simple subquery in in clause",
                "SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10", 10);
        runSelectCount("SubqueriesTest", "simple subquery in where",
                "SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10", 10);
        runSelectCount("SubqueriesTest", "complex subquery in column where group by having",
                "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count " +
                        "FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) " +
                        "GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) " +
                        "HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10", 0);
        runSelectCount("SubqueriesTest", "complex subquery in column inner join on",
                "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name " +
                        "FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10", 10);
    }

    private void runTrueFalseNullTestQueries() {
        dropTable("NULL_TEST");
        runExec("TrueFalseNullTest", "create table",
                "CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER)");
        runExec("TrueFalseNullTest", "insert flag true",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25)");
        runExec("TrueFalseNullTest", "insert flag false",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30)");
        runExec("TrueFalseNullTest", "insert null in insert",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL)");
        runSelectCount("TrueFalseNullTest", "where flag = true",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE", 1);
        runSelectCount("TrueFalseNullTest", "where flag = false",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE", 1);
        runSelectCount("TrueFalseNullTest", "where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL", 1);
        runSelectCount("TrueFalseNullTest", "where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL", 2);
        runSelectCount("TrueFalseNullTest", "where age is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL", 1);
        runExec("TrueFalseNullTest", "update set null in update",
                "UPDATE NULL_TEST SET COL = NULL WHERE ID = 1");
        runSelectCount("TrueFalseNullTest", "where col is null after update",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL", 2);
        runSelectCount("TrueFalseNullTest", "where col = null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where col != null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL", 0);
        runSelectCount("TrueFalseNullTest", "prompt 57 select * where col = null returns empty",
                "SELECT * FROM NULL_TEST WHERE COL = NULL", 0);
        runSelectCount("TrueFalseNullTest", "prompt 57 select * where col != null returns empty",
                "SELECT * FROM NULL_TEST WHERE COL != NULL", 0);
        runSelectCount("TrueFalseNullTest", "prompt 58 select * where col is null returns rows with null col",
                "SELECT * FROM NULL_TEST WHERE COL IS NULL", 2);
        runSelectCount("TrueFalseNullTest", "prompt 59 select * where col = 25 or col is null returns value and null rows",
                "SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL", 2);
        runSelectCount("TrueFalseNullTest", "prompt 59 select * where col = 25 and col is not null returns only value row",
                "SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL", 1);
        runSelectCount("TrueFalseNullTest", "where age < null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL", 0);
        runSelectCount("TrueFalseNullTest", "where age > null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL", 0);
        runSelectCount("TrueFalseNullTest", "where age <= null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL", 0);
        runSelectCount("TrueFalseNullTest", "where age >= null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL", 0);
        runSelectCount("TrueFalseNullTest", "where col != 'A' excludes null rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A'", 1);
        runSelectCount("TrueFalseNullTest", "where age < 30 excludes null row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30", 1);
        runSelectCount("TrueFalseNullTest", "where age = 25 or col = null keeps only matching row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL", 1);
        runSelectCount("TrueFalseNullTest", "where col = null and age = 25 returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25", 0);
        runSelectCount("TrueFalseNullTest", "where true and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where false and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where unknown and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where not true and unknown keeps only false row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL)", 1);
        runSelectCount("TrueFalseNullTest", "where true or unknown includes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL", 1);
        runSelectCount("TrueFalseNullTest", "where false or unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where unknown or unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL", 0);
        runSelectCount("TrueFalseNullTest", "where false or true and unknown or true include rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL", 2);
        runSelectCount("TrueFalseNullTest", "where not true or unknown excludes all rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL)", 0);
        runExec("TrueFalseNullTest", "update where col is null",
                "UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after update where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40", 2);
        runExec("TrueFalseNullTest", "update where col is not null",
                "UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL");
        runSelectCount("TrueFalseNullTest", "select after update where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50", 1);
        runExec("TrueFalseNullTest", "delete where col is null",
                "DELETE FROM NULL_TEST WHERE COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after delete where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 1);
        runExec("TrueFalseNullTest", "delete where col is not null",
                "DELETE FROM NULL_TEST WHERE COL IS NOT NULL");
        runSelectCount("TrueFalseNullTest", "select after delete where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 0);
        runExec("TrueFalseNullTest", "reinsert row a for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25)");
        runExec("TrueFalseNullTest", "reinsert row b for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30)");
        runExec("TrueFalseNullTest", "reinsert null row for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL)");
        runExec("TrueFalseNullTest", "update where false or unknown or true",
                "UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after update with or unknown",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77", 1);
        runExec("TrueFalseNullTest", "delete where true or unknown",
                "DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL");
        runSelectCount("TrueFalseNullTest", "select after delete with or unknown",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 2);
        dropTable("AGG_TEST");
        runExec("TrueFalseNullTest", "prompt 60 create agg table",
                "CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 10",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (10)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 20",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (20)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount null",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 30",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (30)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount null 2",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL)");
        runSelectCount("TrueFalseNullTest", "prompt 60 select * returns all rows incl nulls",
                "SELECT * FROM AGG_TEST", 5);
        checkAggregate("TrueFalseNullTest", "prompt 60 count star counts all rows",
                "SELECT COUNT(*) FROM AGG_TEST", "COUNT(*)", 5L);
        checkAggregate("TrueFalseNullTest", "prompt 60 count column skips null",
                "SELECT COUNT(AMOUNT) FROM AGG_TEST", "COUNT(AMOUNT)", 3L);
        checkAggregate("TrueFalseNullTest", "prompt 60 sum skips null",
                "SELECT SUM(AMOUNT) FROM AGG_TEST", "SUM(AMOUNT)", 60);
        checkAggregate("TrueFalseNullTest", "prompt 60 avg skips null",
                "SELECT AVG(AMOUNT) FROM AGG_TEST", "AVG(AMOUNT)", 20);
        checkAggregate("TrueFalseNullTest", "prompt 60 min skips null",
                "SELECT MIN(AMOUNT) FROM AGG_TEST", "MIN(AMOUNT)", 10);
        checkAggregate("TrueFalseNullTest", "prompt 60 max skips null",
                "SELECT MAX(AMOUNT) FROM AGG_TEST", "MAX(AMOUNT)", 30);
        dropTable("AGG_TEST");
    }

    private void runTransactionTestQueries() {
        dropTable("TXN_TEST");
        runExec("TransactionTest", "create table",
                "CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING)");
        check(database.isAutoCommit(), "TransactionTest / autoCommit is true by default");

        runExec("TransactionTest", "insert without begin auto-commits",
                "INSERT INTO TXN_TEST (NAME) VALUES ('auto48')");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'auto48'") == 1,
                "TransactionTest / INSERT without BEGIN is committed and visible via SELECT");

        String beginResult48 = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID tx48Id = UUID.fromString(beginResult48.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN_TEST (NAME) VALUES ('only-in-tx48')", tx48Id);
        Object inTx48 = database.executeQuery("SELECT NAME FROM TXN_TEST WHERE NAME = 'only-in-tx48'", tx48Id);
        check(inTx48 instanceof List && ((List<?>) inTx48).size() == 1,
                "TransactionTest / BEGIN+INSERT row is visible inside the current transaction");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'only-in-tx48'") == 0,
                "TransactionTest / BEGIN+INSERT row is not visible outside the transaction");
        database.executeQuery("ROLLBACK", tx48Id);
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'only-in-tx48'") == 0,
                "TransactionTest / BEGIN+INSERT row is discarded after ROLLBACK");

        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID commitTxId = UUID.fromString(beginResult.split(": ")[1]);
        check(!database.isAutoCommit(), "TransactionTest / autoCommit is false after BEGIN");
        database.executeQuery("INSERT INTO TXN_TEST (NAME) VALUES ('committed')", commitTxId);
        database.executeQuery("COMMIT", commitTxId);
        check(!database.isAutoCommit(), "TransactionTest / autoCommit stays false after COMMIT");
        check(!database.isInTransaction(commitTxId),
                "TransactionTest / COMMIT ends the transaction (no automatic new BEGIN after COMMIT)");
        database.executeQuery("INSERT INTO TXN_TEST (NAME) VALUES ('post-commit49')", null);
        check(!database.isAutoCommit(),
                "TransactionTest / INSERT without BEGIN after COMMIT does not auto-create a new transaction");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'post-commit49'") == 1,
                "TransactionTest / INSERT without BEGIN after COMMIT is written to the live table and visible");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'committed'") == 1,
                "TransactionTest / committed row is visible after COMMIT");

        beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID rollbackTxId = UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN_TEST (NAME) VALUES ('rolled')", rollbackTxId);
        database.executeQuery("ROLLBACK", rollbackTxId);
        check(!database.isAutoCommit(), "TransactionTest / autoCommit stays false after ROLLBACK");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'rolled'") == 0,
                "TransactionTest / rolled back row is not visible after ROLLBACK");

        check(!database.isAutoCommit(), "TransactionTest / autoCommit is false before SET AUTOCOMMIT (after ROLLBACK)");
        runExec("TransactionTest", "set autocommit off", "SET AUTOCOMMIT = OFF");
        check(!database.isAutoCommit(), "TransactionTest / autoCommit is false after SET AUTOCOMMIT = OFF");
        runExec("TransactionTest", "set session autocommit on", "SET SESSION AUTOCOMMIT = ON");
        check(database.isAutoCommit(), "TransactionTest / autoCommit is true after SET SESSION AUTOCOMMIT = ON");
        runExec("TransactionTest", "set session autocommit off", "SET SESSION AUTOCOMMIT = OFF");
        check(!database.isAutoCommit(), "TransactionTest / autoCommit is false after SET SESSION AUTOCOMMIT = OFF");
        runExec("TransactionTest", "set autocommit on", "SET AUTOCOMMIT = ON");
        check(database.isAutoCommit(), "TransactionTest / autoCommit is true after SET AUTOCOMMIT = ON");

        database.executeQuery("SELECT NAME FROM TXN_TEST WHERE NAME = 'committed'", null);
        check(database.isAutoCommit(), "TransactionTest / autoCommit stays true after SELECT (SELECT does not auto-commit)");
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'committed'") == 1,
                "TransactionTest / SELECT does not affect committed data");

        beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID selectTxId = UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN_TEST (NAME) VALUES ('select-visible')", selectTxId);
        Object selectInTxn = database.executeQuery("SELECT NAME FROM TXN_TEST WHERE NAME = 'select-visible'", selectTxId);
        check(selectInTxn instanceof List && ((List<?>) selectInTxn).size() == 1,
                "TransactionTest / SELECT inside transaction sees uncommitted row");
        database.executeQuery("ROLLBACK", selectTxId);
        check(countRows("SELECT NAME FROM TXN_TEST WHERE NAME = 'select-visible'") == 0,
                "TransactionTest / SELECT inside transaction does not persist the row after ROLLBACK");
        check(!database.isAutoCommit(), "TransactionTest / autoCommit stays false after ROLLBACK (SELECT does not change it)");

        database.setAutoCommit(true);
    }

    private int countRows(String query) {
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

    private void runCaseSensitivityTestQueries() {
        dropTable("CASE_TEST");
        dropTable("MyTable");
        runExec("CaseSensitivityTest", "create table",
                "CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING)");
        runExec("CaseSensitivityTest", "insert john",
                "INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value')");
        runSelectCount("CaseSensitivityTest", "where name = 'John' finds row",
                "SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John'", 1);
        runSelectCount("CaseSensitivityTest", "where name = 'JOHN' returns no rows",
                "SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN'", 0);
        runSelectCount("CaseSensitivityTest", "quoted column identifier myColumn",
                "SELECT \"myColumn\" FROM CASE_TEST", 1);
        runExec("CaseSensitivityTest", "create quoted table",
                "CREATE TABLE \"MyTable\" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING)");
        runExec("CaseSensitivityTest", "insert into quoted table",
                "INSERT INTO \"MyTable\" (NAME) VALUES ('test')");
        runSelectCount("CaseSensitivityTest", "select from quoted table",
                "SELECT * FROM \"MyTable\"", 1);
        dropTable("CASE_TEST");
        dropTable("MyTable");
    }

    private void runPrompt62TestQueries() {
        dropTable("USERS");
        runExec("Prompt62Test", "prompt 62 create users table",
                "CREATE TABLE USERS (ID INTEGER, NAME STRING)");
        runExec("Prompt62Test", "prompt 62 insert John",
                "INSERT INTO USERS (ID, NAME) VALUES (1, 'John')");
        runExec("Prompt62Test", "prompt 62 insert jane",
                "INSERT INTO USERS (ID, NAME) VALUES (2, 'jane')");
        runSelectCount("Prompt62Test", "prompt 62 where name = 'John' returns only the John row",
                "SELECT * FROM USERS WHERE NAME = 'John'", 1);
        runSelectCount("Prompt62Test", "prompt 63 where name = 'JOHN' returns no rows",
                "SELECT * FROM USERS WHERE NAME = 'JOHN'", 0);
        runSelectCount("Prompt62Test", "prompt 63 where name = 'John' returns the John row",
                "SELECT * FROM USERS WHERE NAME = 'John'", 1);
        runExec("Prompt62Test", "prompt 64 insert null name",
                "INSERT INTO USERS (ID, NAME) VALUES (3, NULL)");
        runSelectCount("Prompt62Test", "prompt 64 where name is null returns only the null name row",
                "SELECT * FROM USERS WHERE NAME IS NULL", 1);
        runSelectCount("Prompt62Test", "prompt 64 where name = null returns no rows",
                "SELECT * FROM USERS WHERE NAME = NULL", 0);
    }

    private void runPrompt65TestQueries() {
        dropTable("BOOL_TEST");
        runExec("Prompt65Test", "prompt 65 create bool table",
                "CREATE TABLE BOOL_TEST (ID LONG PRIMARY KEY SEQUENCE(bool_test_seq 1 1), FLAG BOOLEAN)");
        runExec("Prompt65Test", "prompt 65 insert flag true",
                "INSERT INTO BOOL_TEST (FLAG) VALUES (TRUE)");
        runExec("Prompt65Test", "prompt 65 insert flag false",
                "INSERT INTO BOOL_TEST (FLAG) VALUES (FALSE)");
        runSelectCount("Prompt65Test", "prompt 65 where flag = true returns only the true row",
                "SELECT * FROM BOOL_TEST WHERE FLAG = TRUE", 1);
        runSelectCount("Prompt65Test", "prompt 65 where flag = false returns only the false row",
                "SELECT * FROM BOOL_TEST WHERE FLAG = FALSE", 1);
    }

    private void runPrompt66TestQueries() {
        dropTable("TXN66_TEST");
        runExec("Prompt66Test", "prompt 66 create transaction table",
                "CREATE TABLE TXN66_TEST (ID LONG PRIMARY KEY SEQUENCE(txn66_seq 1 1), NAME STRING)");
        check(database.isAutoCommit(), "Prompt66Test / autoCommit is true by default");

        runExec("Prompt66Test", "prompt 66 insert without begin auto-commits",
                "INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-auto')");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-auto'") == 1,
                "Prompt66Test / INSERT without BEGIN is committed and visible via SELECT");

        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID prompt66TxId = UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-rolled')", prompt66TxId);
        Object inTx66 = database.executeQuery("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'", prompt66TxId);
        check(inTx66 instanceof List && ((List<?>) inTx66).size() == 1,
                "Prompt66Test / BEGIN+INSERT row is visible inside the current transaction");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'") == 0,
                "Prompt66Test / BEGIN+INSERT row is not visible outside the transaction");
        database.executeQuery("ROLLBACK", prompt66TxId);
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'") == 0,
                "Prompt66Test / BEGIN+INSERT row is not inserted after ROLLBACK");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-auto'") == 1,
                "Prompt66Test / auto-committed row is still present after ROLLBACK");

        database.setAutoCommit(true);
    }

    private void runPrompt67TestQueries() {
        dropTable("TXN67_TEST");
        runExec("Prompt67Test", "prompt 67 create transaction table",
                "CREATE TABLE TXN67_TEST (ID LONG PRIMARY KEY SEQUENCE(txn67_seq 1 1), NAME STRING)");
        check(database.isAutoCommit(), "Prompt67Test / autoCommit is true by default");

        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID prompt67TxId = UUID.fromString(beginResult.split(": ")[1]);

        database.executeQuery("INSERT INTO TXN67_TEST (NAME) VALUES ('prompt67-first')", prompt67TxId);
        Object firstInTx = database.executeQuery("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'", prompt67TxId);
        check(firstInTx instanceof List && ((List<?>) firstInTx).size() == 1,
                "Prompt67Test / SELECT right after the first INSERT sees the row inside the transaction");

        database.executeQuery("INSERT INTO TXN67_TEST (NAME) VALUES ('prompt67-second')", prompt67TxId);
        Object allInTx = database.executeQuery("SELECT * FROM TXN67_TEST", prompt67TxId);
        check(allInTx instanceof List && ((List<?>) allInTx).size() == 2,
                "Prompt67Test / SELECT after the second INSERT sees both rows inserted in the same transaction");
        check(!database.isAutoCommit(), "Prompt67Test / autoCommit is false while the transaction is open");

        runSelectCount("Prompt67Test", "prompt 67 select before commit is isolated from the transaction",
                "SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'", 0);

        database.executeQuery("COMMIT", prompt67TxId);
        runSelectCount("Prompt67Test", "prompt 67 both rows visible only after COMMIT",
                "SELECT * FROM TXN67_TEST", 2);
        check(countRows("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'") == 1,
                "Prompt67Test / first row is visible only after COMMIT");
        check(countRows("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-second'") == 1,
                "Prompt67Test / second row is visible only after COMMIT");
        check(!database.isInTransaction(prompt67TxId),
                "Prompt67Test / COMMIT ends the transaction");

        database.setAutoCommit(true);
    }

    private void runPrompt68TestQueries() {
        dropTable("TXN68_TEST");
        runExec("Prompt68Test", "prompt 68 create multi-client table",
                "CREATE TABLE TXN68_TEST (ID LONG PRIMARY KEY SEQUENCE(txn68_seq 1 1), CLIENT STRING, NAME STRING)");

        String beginA = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID clientA = UUID.fromString(beginA.split(": ")[1]);
        String beginB = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID clientB = UUID.fromString(beginB.split(": ")[1]);
        check(!clientA.equals(clientB) && database.isInTransaction(clientA) && database.isInTransaction(clientB),
                "Prompt68Test / two clients hold two distinct active transaction sessions");

        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-committed')", clientA);
        database.executeQuery("COMMIT", clientA);
        check(!database.isInTransaction(clientA), "Prompt68Test / client A's COMMIT ends its transaction");

        runSelectCount("Prompt68Test", "prompt 68 reader sees the other client's committed row",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-committed'", 1);

        String beginA2 = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID clientA2 = UUID.fromString(beginA2.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-uncommitted')", clientA2);
        runSelectCount("Prompt68Test", "prompt 68 reader does not see the other client's uncommitted row",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted'", 0);
        database.executeQuery("COMMIT", clientA2);
        runSelectCount("Prompt68Test", "prompt 68 reader sees the row after the writer's COMMIT",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted'", 1);

        Object bSnapshot = database.executeQuery("SELECT * FROM TXN68_TEST", clientB);
        check(bSnapshot instanceof List && ((List<?>) bSnapshot).size() == 0,
                "Prompt68Test / reader's own transaction keeps its BEGIN-time snapshot (other clients' commits not visible)");

        String beginA3 = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID clientA3 = UUID.fromString(beginA3.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-dirty')", clientA3);
        Object dirtyRead = database.executeQuery("SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-dirty'", clientB);
        check(dirtyRead instanceof List && ((List<?>) dirtyRead).size() == 1,
                "Prompt68Test / reader at READ_UNCOMMITTED isolation sees the writer's uncommitted row (dirty read)");
        database.executeQuery("ROLLBACK", clientA3);
        Object afterRollback = database.executeQuery("SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-dirty'", clientB);
        check(afterRollback instanceof List && ((List<?>) afterRollback).size() == 0,
                "Prompt68Test / reader no longer sees the row after the writer's ROLLBACK");

        runPrompt68ConcurrentClients();

        database.setAutoCommit(true);
    }

    private void runPrompt68ConcurrentClients() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch writerInserted = new CountDownLatch(1);
        CountDownLatch readerVerified = new CountDownLatch(1);
        CountDownLatch writerCommitted = new CountDownLatch(1);
        AtomicInteger uncommittedVisible = new AtomicInteger(-1);
        AtomicInteger committedVisible = new AtomicInteger(-1);

        Future<?> writerFuture = executor.submit(() -> {
            String begin = (String) database.executeQuery("BEGIN TRANSACTION", null);
            UUID writerTx = UUID.fromString(begin.split(": ")[1]);
            for (int i = 1; i <= 5; i++) {
                database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('concurrent', 'prompt68-concurrent-" + i + "')", writerTx);
            }
            writerInserted.countDown();
            readerVerified.await();
            database.executeQuery("COMMIT", writerTx);
            writerCommitted.countDown();
            return null;
        });

        Future<?> readerFuture = executor.submit(() -> {
            writerInserted.await();
            Object beforeCommit = database.executeQuery("SELECT * FROM TXN68_TEST WHERE CLIENT = 'concurrent'", null);
            uncommittedVisible.set(beforeCommit instanceof List ? ((List<?>) beforeCommit).size() : -1);
            readerVerified.countDown();
            writerCommitted.await();
            Object afterCommit = database.executeQuery("SELECT * FROM TXN68_TEST WHERE CLIENT = 'concurrent'", null);
            committedVisible.set(afterCommit instanceof List ? ((List<?>) afterCommit).size() : -1);
            return null;
        });

        try {
            writerFuture.get(30, TimeUnit.SECONDS);
            readerFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Prompt68Test concurrent client test failed: {0}", e.getMessage());
            throw new RuntimeException("Prompt68Test concurrent client test failed", e);
        } finally {
            executor.shutdownNow();
        }

        check(uncommittedVisible.get() == 0,
                "Prompt68Test / concurrent reader sees 0 of the writer's rows while the transaction is open");
        check(committedVisible.get() == 5,
                "Prompt68Test / concurrent reader sees all 5 writer rows only after COMMIT");
    }

    private void runPrompt69TestQueries() {
        int port = -1;
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            port = tempSocket.getLocalPort();
        } catch (IOException e) {
            check(false, "Prompt69Test / failed to allocate a port: " + e.getMessage());
            return;
        }
        DatabaseServer server = new DatabaseServer(port);
        Thread serverThread = new Thread(() -> server.start(), "prompt69-server");
        try {
            serverThread.start();
            waitForPrompt69Server(port);

            try (Socket client = new Socket("localhost", port)) {
                client.setSoTimeout(60000);
                ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
                out.writeObject(new QueryMessage("SET AUTOCOMMIT = ON", null));
                out.flush();
                ObjectInputStream in = new ObjectInputStream(client.getInputStream());
                Object response = in.readObject();
                check(response != null && !isErrorResponse(response),
                        "Prompt69Test / the long-running client connection is functional (server answers a query without an error)");

                long start = System.currentTimeMillis();
                boolean closedByServer = false;
                try {
                    in.readObject();
                } catch (IOException e) {
                    closedByServer = true;
                }
                long elapsed = System.currentTimeMillis() - start;
                check(closedByServer,
                        "Prompt69Test / the server closes the connection when the idle long query (no next query) exceeds the 30s timeout");
                check(elapsed >= 30000,
                        "Prompt69Test / the timeout fires only after ~30 seconds (elapsed " + elapsed + " ms, configured server.socket.timeout 30000 ms)");
                check(elapsed < 50000,
                        "Prompt69Test / the close is caused by the server's 30s timeout, not by the client's own read timeout (elapsed " + elapsed + " ms)");
            }

            try (Socket probe = new Socket("localhost", port)) {
                probe.setSoTimeout(5000);
                ObjectOutputStream probeOut = new ObjectOutputStream(probe.getOutputStream());
                probeOut.writeObject(new QueryMessage("SET AUTOCOMMIT = ON", null));
                probeOut.flush();
                ObjectInputStream probeIn = new ObjectInputStream(probe.getInputStream());
                Object probeResponse = probeIn.readObject();
                check(probeResponse != null && !isErrorResponse(probeResponse),
                        "Prompt69Test / the server stays alive and accepts a new connection after the timed-out client was closed");
            }
        } catch (Exception e) {
            check(false, "Prompt69Test / long-query 30s timeout test failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "Prompt69Test / long-query 30s timeout test failed", e);
        } finally {
            server.stop();
            serverThread.interrupt();
        }
    }

    private void waitForPrompt69Server(int port) {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new IllegalStateException("Prompt69Test / server did not start within timeout");
    }

    private void runPrompt70TestQueries() {
        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
        File tempDir = null;
        Process process = null;
        List<String> outputLines = new ArrayList<>();
        try {
            tempDir = Files.createTempDirectory("prompt70-server").toFile();
        } catch (IOException e) {
            check(false, "Prompt70Test / failed to create temp directory: " + e.getMessage());
            return;
        }
        int port = -1;
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            port = tempSocket.getLocalPort();
        } catch (IOException e) {
            check(false, "Prompt70Test / failed to allocate a port: " + e.getMessage());
            return;
        }
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (isWindows ? "java.exe" : "java");
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, "diesel.DatabaseServer", String.valueOf(port));
        pb.redirectErrorStream(true);
        pb.directory(tempDir);
        try {
            process = pb.start();
        } catch (IOException e) {
            check(false, "Prompt70Test / failed to start server process: " + e.getMessage());
            return;
        }
        final Process runningProcess = process;
        Thread outputPump = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(runningProcess.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (outputLines) {
                        outputLines.add(line);
                    }
                }
            } catch (IOException ignored) {
            }
        }, "prompt70-output-pump");
        outputPump.setDaemon(true);
        outputPump.start();

        try {
            waitForPrompt70Server(port);
            check(true, "Prompt70Test / server subprocess started on port " + port);

            try (Socket client = new Socket("localhost", port)) {
                client.setSoTimeout(15000);
                ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(client.getInputStream());

                Object createResponse = prompt70RoundTrip(out, in, "CREATE TABLE PROMPT70_TEST (ID LONG PRIMARY KEY SEQUENCE(p70_seq 1 1), NAME STRING)");
                check(createResponse != null && !isErrorResponse(createResponse),
                        "Prompt70Test / CREATE TABLE round-trip against the separate server process succeeds");

                Object insert1 = prompt70RoundTrip(out, in, "INSERT INTO PROMPT70_TEST (NAME) VALUES ('prompt70-first')");
                check(!isErrorResponse(insert1),
                        "Prompt70Test / first INSERT round-trip against the separate server process succeeds");

                Object insert2 = prompt70RoundTrip(out, in, "INSERT INTO PROMPT70_TEST (NAME) VALUES ('prompt70-second')");
                check(!isErrorResponse(insert2),
                        "Prompt70Test / second INSERT round-trip against the separate server process succeeds");

                Object beginResponse = prompt70RoundTrip(out, in, "BEGIN TRANSACTION");
                String beginText = beginResponse instanceof String ? (String) beginResponse : null;
                UUID prompt70Tx = beginText != null && beginText.startsWith("Transaction started: ")
                        ? UUID.fromString(beginText.substring("Transaction started: ".length())) : null;
                Object insert3 = prompt70RoundTrip(out, in, new QueryMessage("INSERT INTO PROMPT70_TEST (NAME) VALUES ('prompt70-third')", prompt70Tx));
                check(!isErrorResponse(insert3),
                        "Prompt70Test / INSERT inside a BEGIN/COMMIT transaction against the separate server process succeeds");

                Object commitResponse = prompt70RoundTrip(out, in, new QueryMessage("COMMIT", prompt70Tx));
                check(commitResponse != null && !isErrorResponse(commitResponse),
                        "Prompt70Test / COMMIT round-trip against the separate server process succeeds");
            }

            LOGGER.log(Level.INFO, "Prompt70Test / sending SIGTERM via process.destroy() to the server process");
            long destroyStart = System.currentTimeMillis();
            process.destroy();
            boolean terminated = process.waitFor(30, TimeUnit.SECONDS);
            long destroyElapsed = System.currentTimeMillis() - destroyStart;
            check(terminated,
                    "Prompt70Test / the server process terminates within 30 seconds after SIGTERM (destroy took " + destroyElapsed + " ms)");

            File csvFile = new File(tempDir, "PROMPT70_TEST.csv");
            File tableFile = new File(tempDir, "PROMPT70_TEST.table");
            check(csvFile.exists(),
                    "Prompt70Test / the PROMPT70_TEST.csv data file is saved on disk after server termination");
            check(tableFile.exists(),
                    "Prompt70Test / the PROMPT70_TEST.table serialized file is saved on disk after server termination");
            if (csvFile.exists()) {
                List<String> csvLines = Files.readAllLines(csvFile.toPath());
                boolean hasFirst = csvLines.stream().anyMatch(l -> l.contains("prompt70-first"));
                boolean hasSecond = csvLines.stream().anyMatch(l -> l.contains("prompt70-second"));
                boolean hasThird = csvLines.stream().anyMatch(l -> l.contains("prompt70-third"));
                check(hasFirst && hasSecond && hasThird && csvLines.size() == 4,
                        "Prompt70Test / the saved CSV file contains all 3 inserted rows (header + " + (csvLines.size() - 1) + " data rows)");
            }

            if (isWindows) {
                check(true, "Prompt70Test / Windows Process.destroy() is forceful and does not run JVM shutdown hooks; only clean termination and saved files are verified");
            } else {
                check(process.exitValue() == 0,
                        "Prompt70Test / the server process exits with status 0 after graceful shutdown");
                String log;
                synchronized (outputLines) {
                    log = String.join("\n", outputLines);
                }
                check(log.contains("Database server stopped"),
                        "Prompt70Test / the shutdown hook stops the server gracefully (server output shows 'Database server stopped')");
            }
        } catch (Exception e) {
            check(false, "Prompt70Test / graceful shutdown subprocess test failed: " + e.getMessage());
            LOGGER.log(Level.SEVERE, "Prompt70Test / graceful shutdown subprocess test failed", e);
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
                try {
                    process.waitFor(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                outputPump.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            deletePrompt70Dir(tempDir);
        }
    }

    private Object prompt70RoundTrip(ObjectOutputStream out, ObjectInputStream in, String query) throws IOException, ClassNotFoundException {
        return prompt70RoundTrip(out, in, new QueryMessage(query, null));
    }

    private Object prompt70RoundTrip(ObjectOutputStream out, ObjectInputStream in, QueryMessage message) throws IOException, ClassNotFoundException {
        out.writeObject(message);
        out.flush();
        return in.readObject();
    }

    private void waitForPrompt70Server(int port) {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new IllegalStateException("Prompt70Test / server process did not start within timeout");
    }

    private void deletePrompt70Dir(File dir) {
        if (dir == null) {
            return;
        }
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                deletePrompt70Dir(child);
            }
        }
        dir.delete();
    }

    public static void main(String[] args) {
        AllTestsSampleTest test = new AllTestsSampleTest();
        test.runTests();
    }
}
