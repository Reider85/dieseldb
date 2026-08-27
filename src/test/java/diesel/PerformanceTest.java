package diesel;

import diesel.Database;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Locale;
import java.util.concurrent.*;
import java.io.*;

public class PerformanceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTest.class);
    private static final int RECORD_COUNT = 10;
    private static final int WARMUP_RUNS = 1;
    private static final int TEST_RUNS = 10;
    private static final long TRUE_CONDITION_WARNING_THRESHOLD_MS = 500;
    private static final Map<String, Integer> EXPECTED_QUERY_ROW_COUNTS = new HashMap<>();
    private static final String BENCHMARK_REPORT_FILE = "benchmark_report.md";

    static {
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = 25", 1);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE > 75.0", 0);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE", 5);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 40 OR LEVEL > 50", 0);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE = 30", 10);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500", 5);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE < 40 OR NOT ACTIVE = FALSE) AND RANK < 5", 4);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'", 0);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' AND AGE = 25", 0);
        EXPECTED_QUERY_ROW_COUNTS.put("SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE", 5);
    }

    private final Database database;

    public PerformanceTest() {
        this.database = new Database();
    }

    private void assertQueryRowCount(String query, Object result) {
        assertEquals(EXPECTED_QUERY_ROW_COUNTS.get(query).intValue(), ((List<?>) result).size(),
                "Unexpected row count for query: " + query);
    }

    @Test
    public void runTests() {
        initializeBenchmarkReport();
        runInsertPerformanceTest();
        setupTable();
        runUpdatePerformanceTest();
        runTransactionPerformanceTest();
        runReadUncommittedPerformanceTest();
        runTrueConditionPerformanceTest();
        List<String> queries = prepareQueries();
        for (String query : queries) {
            runPerformanceTest(query);
        }
        LOGGER.info("Benchmark report written to {}", BENCHMARK_REPORT_FILE);
    }

    private void initializeBenchmarkReport() {
        try (FileWriter fw = new FileWriter(BENCHMARK_REPORT_FILE);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println("# DieselDB Benchmark Report");
            out.println();
            out.println("| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |");
            out.println("|----------------------|----------------------------------------------|------------|------------|------------|-------------|");
        } catch (IOException e) {
            LOGGER.error("Failed to initialize benchmark report: {}", e.getMessage(), e);
        }
    }

    private void setupTable() {
        dropTable(); // Ensure table does not exist
        String createTableQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
        LOGGER.info("Executing CREATE TABLE query in setupTable: {}", createTableQuery);
        database.executeQuery(createTableQuery, null);

        // Create clustered index on USER_CODE
        String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
        LOGGER.info("Executing: {}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.info("Unique clustered index created on USER_CODE");

        insertRecords(RECORD_COUNT);
        Object verify = database.executeQuery("SELECT NAME FROM USERS", null);
        assertEquals(RECORD_COUNT, ((List<?>) verify).size(), "Expected " + RECORD_COUNT + " rows after setup insert");
        LOGGER.info("Setup completed: {} records inserted into USERS table", RECORD_COUNT);
    }

    private void insertRecords(int count) {
        Random random = new Random();
        List<String> columns = Arrays.asList("ID", "USER_CODE", "NAME", "AGE", "ACTIVE", "BIRTHDATE", "LASTLOGIN", "LASTACTION", "USERSCORE", "LEVEL", "RANK", "BALANCE", "SCORE", "PRECISION", "INITIAL", "SESSION_ID");
        String tableName = "USERS";
        Table table = database.getTable(tableName);

        for (int i = 1; i <= count; i++) {
            List<Object> values = generateRecordValues(i, random);
            InsertQuery insertQuery = new InsertQuery(columns, values);
            insertQuery.execute(table);
            table.saveToFile(tableName); // Save after each insert
        }
    }

    private List<Object> generateRecordValues(int index, Random random) {
        List<Object> values = new ArrayList<>();
        String id = String.valueOf(index);
        String userCode = "CODE" + index; // Ensure unique USER_CODE for clustered index
        String name = "User" + index;
        int age = 18 + (index % 52);
        boolean active = (index % 2) == 0;
        LocalDate birthdate = LocalDate.of(1955 + (index % 50), 1 + (index % 12), 1 + (index % 28));
        LocalDateTime lastLogin = LocalDateTime.of(2023, 10, 1 + (index % 30), index % 24, index % 60, index % 60);
        LocalDateTime lastAction = lastLogin.plusSeconds(index % 3600).plusNanos(index % 999000000);
        long userScore = (index % 1000000000L);
        short level = (short) (1 + (index % 100));
        byte rank = (byte) (1 + (index % 10));
        java.math.BigDecimal balance = new java.math.BigDecimal(100 + (index % 1000)).setScale(2, java.math.RoundingMode.HALF_UP);
        float score = 50 + (index % 50);
        double precision = 1000 + (index % 100000);
        char initial = (char) ('A' + (index % 26));
        UUID sessionId = new UUID((long) index, (long) index);

        values.add(id);
        values.add(userCode);
        values.add(name);
        values.add(age);
        values.add(active);
        values.add(birthdate);
        values.add(lastLogin);
        values.add(lastAction);
        values.add(userScore);
        values.add(level);
        values.add(rank);
        values.add(balance);
        values.add(score);
        values.add(precision);
        values.add(initial);
        values.add(sessionId);

        return values;
    }

    private void runInsertPerformanceTest() {
        try {
            LOGGER.info("Starting INSERT performance test for {} records", RECORD_COUNT);

            for (int i = 0; i < WARMUP_RUNS; i++) {
                LOGGER.info("Warmup run {}", i);
                dropTable();
                String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
                LOGGER.info("Executing CREATE TABLE query in warmup: {}", createQuery);
                database.executeQuery(createQuery, null);
                String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
                database.executeQuery(createIndexQuery, null);
                insertRecords(RECORD_COUNT);
            }

            List<Long> executionTimes = new ArrayList<>();
            for (int i = 0; i < TEST_RUNS; i++) {
                LOGGER.info("Test run {}", i);
                dropTable();
                String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
                LOGGER.info("Executing CREATE TABLE query in test run: {}", createQuery);
                database.executeQuery(createQuery, null);
                String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
                database.executeQuery(createIndexQuery, null);
                long startTime = System.nanoTime();
                insertRecords(RECORD_COUNT);
                long endTime = System.nanoTime();
                executionTimes.add(endTime - startTime);
            }

            double averageTimeMs = executionTimes.stream()
                    .mapToLong(Long::longValue)
                    .average()
                    .orElse(0.0) / 1_000_000.0;
            long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
            long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
            double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

            LOGGER.info("INSERT performance for {} records", RECORD_COUNT);
            LOGGER.info("Average execution time: {} ms", String.format("%.3f", averageTimeMs));
            LOGGER.info("Min execution time: {} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
            LOGGER.info("Max execution time: {} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
            LOGGER.info("Standard deviation: {} ms", String.format("%.3f", stdDevMs));
            LOGGER.info("--------------------------------");
            writeBenchmarkResult("INSERT", RECORD_COUNT, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);
        } catch (Exception e) {
            LOGGER.error("Error in INSERT performance test: {}", e.getMessage(), e);
        }
    }

    private void runUpdatePerformanceTest() {
        LOGGER.info("Testing UPDATE performance for {} records", RECORD_COUNT);

        Random random = new Random();

        for (int i = 0; i < WARMUP_RUNS; i++) {
            resetScoreColumn();
            performUpdateRun(random);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.info("Test run {}", i);
            resetScoreColumn();
            long startTime = System.nanoTime();
            performUpdateRun(random);
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        Object verifyUpdate = database.executeQuery("SELECT NAME FROM USERS WHERE SCORE > 50", null);
        assertEquals(RECORD_COUNT, ((List<?>) verifyUpdate).size(), "Expected all records to have an updated SCORE");

        LOGGER.info("UPDATE performance for {} records", RECORD_COUNT);
        LOGGER.info("Average execution time: {} ms", String.format("%.3f", averageTimeMs));
        LOGGER.info("Min execution time: {} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.info("Max execution time: {} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.info("Standard deviation: {} ms", String.format("%.3f", stdDevMs));
        LOGGER.info("--------------------------------");
        writeBenchmarkResult("UPDATE", RECORD_COUNT, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);
    }

    private void runTransactionPerformanceTest() {
        LOGGER.info("Testing TRANSACTION performance for {} records", RECORD_COUNT);

        Random random = new Random();

        for (int i = 0; i < WARMUP_RUNS; i++) {
            LOGGER.info("Warmup run {}", i);
            dropTable();
            UUID txId = database.beginTransaction(null);
            String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
            LOGGER.info("Executing CREATE TABLE query in transaction warmup: {}", createQuery);
            database.executeQuery(createQuery, txId);
            String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
            database.executeQuery(createIndexQuery, txId);
            insertRecords(RECORD_COUNT);
            performUpdateRun(random);
            database.executeQuery("COMMIT TRANSACTION", txId);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.info("Test run {}", i);
            dropTable();
            long startTime = System.nanoTime();
            UUID txId = database.beginTransaction(null);
            String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
            LOGGER.info("Executing CREATE TABLE query in transaction test: {}", createQuery);
            database.executeQuery(createQuery, txId);
            String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
            database.executeQuery(createIndexQuery, txId);
            insertRecords(RECORD_COUNT);
            performUpdateRun(random);
            database.executeQuery("COMMIT TRANSACTION", txId);
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        LOGGER.info("TRANSACTION performance for {} records", RECORD_COUNT);
        LOGGER.info("Average execution time: {} ms", String.format("%.3f", averageTimeMs));
        LOGGER.info("Min execution time: {} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.info("Max execution time: {} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.info("Standard deviation: {} ms", String.format("%.3f", stdDevMs));
        LOGGER.info("--------------------------------");
        writeBenchmarkResult("TRANSACTION", RECORD_COUNT, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);
    }

    private void runReadUncommittedPerformanceTest() {
        LOGGER.info("Тестирование производительности READ UNCOMMITTED с {} записями", RECORD_COUNT);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (int i = 0; i < WARMUP_RUNS; i++) {
            LOGGER.info("Прогревочный запуск {}", i);
            dropTable();
            database.executeQuery("CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER)", null);
            String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
            database.executeQuery(createIndexQuery, null);
            UUID tx1Id = database.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
            UUID tx2Id = database.beginTransaction(IsolationLevel.READ_UNCOMMITTED);

            // Транзакция 1: Вставка записей
            Future<?> tx1 = executor.submit(() -> {
                for (int j = 1; j <= RECORD_COUNT; j++) {
                    String insertQuery = String.format("INSERT INTO USERS (ID, USER_CODE, NAME, AGE) VALUES ('%d', 'CODE%d', 'User%d', %d)",
                            j, j, j, 18 + (j % 52));
                    database.executeQuery(insertQuery, tx1Id);
                }
            });

            // Транзакция 2: Чтение неподтверждённых данных
            Future<?> tx2 = executor.submit(() -> {
                try {
                    Thread.sleep(50); // Убедимся, что tx1 начинает вставку
                    String selectQuery = "SELECT NAME, AGE FROM USERS WHERE AGE < 30";
                    database.executeQuery(selectQuery, tx2Id);
                } catch (InterruptedException e) {
                    LOGGER.error("Ошибка в tx2: {}", e.getMessage(), e);
                }
            });

            try {
                tx1.get();
                tx2.get();
                database.executeQuery("COMMIT TRANSACTION", tx1Id);
                database.executeQuery("COMMIT TRANSACTION", tx2Id);
            } catch (Exception e) {
                LOGGER.error("Ошибка в прогревочном запуске: {}", e.getMessage(), e);
            }
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.info("Тестовый запуск {}", i);
            dropTable();
            database.executeQuery("CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER)", null);
            String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
            database.executeQuery(createIndexQuery, null);
            UUID tx1Id = database.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
            UUID tx2Id = database.beginTransaction(IsolationLevel.READ_UNCOMMITTED);

            long startTime = System.nanoTime();
            Future<?> tx1 = executor.submit(() -> {
                for (int j = 1; j <= RECORD_COUNT; j++) {
                    String insertQuery = String.format("INSERT INTO USERS (ID, USER_CODE, NAME, AGE) VALUES ('%d', 'CODE%d', 'User%d', %d)",
                            j, j, j, 18 + (j % 52));
                    database.executeQuery(insertQuery, tx1Id);
                }
            });

            Future<?> tx2 = executor.submit(() -> {
                try {
                    Thread.sleep(50); // Убедимся, что tx1 начинает вставку
                    String selectQuery = "SELECT NAME, AGE FROM USERS WHERE AGE < 30";
                    database.executeQuery(selectQuery, tx2Id);
                } catch (InterruptedException e) {
                    LOGGER.error("Ошибка в tx2: {}", e.getMessage(), e);
                }
            });

            try {
                tx1.get();
                tx2.get();
                database.executeQuery("COMMIT TRANSACTION", tx1Id);
                database.executeQuery("COMMIT TRANSACTION", tx2Id);
            } catch (Exception e) {
                LOGGER.error("Ошибка в тесте: {}", e.getMessage(), e);
            }
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Прерывание завершения исполнителя: {}", e.getMessage(), e);
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        LOGGER.info("Производительность READ UNCOMMITTED для {} записей", RECORD_COUNT);
        LOGGER.info("Среднее время выполнения: {} мс", String.format("%.3f", averageTimeMs));
        LOGGER.info("Минимальное время выполнения: {} мс", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.info("Максимальное время выполнения: {} мс", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.info("Стандартное отклонение: {} мс", String.format("%.3f", stdDevMs));
        LOGGER.info("--------------------------------");
        writeBenchmarkResult("READ_UNCOMMITTED", RECORD_COUNT, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);

        // Восстановление исходной схемы таблицы
        setupTable();
    }

    private void performUpdateRun(Random random) {
        String tableName = "USERS";
        Table table = database.getTable(tableName);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            String updateQuery = String.format(Locale.US, "UPDATE USERS SET SCORE = %f WHERE USER_CODE = 'CODE%d'",
                    50 + random.nextFloat() * 50, i);
            database.executeQuery(updateQuery, null);
            table.saveToFile(tableName);
        }
    }

    private void resetScoreColumn() {
        String resetQuery = String.format(Locale.US, "UPDATE USERS SET SCORE = %f", 50.0);
        database.executeQuery(resetQuery, null);
        Table table = database.getTable("USERS");
        table.saveToFile("USERS");
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
        } catch (TableNotFoundException e) {
            LOGGER.warn("Table USERS not found for dropping");
        }
    }

    private List<String> prepareQueries() {
        return Arrays.asList(
                "SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = 25",
                "SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE > 75.0", // Keep original case for testing
                "SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE",
                "SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 40 OR LEVEL > 50",
                "SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE = 30",
                "SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500",
                "SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE < 40 OR NOT ACTIVE = FALSE) AND RANK < 5",
                "SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'", // Clustered index query
                "SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' AND AGE = 25" // Clustered index with additional condition
        );
    }

    private void runPerformanceTest(String query) {
        LOGGER.info("Testing query: {}", query);

        for (int i = 0; i < WARMUP_RUNS; i++) {
            Object result = database.executeQuery(query, null);
            assertQueryRowCount(query, result);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        LOGGER.info("Query: {}", query);
        LOGGER.info("Average execution time: {} ms", String.format("%.3f", averageTimeMs));
        LOGGER.info("Min execution time: {} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.info("Max execution time: {} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.info("Standard deviation: {} ms", String.format("%.3f", stdDevMs));
        LOGGER.info("--------------------------------");
        writeBenchmarkResult("SELECT", query, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);
    }

    private double calculateStandardDeviation(List<Long> times, double meanNs) {
        double sumSquaredDiff = times.stream()
                .mapToDouble(time -> Math.pow(time - meanNs, 2))
                .sum();
        return Math.sqrt(sumSquaredDiff / times.size());
    }

    private void runTrueConditionPerformanceTest() {
        String query = "SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE";
        LOGGER.info("Testing TRUE condition query performance: {}", query);

        for (int i = 0; i < WARMUP_RUNS; i++) {
            Object result = database.executeQuery(query, null);
            assertQueryRowCount(query, result);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            long startTime = System.nanoTime();
            database.executeQuery(query, null);
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        LOGGER.info("TRUE condition query: {}", query);
        LOGGER.info("Average execution time: {} ms", String.format("%.3f", averageTimeMs));
        LOGGER.info("Min execution time: {} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.info("Max execution time: {} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.info("Standard deviation: {} ms", String.format("%.3f", stdDevMs));
        LOGGER.info("--------------------------------");
        writeBenchmarkResult("TRUE_CONDITION", query, averageTimeMs, minTimeNs / 1_000_000.0, maxTimeNs / 1_000_000.0, stdDevMs);

        if (averageTimeMs > TRUE_CONDITION_WARNING_THRESHOLD_MS) {
            LOGGER.warn("TRUE condition query is too slow: {} ms (threshold {} ms)",
                    String.format("%.3f", averageTimeMs), TRUE_CONDITION_WARNING_THRESHOLD_MS);
        }
    }

    private void writeBenchmarkResult(String operation, String details, double avgMs, double minMs, double maxMs, double stdDevMs) {
        try (FileWriter fw = new FileWriter(BENCHMARK_REPORT_FILE, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.printf("| %-20s | %-50s | %10.3f | %10.3f | %10.3f | %10.3f |%n",
                    operation, details.length() > 48 ? details.substring(0, 48) + ".." : details, avgMs, minMs, maxMs, stdDevMs);
        } catch (IOException e) {
            LOGGER.error("Failed to write benchmark report: {}", e.getMessage(), e);
        }
    }

    private void writeBenchmarkResult(String operation, int recordCount, double avgMs, double minMs, double maxMs, double stdDevMs) {
        writeBenchmarkResult(operation, String.valueOf(recordCount) + " records", avgMs, minMs, maxMs, stdDevMs);
    }
}
