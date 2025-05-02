package diesel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Locale;
import java.util.concurrent.*;

public class PerformanceTest {
    private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());
    private static final int RECORD_COUNT = 100;
    private static final int WARMUP_RUNS = 1;
    private static final int TEST_RUNS = 10;
    private final Database database;

    public PerformanceTest() {
        this.database = new Database();
    }

    public void runTests() {
        runInsertPerformanceTest();
        setupTable();
        runUpdatePerformanceTest();
        runTransactionPerformanceTest();
        runReadUncommittedPerformanceTest();
        List<String> queries = prepareQueries();
        for (String query : queries) {
            runPerformanceTest(query);
        }
    }

    private void setupTable() {
        dropTable(); // Ensure table does not exist
        String createTableQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
        LOGGER.log(Level.INFO, "Executing CREATE TABLE query in setupTable: {0}", createTableQuery);
        database.executeQuery(createTableQuery, null);

        // Create clustered index on USER_CODE
        String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
        LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
        database.executeQuery(createIndexQuery, null);
        LOGGER.log(Level.INFO, "Unique clustered index created on USER_CODE");

        insertRecords(RECORD_COUNT);
        LOGGER.log(Level.INFO, "Setup completed: {0} records inserted into USERS table", RECORD_COUNT);
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
            LOGGER.log(Level.INFO, "Starting INSERT performance test for {0} records", RECORD_COUNT);

            List<String> columns = Arrays.asList("ID", "USER_CODE", "NAME", "AGE", "ACTIVE", "BIRTHDATE", "LASTLOGIN", "LASTACTION", "USERSCORE", "LEVEL", "RANK", "BALANCE", "SCORE", "PRECISION", "INITIAL", "SESSION_ID");
            Random random = new Random();

            for (int i = 0; i < WARMUP_RUNS; i++) {
                LOGGER.log(Level.INFO, "Warmup run {0}", i);
                dropTable();
                String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
                LOGGER.log(Level.INFO, "Executing CREATE TABLE query in warmup: {0}", createQuery);
                database.executeQuery(createQuery, null);
                String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
                database.executeQuery(createIndexQuery, null);
                insertRecords(RECORD_COUNT);
            }

            List<Long> executionTimes = new ArrayList<>();
            for (int i = 0; i < TEST_RUNS; i++) {
                LOGGER.log(Level.INFO, "Test run {0}", i);
                dropTable();
                String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
                LOGGER.log(Level.INFO, "Executing CREATE TABLE query in test run: {0}", createQuery);
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

            LOGGER.log(Level.INFO, "INSERT performance for {0} records", RECORD_COUNT);
            LOGGER.log(Level.INFO, "Average execution time: {0} ms", String.format("%.3f", averageTimeMs));
            LOGGER.log(Level.INFO, "Min execution time: {0} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
            LOGGER.log(Level.INFO, "Max execution time: {0} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
            LOGGER.log(Level.INFO, "Standard deviation: {0} ms", String.format("%.3f", stdDevMs));
            LOGGER.log(Level.INFO, "--------------------------------");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in INSERT performance test: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void runUpdatePerformanceTest() {
        LOGGER.log(Level.INFO, "Testing UPDATE performance for {0} records", RECORD_COUNT);

        Random random = new Random();

        for (int i = 0; i < WARMUP_RUNS; i++) {
            resetScoreColumn();
            performUpdateRun(random);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.log(Level.INFO, "Test run {0}", i);
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

        LOGGER.log(Level.INFO, "UPDATE performance for {0} records", RECORD_COUNT);
        LOGGER.log(Level.INFO, "Average execution time: {0} ms", String.format("%.3f", averageTimeMs));
        LOGGER.log(Level.INFO, "Min execution time: {0} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Max execution time: {0} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Standard deviation: {0} ms", String.format("%.3f", stdDevMs));
        LOGGER.log(Level.INFO, "--------------------------------");
    }

    private void runTransactionPerformanceTest() {
        LOGGER.log(Level.INFO, "Testing TRANSACTION performance for {0} records", RECORD_COUNT);

        Random random = new Random();

        for (int i = 0; i < WARMUP_RUNS; i++) {
            LOGGER.log(Level.INFO, "Warmup run {0}", i);
            dropTable();
            UUID txId = database.beginTransaction(null);
            String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
            LOGGER.log(Level.INFO, "Executing CREATE TABLE query in transaction warmup: {0}", createQuery);
            database.executeQuery(createQuery, txId);
            String createIndexQuery = "CREATE UNIQUE CLUSTERED INDEX ON USERS (USER_CODE)";
            database.executeQuery(createIndexQuery, txId);
            insertRecords(RECORD_COUNT);
            performUpdateRun(random);
            database.executeQuery("COMMIT TRANSACTION", txId);
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.log(Level.INFO, "Test run {0}", i);
            dropTable();
            long startTime = System.nanoTime();
            UUID txId = database.beginTransaction(null);
            String createQuery = "CREATE TABLE USERS (ID STRING, USER_CODE STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LASTLOGIN DATETIME, LASTACTION DATETIME_MS, USERSCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
            LOGGER.log(Level.INFO, "Executing CREATE TABLE query in transaction test: {0}", createQuery);
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

        LOGGER.log(Level.INFO, "TRANSACTION performance for {0} records", RECORD_COUNT);
        LOGGER.log(Level.INFO, "Average execution time: {0} ms", String.format("%.3f", averageTimeMs));
        LOGGER.log(Level.INFO, "Min execution time: {0} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Max execution time: {0} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Standard deviation: {0} ms", String.format("%.3f", stdDevMs));
        LOGGER.log(Level.INFO, "--------------------------------");
    }

    private void runReadUncommittedPerformanceTest() {
        LOGGER.log(Level.INFO, "Тестирование производительности READ UNCOMMITTED с {0} записями", RECORD_COUNT);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Random random = new Random();

        for (int i = 0; i < WARMUP_RUNS; i++) {
            LOGGER.log(Level.INFO, "Прогревочный запуск {0}", i);
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
                    LOGGER.log(Level.SEVERE, "Ошибка в tx2: {0}", e.getMessage());
                }
            });

            try {
                tx1.get();
                tx2.get();
                database.executeQuery("COMMIT TRANSACTION", tx1Id);
                database.executeQuery("COMMIT TRANSACTION", tx2Id);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Ошибка в прогревочном запуске: {0}", e.getMessage());
            }
        }

        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            LOGGER.log(Level.INFO, "Тестовый запуск {0}", i);
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
                    LOGGER.log(Level.SEVERE, "Ошибка в tx2: {0}", e.getMessage());
                }
            });

            try {
                tx1.get();
                tx2.get();
                database.executeQuery("COMMIT TRANSACTION", tx1Id);
                database.executeQuery("COMMIT TRANSACTION", tx2Id);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Ошибка в тесте: {0}", e.getMessage());
            }
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Прерывание завершения исполнителя: {0}", e.getMessage());
        }

        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        LOGGER.log(Level.INFO, "Производительность READ UNCOMMITTED для {0} записей", RECORD_COUNT);
        LOGGER.log(Level.INFO, "Среднее время выполнения: {0} мс", String.format("%.3f", averageTimeMs));
        LOGGER.log(Level.INFO, "Минимальное время выполнения: {0} мс", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Максимальное время выполнения: {0} мс", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Стандартное отклонение: {0} мс", String.format("%.3f", stdDevMs));
        LOGGER.log(Level.INFO, "--------------------------------");

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
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table USERS not found for dropping");
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
        LOGGER.log(Level.INFO, "Testing query: {0}", query);

        for (int i = 0; i < WARMUP_RUNS; i++) {
            database.executeQuery(query, null);
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

        LOGGER.log(Level.INFO, "Query: {0}", query);
        LOGGER.log(Level.INFO, "Average execution time: {0} ms", String.format("%.3f", averageTimeMs));
        LOGGER.log(Level.INFO, "Min execution time: {0} ms", String.format("%.3f", minTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Max execution time: {0} ms", String.format("%.3f", maxTimeNs / 1_000_000.0));
        LOGGER.log(Level.INFO, "Standard deviation: {0} ms", String.format("%.3f", stdDevMs));
        LOGGER.log(Level.INFO, "--------------------------------");
    }

    private double calculateStandardDeviation(List<Long> times, double meanNs) {
        double sumSquaredDiff = times.stream()
                .mapToDouble(time -> Math.pow(time - meanNs, 2))
                .sum();
        return Math.sqrt(sumSquaredDiff / times.size());
    }

    public static void main(String[] args) {
        PerformanceTest test = new PerformanceTest();
        test.runTests();
    }
}