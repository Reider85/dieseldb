package diesel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PerformanceTest {
    private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private static final int WARMUP_RUNS = 5;
    private static final int TEST_RUNS = 10;
    private final Database database;

    public PerformanceTest() {
        this.database = new Database();
    }

    public void runTests() {
        setupTable();
        List<String> queries = prepareQueries();
        for (String query : queries) {
            runPerformanceTest(query);
        }
    }

    private void setupTable() {
        // Create table with the same structure as in DatabaseClient
        String createTableQuery = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, LAST_ACTION DATETIME_MS, USER_SCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
        database.executeQuery(createTableQuery);

        // Generate 1000 random records
        Random random = new Random();
        List<String> columns = Arrays.asList("ID", "NAME", "AGE", "ACTIVE", "BIRTHDATE", "LAST_LOGIN", "LAST_ACTION", "USER_SCORE", "LEVEL", "RANK", "BALANCE", "SCORE", "PRECISION", "INITIAL", "SESSION_ID");

        for (int i = 1; i <= RECORD_COUNT; i++) {
            List<Object> values = new ArrayList<>();
            String id = String.valueOf(i);
            String name = "User" + i;
            int age = 18 + random.nextInt(52); // Ages 18-69
            boolean active = random.nextBoolean();
            LocalDate birthdate = LocalDate.of(1955 + random.nextInt(50), 1 + random.nextInt(12), 1 + random.nextInt(28));
            LocalDateTime lastLogin = LocalDateTime.of(2023, 10, 1 + random.nextInt(30), random.nextInt(24), random.nextInt(60), random.nextInt(60));
            LocalDateTime lastAction = lastLogin.plusSeconds(random.nextInt(3600)).plusNanos(random.nextInt(999000000));
            long userScore = random.nextLong() % 1000000000L;
            short level = (short) (1 + random.nextInt(100));
            byte rank = (byte) (1 + random.nextInt(10));
            java.math.BigDecimal balance = new java.math.BigDecimal(100 + random.nextDouble() * 1000).setScale(2, java.math.RoundingMode.HALF_UP);
            float score = 50 + random.nextFloat() * 50;
            double precision = 1000 + random.nextDouble() * 100000;
            char initial = (char) ('A' + random.nextInt(26));
            UUID sessionId = UUID.randomUUID();

            // Add values in the same order as columns
            values.add(id);
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

            // Create and execute InsertQuery
            InsertQuery insertQuery = new InsertQuery(columns, values);
            String tableName = "USERS";
            try {
                java.lang.reflect.Field tablesField = database.getClass().getDeclaredField("tables");
                tablesField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String, Table> tables = (Map<String, Table>) tablesField.get(database);
                Table table = tables.get(tableName);
                if (table == null) {
                    throw new IllegalStateException("Table USERS not found");
                }
                insertQuery.execute(table);
                table.saveToFile(tableName);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                LOGGER.log(Level.SEVERE, "Failed to access tables field: {0}", e.getMessage());
                throw new RuntimeException("Reflection error", e);
            }
        }
        LOGGER.log(Level.INFO, "Setup completed: {0} records inserted into USERS table", RECORD_COUNT);
    }

    private List<String> prepareQueries() {
        // Define a variety of SELECT queries to test WHERE clause performance
        return Arrays.asList(
                // Simple equality condition
                "SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = 25",
                // Range comparison
                "SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE > 75.0",
                // Compound condition with AND
                "SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE",
                // Compound condition with OR
                "SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 40 OR LEVEL > 50",
                // Condition with NOT
                "SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE = 30",
                // Grouped condition with parentheses
                "SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500",
                // Complex condition with multiple operators and grouping
                "SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE < 40 OR NOT ACTIVE = FALSE) AND RANK < 5"
        );
    }

    private void runPerformanceTest(String query) {
        LOGGER.log(Level.INFO, "Testing query: {0}", query);

        // Warm-up runs
        for (int i = 0; i < WARMUP_RUNS; i++) {
            database.executeQuery(query);
        }

        // Measure execution time
        List<Long> executionTimes = new ArrayList<>();
        for (int i = 0; i < TEST_RUNS; i++) {
            long startTime = System.nanoTime();
            database.executeQuery(query);
            long endTime = System.nanoTime();
            executionTimes.add(endTime - startTime);
        }

        // Calculate statistics
        double averageTimeMs = executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0) / 1_000_000.0;
        long minTimeNs = executionTimes.stream().min(Long::compareTo).orElse(0L);
        long maxTimeNs = executionTimes.stream().max(Long::compareTo).orElse(0L);
        double stdDevMs = calculateStandardDeviation(executionTimes, averageTimeMs * 1_000_000.0) / 1_000_000.0;

        // Log results
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