import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;

public class DieselDBPerformanceTest {
    private static final int RECORD_COUNT = 1000;
    private static final int BATCH_SIZE = 10;
    private static final String TABLE_NAME_1 = "perf_users";
    private static final String TABLE_NAME_2 = "perf_orders";
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        DieselDBClient client = new DieselDBClient("localhost", 9090);

        try {
            client.connect();
            System.out.println("Connected to server");

            // Cleanup existing tables if they exist
            try {
                client.delete(TABLE_NAME_1, null);
            } catch (IOException e) {
                if (!e.getMessage().contains("Table not found")) {
                    throw e; // Rethrow if it's not a "Table not found" error
                }
                System.out.println("Table " + TABLE_NAME_1 + " didn't exist for cleanup");
            }
            try {
                client.delete(TABLE_NAME_2, null);
            } catch (IOException e) {
                if (!e.getMessage().contains("Table not found")) {
                    throw e;
                }
                System.out.println("Table " + TABLE_NAME_2 + " didn't exist for cleanup");
            }

            // Create tables
            client.createTable(TABLE_NAME_1);
            client.createTable(TABLE_NAME_2);
            System.out.println("Tables created");

            // Run tests
            testInsert(client);
            testSelect(client);
            testJoin(client);
            testUpdate(client);
            testDelete(client);

        } catch (IOException e) {
            System.err.println("Error during test execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.disconnect();
        }
    }

    private static void testInsert(DieselDBClient client) throws IOException {
        System.out.println("Starting insert performance test...");
        long startTime = System.nanoTime();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < RECORD_COUNT; i += BATCH_SIZE) {
            final int batchStart = i;
            futures.add(executor.submit(() -> {
                try {
                    for (int j = batchStart; j < batchStart + BATCH_SIZE && j < RECORD_COUNT; j++) {
                        Map<String, Object> user = new HashMap<>();
                        user.put("id", j);
                        user.put("name", "User_" + j);
                        user.put("balance", new BigDecimal(j * 1.25));
                        user.put("active", j % 2 == 0);
                        user.put("join_date", new Date(System.currentTimeMillis() - j * 86400000L));
                        client.insert(TABLE_NAME_1, user);

                        Map<String, Object> order = new HashMap<>();
                        order.put("order_id", j);
                        order.put("user_id", j);
                        order.put("amount", new BigDecimal(j * 9.99));
                        order.put("completed", j % 3 == 0);
                        client.insert(TABLE_NAME_2, order);
                    }
                } catch (IOException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }));
        }

        awaitFutures(futures);
        executor.shutdown();
        awaitTermination(executor);

        long endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1_000_000_000.0;
        double recordsPerSec = (RECORD_COUNT * 2) / duration;

        System.out.printf("Inserted %,d records across 2 tables in %.3f seconds (%.2f records/sec)%n",
                RECORD_COUNT * 2, duration, recordsPerSec);
    }

    private static void testSelect(DieselDBClient client) throws IOException {
        System.out.println("Starting select performance test...");

        client.select(TABLE_NAME_1, null); // Warm-up

        long startTime = System.nanoTime();
        List<Map<String, Object>> complexFilter = client.select(TABLE_NAME_1,
                "balance>=500.00 AND active=true OR join_date>2024-01-01");
        long endTime = System.nanoTime();

        double complexDuration = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Selected %d records with complex condition in %.2f ms%n",
                complexFilter.size(), complexDuration);
        if (!complexFilter.isEmpty()) {
            System.out.println("Sample record: " + complexFilter.get(0));
        }

        startTime = System.nanoTime();
        List<Map<String, Object>> rangeFilter = client.select(TABLE_NAME_1,
                "id>=100 AND id<=200");
        endTime = System.nanoTime();

        double rangeDuration = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Selected %d records with range condition in %.2f ms%n",
                rangeFilter.size(), rangeDuration);
    }

    private static void testJoin(DieselDBClient client) throws IOException {
        System.out.println("Starting join performance test...");

        long startTime = System.nanoTime();
        List<Map<String, Object>> joinedRecords = client.select(TABLE_NAME_1,
                "JOIN§§§" + TABLE_NAME_2 + "§§§id=user_id§§§amount>100.00 AND completed=false");
        long endTime = System.nanoTime();

        double joinDuration = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Joined %d records with condition in %.2f ms%n",
                joinedRecords.size(), joinDuration);
        if (!joinedRecords.isEmpty()) {
            System.out.println("Sample joined record: " + joinedRecords.get(0));
        }
    }

    private static void testUpdate(DieselDBClient client) throws IOException {
        System.out.println("Starting update performance test...");
        long startTime = System.nanoTime();

        Map<String, Object> updates = new HashMap<>();
        updates.put("balance", new BigDecimal("999.99"));
        updates.put("active", false);
        updates.put("join_date", new Date());

        int updated = client.update(TABLE_NAME_1,
                "balance<=500.00 AND active=true",
                updates);

        long endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1_000_000_000.0;
        double recordsPerSec = updated > 0 ? updated / duration : 0;

        System.out.printf("Updated %,d records with multiple types in %.3f seconds (%.2f records/sec)%n",
                updated, duration, recordsPerSec);
    }

    private static void testDelete(DieselDBClient client) throws IOException {
        System.out.println("Starting delete performance test...");

        testInsert(client); // Recreate data

        long startTime = System.nanoTime();
        int deleted = client.delete(TABLE_NAME_2,
                "amount<500.00 OR completed=true");
        long endTime = System.nanoTime();

        double duration = (endTime - startTime) / 1_000_000_000.0;
        double recordsPerSec = deleted > 0 ? deleted / duration : 0;

        System.out.printf("Deleted %,d records with condition in %.3f seconds (%.2f records/sec)%n",
                deleted, duration, recordsPerSec);
    }

    private static void awaitFutures(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                System.err.println("Task failed: " + e.getMessage());
            }
        }
    }

    private static void awaitTermination(ExecutorService executor) {
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}