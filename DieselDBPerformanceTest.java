import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DieselDBPerformanceTest {
    private static DieselDBClient client;
    private static final int THREAD_COUNT = 10;
    private static final int OPERATIONS_PER_THREAD = 100;
    private static final int TOTAL_OPERATIONS = THREAD_COUNT * OPERATIONS_PER_THREAD;

    public static void main(String[] args) throws IOException, InterruptedException {
        client = new DieselDBClient("localhost", DieselDBConfig.PORT);
        runPerformanceTests();
        client.close();
    }

    private static void runPerformanceTests() throws IOException, InterruptedException {
        System.out.println("Starting DieselDB Performance Test");
        System.out.println("Threads: " + THREAD_COUNT + ", Operations per thread: " + OPERATIONS_PER_THREAD);

        // Создаем тестовые таблицы
        client.create("users", "id:autoincrement:primary,name:string:unique,age:integer");
        client.create("orders", "order_id:autoincrement:primary,user_id:integer,amount:bigdecimal");

        testAutoIncrementPerformance();
        testUniqueIndexPerformance();
        testUniqueIndexSearchPerformance();
        testInsertPerformance();
        testDeletePerformance();
        testUpdatePerformance();
        testWherePerformance();
        testJoinPerformance();
        testTransactionPerformance();
        testOrderByPerformance();

        client.delete("users", null);
        client.delete("orders", null);
    }

    private static void testAutoIncrementPerformance() throws InterruptedException {
        System.out.println("\n=== AutoIncrement Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        String response = client.insert("users", "name,age",
                                "User_" + threadId + "_" + j + "," + (20 + j % 80));
                        long end = System.nanoTime();
                        if (response.equals("OK: 1 row inserted")) {
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("AutoIncrement error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testUniqueIndexPerformance() throws InterruptedException {
        System.out.println("\n=== Unique Index Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        String response = client.insert("users", "name,age",
                                "Unique_" + threadId + "_" + j + "," + (20 + j % 80));
                        long end = System.nanoTime();
                        if (response.equals("OK: 1 row inserted")) {
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Unique Index error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testUniqueIndexSearchPerformance() throws InterruptedException {
        System.out.println("\n=== Unique Index Search Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String name = "Unique_" + random.nextInt(THREAD_COUNT) + "_" +
                                random.nextInt(OPERATIONS_PER_THREAD);
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("users", "name=" + name, null);
                        long end = System.nanoTime();
                        totalTime.addAndGet(end - start);
                        successfulOps.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("Unique Index Search error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testInsertPerformance() throws InterruptedException {
        System.out.println("\n=== Insert Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        String response = client.insert("users", "name,age",
                                "Insert_" + threadId + "_" + j + "," + (20 + j % 80));
                        long end = System.nanoTime();
                        if (response.equals("OK: 1 row inserted")) {
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Insert error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testDeletePerformance() throws InterruptedException {
        System.out.println("\n=== Delete Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        int id = random.nextInt(TOTAL_OPERATIONS * 2) + 1;
                        long start = System.nanoTime();
                        String response = client.delete("users", "id=" + id);
                        long end = System.nanoTime();
                        if (response.startsWith("OK")) {
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Delete error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testUpdatePerformance() throws InterruptedException {
        System.out.println("\n=== Update Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        int id = random.nextInt(TOTAL_OPERATIONS * 2) + 1;
                        Map<String, String> updates = new HashMap<>();
                        updates.put("age", String.valueOf(20 + j % 80));
                        long start = System.nanoTime();
                        String response = client.update("users", "id=" + id, updates);
                        long end = System.nanoTime();
                        if (response.startsWith("OK")) {
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Update error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testWherePerformance() throws InterruptedException, IOException {
        System.out.println("\n=== WHERE Performance Test ===");
        client.delete("users", null);
        System.out.println("Tables cleared");

        for (int i = 1; i <= TOTAL_OPERATIONS; i++) {
            client.insert("users", "name,age", "User_" + i + "," + (20 + i % 80));
        }
        System.out.println("Inserted " + TOTAL_OPERATIONS + " users");

        Thread.sleep(2000);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("users", "age>30", null);
                        long end = System.nanoTime();
                        totalTime.addAndGet(end - start);
                        successfulOps.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("WHERE error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void testJoinPerformance() throws InterruptedException, IOException {
        System.out.println("\n=== JOIN Performance Test ===");

        String clearUsers = client.delete("users", null);
        String clearOrders = client.delete("orders", null);
        System.out.println("Tables cleared: " + clearUsers + ", " + clearOrders);

        int usersInserted = 0;
        for (int i = 1; i <= TOTAL_OPERATIONS; i++) {
            String response = client.insert("users", "name,age", "User_" + i + "," + (20 + i % 80));
            System.out.println("Insert user " + i + " response: " + response);
            if (response.equals("OK: 1 row inserted")) usersInserted++;
            else System.err.println("Failed to insert user " + i + ": " + response);
        }
        System.out.println("Inserted " + usersInserted + " users (expected " + TOTAL_OPERATIONS + ")");

        String rawUsersResponse = client.sendCommand("SELECT users WHERE null ORDER BY id ASC");
        System.out.println("Raw SELECT users response: " + rawUsersResponse);
        List<Map<String, String>> usersImmediate = client.select("users", null, "id ASC");
        System.out.println("Users count immediately after insert: " + usersImmediate.size());

        int ordersInserted = 0;
        for (int i = 1; i <= TOTAL_OPERATIONS; i++) {
            String amountValue = "bigdecimal:" + (i * 10);
            String response = client.insert("orders", "user_id,amount", i + "," + amountValue);
            System.out.println("Insert order " + i + " response: " + response);
            if (response.equals("OK: 1 row inserted")) ordersInserted++;
            else System.err.println("Failed to insert order " + i + ": " + response);
        }
        System.out.println("Inserted " + ordersInserted + " orders (expected " + TOTAL_OPERATIONS + ")");

        String rawOrdersResponse = client.sendCommand("SELECT orders WHERE null ORDER BY order_id ASC");
        System.out.println("Raw SELECT orders response: " + rawOrdersResponse);
        List<Map<String, String>> ordersImmediate = client.select("orders", null, "order_id ASC");
        System.out.println("Orders count immediately after insert: " + ordersImmediate.size());

        Thread.sleep(5000);

        List<Map<String, String>> users = client.select("users", null, "id ASC");
        List<Map<String, String>> orders = client.select("orders", null, "order_id ASC");
        System.out.println("Users count after delay: " + users.size());
        System.out.println("Orders count after delay: " + orders.size());
        System.out.println("Sample users (first 5): " + client.select("users", "id<=5", "id ASC"));
        System.out.println("Sample orders (first 5): " + client.select("orders", "order_id<=5", "order_id ASC"));

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong totalRowsFetched = new AtomicLong(0);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("users",
                                "JOIN orders ON users.id=orders.user_id", null);
                        long end = System.nanoTime();
                        totalTime.addAndGet(end - start);
                        successfulOps.incrementAndGet();
                        totalRowsFetched.addAndGet(result.size());
                    }
                } catch (IOException e) {
                    System.err.println("JOIN error: " + e.getMessage());
                }
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
        System.out.println("Total rows fetched by JOIN: " + totalRowsFetched.get());
        System.out.println("Average rows per JOIN: " +
                String.format("%.2f", totalRowsFetched.get() / (double) successfulOps.get()));
    }
    private static void testTransactionPerformance() throws InterruptedException, IOException {
        System.out.println("\n=== Transaction Performance Test ===");
        String[] isolationLevels = {"READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"};

        for (String level : isolationLevels) {
            System.out.println("\nTesting isolation level: " + level);
            client.setIsolation(level);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            AtomicLong totalTime = new AtomicLong(0);
            AtomicLong successfulOps = new AtomicLong(0);
            Random random = new Random();

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            long start = System.nanoTime();
                            client.begin();
                            client.insert("users", "name,age", "Trans_" + j + "," + (20 + j % 80));
                            client.update("users", "id=" + (random.nextInt(TOTAL_OPERATIONS * 2) + 1),
                                    new HashMap<>() {{ put("age", "25"); }});
                            client.commit();
                            long end = System.nanoTime();
                            totalTime.addAndGet(end - start);
                            successfulOps.incrementAndGet();
                        }
                    } catch (IOException e) {
                        System.err.println("Transaction error: " + e.getMessage());
                    }
                }, executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executor.shutdown();
            printResults(totalTime, successfulOps, startTime);
        }
    }

    private static void testOrderByPerformance() throws InterruptedException {
        System.out.println("\n=== ORDER BY Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("users", null, "age ASC");
                        long end = System.nanoTime();
                        totalTime.addAndGet(end - start);
                        successfulOps.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("ORDER BY error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
    }

    private static void printResults(AtomicLong totalTime, AtomicLong successfulOps, long startTime) throws InterruptedException {
        Thread.sleep(100);
        long endTime = System.currentTimeMillis();
        double totalTimeMs = (endTime - startTime);
        double avgTimeNs = successfulOps.get() > 0 ? totalTime.get() / (double) successfulOps.get() : 0;
        double throughput = successfulOps.get() / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful operations: " + successfulOps.get());
        System.out.println("Average time per operation: " + String.format("%.2f", avgTimeNs / 1_000_000.0) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " ops/second");
    }
}