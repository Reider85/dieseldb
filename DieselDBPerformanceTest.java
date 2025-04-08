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

    private static void testWherePerformance() throws InterruptedException {
        System.out.println("\n=== WHERE Performance Test ===");
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
                        String condition = "age>" + (20 + random.nextInt(60));
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("users", condition, null);
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
        for (int i = 1; i <= TOTAL_OPERATIONS; i++) {
            client.insert("orders", "user_id,amount", i + "," + (i * 10));
        }

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
                        List<Map<String, String>> result = client.select("users",
                                "JOIN orders ON users.id=orders.user_id", null);
                        long end = System.nanoTime();
                        totalTime.addAndGet(end - start);
                        successfulOps.incrementAndGet();
                    }
                } catch (IOException e) {
                    System.err.println("JOIN error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        printResults(totalTime, successfulOps, startTime);
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