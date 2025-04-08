import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DieselDBPerformanceTest {
    private static DieselDBClient client;
    private static final int THREAD_COUNT = 10; // Количество потоков
    private static final int OPERATIONS_PER_THREAD = 1000; // Количество операций на поток
    private static final int TOTAL_OPERATIONS = THREAD_COUNT * OPERATIONS_PER_THREAD;

    public static void main(String[] args) throws IOException, InterruptedException {
        // Запускаем сервер в отдельном потоке
        new Thread(() -> {
            try {
                DieselDBServer.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Даем серверу время на запуск
        Thread.sleep(1000);

        client = new DieselDBClient("localhost", DieselDBConfig.PORT);
        runPerformanceTests();
        client.close();
    }

    private static void runPerformanceTests() throws IOException, InterruptedException {
        System.out.println("Starting DieselDB Performance Test");
        System.out.println("Threads: " + THREAD_COUNT + ", Operations per thread: " + OPERATIONS_PER_THREAD);
        System.out.println("Total operations: " + TOTAL_OPERATIONS);

        // Создаем таблицу с автоинкрементным первичным ключом
        String createResponse = client.create("perf_users", "id:autoincrement:primary,name:string,age:integer");
        System.out.println("Table creation: " + createResponse);

        // Тест вставки
        testInsertPerformance();

        // Тест выборки
        testSelectPerformance();

        // Тест обновления
        testUpdatePerformance();

        // Очистка
        String deleteResponse = client.delete("perf_users", null);
        System.out.println("Cleanup: " + deleteResponse);
    }

    private static void testInsertPerformance() throws InterruptedException {
        System.out.println("\n=== Insert Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalInsertTime = new AtomicLong(0);
        AtomicLong successfulInserts = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        long start = System.nanoTime();
                        String name = "User_" + threadId + "_" + j;
                        String values = name + "," + (20 + (threadId + j) % 80); // Возраст от 20 до 99
                        String response = client.insert("perf_users", "name,age", values);
                        long end = System.nanoTime();
                        if (response.equals("OK: 1 row inserted")) {
                            totalInsertTime.addAndGet(end - start);
                            successfulInserts.incrementAndGet();
                        } else {
                            System.err.println("Insert failed: " + response);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Insert error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        double totalTimeMs = (endTime - startTime);
        double avgTimePerInsertNs = successfulInserts.get() > 0 ? totalInsertTime.get() / (double) successfulInserts.get() : 0;
        double throughput = successfulInserts.get() / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful inserts: " + successfulInserts.get());
        System.out.println("Average time per insert: " + String.format("%.2f", avgTimePerInsertNs / 1_000_000.0) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " inserts/second");
    }

    private static void testSelectPerformance() throws InterruptedException {
        System.out.println("\n=== Select Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalSelectTime = new AtomicLong(0);
        AtomicLong successfulSelects = new AtomicLong(0);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        // Случайный выбор по ID (от 1 до TOTAL_OPERATIONS)
                        int id = random.nextInt(TOTAL_OPERATIONS) + 1;
                        long start = System.nanoTime();
                        List<Map<String, String>> result = client.select("perf_users", "id=" + id, null);
                        long end = System.nanoTime();
                        if (!result.isEmpty() || "OK: 0 rows".equals(client.sendCommand("SELECT perf_users id=" + id))) {
                            totalSelectTime.addAndGet(end - start);
                            successfulSelects.incrementAndGet();
                        } else {
                            System.err.println("Select failed for id=" + id);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Select error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        double totalTimeMs = (endTime - startTime);
        double avgTimePerSelectNs = successfulSelects.get() > 0 ? totalSelectTime.get() / (double) successfulSelects.get() : 0;
        double throughput = successfulSelects.get() / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful selects: " + successfulSelects.get());
        System.out.println("Average time per select: " + String.format("%.2f", avgTimePerSelectNs / 1_000_000.0) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " selects/second");
    }

    private static void testUpdatePerformance() throws InterruptedException {
        System.out.println("\n=== Update Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicLong totalUpdateTime = new AtomicLong(0);
        AtomicLong successfulUpdates = new AtomicLong(0);
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        // Случайное обновление возраста по ID
                        int id = random.nextInt(TOTAL_OPERATIONS) + 1;
                        Map<String, String> updates = new HashMap<>();
                        updates.put("age", String.valueOf(20 + (threadId + j) % 80));
                        long start = System.nanoTime();
                        String response = client.update("perf_users", "id=" + id, updates);
                        long end = System.nanoTime();
                        if (response.startsWith("OK")) {
                            totalUpdateTime.addAndGet(end - start);
                            successfulUpdates.incrementAndGet();
                        } else {
                            System.err.println("Update failed for id=" + id + ": " + response);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Update error: " + e.getMessage());
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        double totalTimeMs = (endTime - startTime);
        double avgTimePerUpdateNs = successfulUpdates.get() > 0 ? totalUpdateTime.get() / (double) successfulUpdates.get() : 0;
        double throughput = successfulUpdates.get() / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful updates: " + successfulUpdates.get());
        System.out.println("Average time per update: " + String.format("%.2f", avgTimePerUpdateNs / 1_000_000.0) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " updates/second");
    }
}