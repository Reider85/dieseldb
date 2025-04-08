import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class DieselDBPerformanceTest {
    private static DieselDBClient client;

    public static void main(String[] args) throws IOException, InterruptedException {
        client = new DieselDBClient("localhost", DieselDBConfig.PORT);
        try {
            runPerformanceTests();
        } finally {
            client.close();
            System.out.println("Client connection closed");
        }
    }

    private static void runPerformanceTests() throws IOException, InterruptedException {
        System.out.println("Starting DieselDB Performance Test");
        int threads = 10;
        int operationsPerThread = 1000;
        System.out.println("Threads: " + threads + ", Operations per thread: " + operationsPerThread);
        int totalOperations = threads * operationsPerThread;
        System.out.println("Total operations: " + totalOperations);

        String createResponse = client.create("perf_users", "id:autoincrement:primary,name:string,age:integer");
        System.out.println("Table creation (perf_users): " + createResponse);

        testInsertPerformance(threads, totalOperations);
        testSelectPerformance(threads, totalOperations);
        testUpdatePerformance(threads, totalOperations);

        testJoinPerformance(100, 100, "Join 100x100");
        testJoinPerformance(1000, 1000, "Join 1000x1000");
        testJoinPerformance(10000, 10000, "Join 10000x10000");

        String cleanupResponse = client.delete("perf_users", null);
        System.out.println("Cleanup (perf_users): " + cleanupResponse);
    }

    private static void testInsertPerformance(int threads, int totalOperations) throws IOException, InterruptedException {
        System.out.println("\n=== Insert Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        int operationsPerThread = totalOperations / threads;

        for (int i = 0; i < threads; i++) {
            int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        String name = "User_" + (threadId * operationsPerThread + j);
                        String values = name + "," + (20 + j % 80);
                        String response = client.insert("perf_users", "name,age", values);
                        if (!response.equals("OK: 1 row inserted")) {
                            System.err.println("Insert failed: " + response);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        long endTime = System.currentTimeMillis();

        double totalTimeMs = (endTime - startTime);
        double avgTimePerInsertMs = totalTimeMs / totalOperations;
        double throughput = totalOperations / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful inserts: " + totalOperations);
        System.out.println("Average time per insert: " + String.format("%.2f", avgTimePerInsertMs) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " inserts/second");
    }

    private static void testSelectPerformance(int threads, int totalOperations) throws IOException, InterruptedException {
        System.out.println("\n=== Select Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        int operationsPerThread = totalOperations / threads;

        for (int i = 0; i < threads; i++) {
            int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        int id = threadId * operationsPerThread + j + 1;
                        List<Map<String, String>> result = client.select("perf_users", "id=" + id, null);
                        if (result.isEmpty()) {
                            System.err.println("Select failed for id=" + id + ": No rows returned");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        long endTime = System.currentTimeMillis();

        double totalTimeMs = (endTime - startTime);
        double avgTimePerSelectMs = totalTimeMs / totalOperations;
        double throughput = totalOperations / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful selects: " + totalOperations);
        System.out.println("Average time per select: " + String.format("%.2f", avgTimePerSelectMs) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " selects/second");
    }

    private static void testUpdatePerformance(int threads, int totalOperations) throws IOException, InterruptedException {
        System.out.println("\n=== Update Performance Test ===");
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        int operationsPerThread = totalOperations / threads;

        for (int i = 0; i < threads; i++) {
            int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        int id = threadId * operationsPerThread + j + 1;
                        String condition = "id=" + id;
                        Map<String, String> updates = Map.of("age", String.valueOf(30 + j % 70));
                        String response = client.update("perf_users", condition, updates);
                        if (!response.startsWith("OK:")) {
                            System.err.println("Update failed for id=" + id + ": " + response);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        long endTime = System.currentTimeMillis();

        double totalTimeMs = (endTime - startTime);
        double avgTimePerUpdateMs = totalTimeMs / totalOperations;
        double throughput = totalOperations / (totalTimeMs / 1000.0);

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Successful updates: " + totalOperations);
        System.out.println("Average time per update: " + String.format("%.2f", avgTimePerUpdateMs) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " updates/second");
    }

    private static void testJoinPerformance(int leftSize, int rightSize, String testName) throws IOException, InterruptedException {
        System.out.println("\n=== " + testName + " Performance Test ===");

        String createOrdersResponse = client.create("perf_orders", "id:autoincrement:primary,user_id:integer,amount:integer");
        System.out.println("Table creation (perf_orders): " + createOrdersResponse);

        client.delete("perf_users", null);
        client.delete("perf_orders", null);

        long startFillUsers = System.currentTimeMillis();
        for (int i = 0; i < leftSize; i++) {
            String values = "User_" + i + "," + (20 + i % 80);
            String response = client.insert("perf_users", "name,age", values);
            if (!response.equals("OK: 1 row inserted")) {
                System.err.println("Insert failed for perf_users: " + response);
            }
        }
        long endFillUsers = System.currentTimeMillis();
        System.out.println("Filled perf_users with " + leftSize + " rows in " + (endFillUsers - startFillUsers) + " ms");

        List<Map<String, String>> users = client.select("perf_users", null, "id ASC");
        System.out.println("Sample from perf_users (first 5 rows):");
        for (int i = 0; i < Math.min(5, users.size()); i++) {
            System.out.println(users.get(i));
        }
        if (users.isEmpty()) {
            System.err.println("WARNING: perf_users is empty after insertion!");
        }

        long startFillOrders = System.currentTimeMillis();
        for (int i = 0; i < rightSize; i++) {
            int userId = (i % leftSize) + 1;
            String values = userId + "," + (100 + i % 900);
            String response = client.insert("perf_orders", "user_id,amount", values);
            if (!response.equals("OK: 1 row inserted")) {
                System.err.println("Insert failed for perf_orders: " + response);
            }
        }
        long endFillOrders = System.currentTimeMillis();
        System.out.println("Filled perf_orders with " + rightSize + " rows in " + (endFillOrders - startFillOrders) + " ms");

        List<Map<String, String>> orders = client.select("perf_orders", null, "id ASC");
        System.out.println("Sample from perf_orders (first 5 rows):");
        for (int i = 0; i < Math.min(5, orders.size()); i++) {
            System.out.println(orders.get(i));
        }
        if (orders.isEmpty()) {
            System.err.println("WARNING: perf_orders is empty after insertion!");
        }

        long startTime = System.currentTimeMillis();
        List<Map<String, String>> result = client.select("perf_users", "JOIN perf_orders ON perf_users.id=perf_orders.user_id", null);
        long endTime = System.currentTimeMillis();

        double totalTimeMs = (endTime - startTime);
        int resultSize = result.size();
        double avgTimePerJoinMs = resultSize > 0 ? totalTimeMs / resultSize : 0;
        double throughput = totalTimeMs > 0 ? resultSize / (totalTimeMs / 1000.0) : 0;

        System.out.println("Total time: " + totalTimeMs + " ms");
        System.out.println("Result rows: " + resultSize);
        System.out.println("Average time per join: " + String.format("%.2f", avgTimePerJoinMs) + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " joins/second");

        System.out.println("Sample join result (first 5 rows):");
        for (int i = 0; i < Math.min(5, result.size()); i++) {
            System.out.println(result.get(i));
        }

        client.delete("perf_orders", null);
    }
}