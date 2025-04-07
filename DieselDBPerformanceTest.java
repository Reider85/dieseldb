import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DieselDBPerformanceTest {
    private static final int RECORD_COUNT = 1_000_000;
    private static final int BATCH_SIZE = 10_000;
    private static final String TABLE_NAME = "perf_test";

    public static void main(String[] args) {
        DieselDBClient client = new DieselDBClient("localhost", 9090);

        try {
            // Создаем таблицу
            client.createTable(TABLE_NAME);
            System.out.println("Table created");

            // Тест вставки
            testInsert(client);

            // Тест выборки
            testSelect(client);

            // Тест обновления
            testUpdate(client);

            // Тест удаления
            testDelete(client);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testInsert(DieselDBClient client) throws IOException {
        System.out.println("Starting insert performance test...");
        long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < RECORD_COUNT; i += BATCH_SIZE) {
            final int batchStart = i;
            futures.add(executor.submit(() -> {
                try {
                    for (int j = batchStart; j < batchStart + BATCH_SIZE && j < RECORD_COUNT; j++) {
                        Map<String, String> record = new HashMap<>();
                        record.put("id", String.valueOf(j));
                        record.put("name", "User_" + j);
                        record.put("email", "user_" + j + "@example.com");
                        record.put("data", UUID.randomUUID().toString());
                        client.insert(TABLE_NAME, record);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));
        }

        // Ожидаем завершения всех задач
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double recordsPerSec = RECORD_COUNT / duration;

        System.out.printf("Inserted %,d records in %.2f seconds (%.2f records/sec)%n",
                RECORD_COUNT, duration, recordsPerSec);
    }

    private static void testSelect(DieselDBClient client) throws IOException {
        System.out.println("Starting select performance test...");

        // Тест выборки всех записей
        long startTime = System.currentTimeMillis();
        List<Map<String, String>> allRecords = client.select(TABLE_NAME, null);
        long endTime = System.currentTimeMillis();

        System.out.printf("Selected all %,d records in %d ms%n",
                allRecords.size(), (endTime - startTime));

        // Тест выборки по условию
        startTime = System.currentTimeMillis();
        List<Map<String, String>> filteredRecords = client.select(TABLE_NAME, "id=500000");
        endTime = System.currentTimeMillis();

        System.out.printf("Selected 1 record with condition in %d ms%n", (endTime - startTime));
        System.out.println("Sample record: " + filteredRecords.get(0));
    }

    private static void testUpdate(DieselDBClient client) throws IOException {
        System.out.println("Starting update performance test...");
        long startTime = System.currentTimeMillis();

        Map<String, String> updates = new HashMap<>();
        updates.put("name", "Updated_Name");
        updates.put("email", "updated@example.com");

        int updated = client.update(TABLE_NAME, "id>=" + (RECORD_COUNT/2), updates);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double recordsPerSec = updated / duration;

        System.out.printf("Updated %,d records in %.2f seconds (%.2f records/sec)%n",
                updated, duration, recordsPerSec);
    }

    private static void testDelete(DieselDBClient client) throws IOException {
        System.out.println("Starting delete performance test...");
        long startTime = System.currentTimeMillis();

        int deleted = client.delete(TABLE_NAME, null);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        double recordsPerSec = deleted / duration;

        System.out.printf("Deleted %,d records in %.2f seconds (%.2f records/sec)%n",
                deleted, duration, recordsPerSec);
    }
}