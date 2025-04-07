import java.io.IOException;

public class DieselDBPerformanceTest {
    private static final String TABLE_ORDERS = "orders";
    private final DieselDBClient client;

    public DieselDBPerformanceTest() throws IOException {
        client = new DieselDBClient("localhost", 9090);
    }

    private void populateLargeTable() throws IOException {
        System.out.println("Creating table: " + client.create(TABLE_ORDERS));
        long startTime = System.currentTimeMillis();
        for (int i = 1; i <= 20000; i++) {
            String response = client.insert(TABLE_ORDERS, "order_id:::integer:" + i + ":::amount:::bigdecimal:" + (i % 1000) + ".00");
            if (!response.startsWith("OK")) {
                System.err.println("Insert failed at row " + i + ": " + response);
                break;
            }
            if (i % 5000 == 0) {
                System.out.println("Inserted " + i + " rows...");
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Populated table with 20,000 rows in " + (endTime - startTime) + " ms");
    }

    private void testDeletePerformance() throws IOException, InterruptedException {
        System.out.println("Testing delete performance...");

        // Проверяем исходное количество строк
        String initialSelect = client.select(TABLE_ORDERS, null, "amount ASC");
        int initialRows = countRows(initialSelect);
        System.out.println("Initial row count: " + initialRows);

        // Выполняем DELETE и замеряем время
        long startTime = System.currentTimeMillis();
        String deleteResponse = client.delete(TABLE_ORDERS, "amount<500.00");
        long endTime = System.currentTimeMillis();
        System.out.println("Delete response: " + deleteResponse);
        System.out.println("Delete orders (amount < 500.00) response time: " + (endTime - startTime) + " ms");

        // Проверяем результат сразу после DELETE
        String immediateSelect = client.select(TABLE_ORDERS, null, "amount ASC");
        int immediateRows = countRows(immediateSelect);
        System.out.println("Rows remaining immediately after delete: " + immediateRows);
        System.out.println("Sample of remaining orders: " +
                (immediateSelect.length() > 100 ? immediateSelect.substring(0, 100) + "..." : immediateSelect));

        // Ждём завершения асинхронной очистки (5-6 секунд)
        System.out.println("Waiting 6 seconds for index cleanup...");
        Thread.sleep(6000);

        // Проверяем результат после очистки
        String finalSelect = client.select(TABLE_ORDERS, null, "amount ASC");
        int finalRows = countRows(finalSelect);
        System.out.println("Rows remaining after cleanup: " + finalRows);
        System.out.println("Sample of remaining orders after cleanup: " +
                (finalSelect.length() > 100 ? finalSelect.substring(0, 100) + "..." : finalSelect));
    }

    private int countRows(String selectResponse) {
        if (selectResponse.equals("OK: 0 rows")) {
            return 0;
        }
        if (!selectResponse.startsWith("OK: ")) {
            System.err.println("Unexpected select response: " + selectResponse);
            return -1;
        }
        String[] parts = selectResponse.split(";;;");
        return parts.length - 1; // Вычитаем заголовок
    }

    public static void main(String[] args) {
        try {
            DieselDBPerformanceTest test = new DieselDBPerformanceTest();
            test.populateLargeTable();
            test.testDeletePerformance();
            test.client.close();
            System.out.println("Test completed successfully");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error during test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}