import java.io.IOException;

public class DieselDBPerformanceTest {
    private static final String TABLE_USERS = "users";
    private static final String TABLE_ORDERS = "orders";
    private final DieselDBClient client;

    public DieselDBPerformanceTest() throws IOException {
        client = new DieselDBClient("localhost", DieselDBConfig.PORT);
    }

    private void createTables() throws IOException {
        String userSchema = "id:integer:primary,name:string:unique,age:integer";
        String orderSchema = "order_id:integer:primary,user_id:integer,amount:bigdecimal";
        System.out.println("Creating table users: " + client.create(TABLE_USERS, userSchema));
        System.out.println("Creating table orders: " + client.create(TABLE_ORDERS, orderSchema));
    }

    private void populateTables(int numRecords) throws IOException {
        long startTime = System.currentTimeMillis();
        for (int i = 1; i <= numRecords; i++) {
            String userColumns = "id, name, age";
            String userValues = i + ", User" + String.format("%05d", i) + ", " + (20 + (i % 60));
            client.insert(TABLE_USERS, userColumns, userValues);

            String orderColumns = "order_id, user_id, amount";
            String orderValues = i + ", " + i + ", " + (i % 1000) + ".00";
            client.insert(TABLE_ORDERS, orderColumns, orderValues);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Populated " + numRecords + " records in both tables in " + (endTime - startTime) + " ms");
    }

    private void testInsertPerformance(int numRecords) throws IOException {
        System.out.println("\nTesting INSERT performance for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        for (int i = numRecords + 1; i <= numRecords * 2; i++) {
            String columns = "id, name, age";
            String values = i + ", InsertTest" + String.format("%05d", i) + ", " + (20 + (i % 60));
            String response = client.insert(TABLE_USERS, columns, values);
            if (!response.startsWith("OK")) {
                System.err.println("Insert failed at row " + i + ": " + response);
                break;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Inserted " + numRecords + " records in " + (endTime - startTime) + " ms");
    }

    private void testSelectPerformance(int numRecords) throws IOException {
        System.out.println("\nTesting SELECT performance for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        String response = client.select(TABLE_USERS, "id<=" + numRecords, "id ASC");
        long endTime = System.currentTimeMillis();
        int rows = countRows(response);
        System.out.println("Selected " + rows + " records in " + (endTime - startTime) + " ms");
    }

    private void testDeletePerformance(int numRecords) throws IOException {
        System.out.println("\nTesting DELETE performance for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        String response = client.delete(TABLE_USERS, "id<=" + numRecords);
        long endTime = System.currentTimeMillis();
        System.out.println("Delete response: " + response);
        System.out.println("Deleted " + numRecords + " records in " + (endTime - startTime) + " ms");
    }

    private void testJoinPerformance(int numRecords) throws IOException {
        System.out.println("\nTesting JOIN performance for " + numRecords + " x " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        String response = client.select(TABLE_USERS, "JOIN " + TABLE_ORDERS + " id=user_id", null);
        long endTime = System.currentTimeMillis();
        int rows = countRows(response);
        System.out.println("Joined " + rows + " records in " + (endTime - startTime) + " ms");
    }

    private int countRows(String response) {
        if (response.equals("OK: 0 rows")) {
            return 0;
        }
        if (!response.startsWith("OK: ")) {
            System.err.println("Unexpected response: " + response);
            return -1;
        }
        String[] parts = response.split(";;;");
        return parts.length - 1; // Вычитаем заголовок
    }

    private void runTests() throws IOException {
        // Создаем таблицы
        createTables();

        // Тесты для 100 записей
        System.out.println("\n=== Testing with 100 records ===");
        populateTables(100);
        testInsertPerformance(100);
        testSelectPerformance(100);
        testJoinPerformance(100);
        testDeletePerformance(100);

        // Очистка перед следующим тестом
        client.delete(TABLE_USERS, null);
        client.delete(TABLE_ORDERS, null);

        // Тесты для 1000 записей
        System.out.println("\n=== Testing with 1000 records ===");
        populateTables(1000);
        testInsertPerformance(1000);
        testSelectPerformance(1000);
        testJoinPerformance(1000);
        testDeletePerformance(1000);

        // Очистка перед следующим тестом
        client.delete(TABLE_USERS, null);
        client.delete(TABLE_ORDERS, null);

        // Тесты для 10000 записей
        System.out.println("\n=== Testing with 10000 records ===");
        populateTables(10000);
        testInsertPerformance(10000);
        testSelectPerformance(10000);
        testJoinPerformance(10000);
        testDeletePerformance(10000);
    }

    public static void main(String[] args) {
        try {
            DieselDBPerformanceTest test = new DieselDBPerformanceTest();
            test.runTests();
            test.client.close();
            System.out.println("Performance tests completed successfully");
        } catch (IOException e) {
            System.err.println("Error during test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}