import java.io.IOException;
import java.math.BigDecimal;

public class DieselDBEnhancedPerformanceTest {
    private static final String TABLE_USERS = "users";
    private static final String TABLE_ORDERS = "orders";
    private final DieselDBClient client;

    public DieselDBEnhancedPerformanceTest() throws IOException {
        client = new DieselDBClient("localhost", DieselDBConfig.PORT);
    }

    private void createTables() throws IOException {
        // Попытка удалить данные из таблиц
        String deleteUsers = client.delete(TABLE_USERS, null);
        String deleteOrders = client.delete(TABLE_ORDERS, null);
        System.out.println("Cleanup: Deleting users table data: " + deleteUsers);
        System.out.println("Cleanup: Deleting orders table data: " + deleteOrders);

        // Создание таблиц, игнорируя "already exists"
        String userSchema = "id:integer:primary,name:string:unique,age:integer";
        String orderSchema = "order_id:integer:primary,user_id:integer,amount:bigdecimal";
        String userResponse = client.create(TABLE_USERS, userSchema);
        String orderResponse = client.create(TABLE_ORDERS, orderSchema);
        System.out.println("Creating table users: " + userResponse);
        System.out.println("Creating table orders: " + orderResponse);

        if (!userResponse.startsWith("OK") && !userResponse.contains("Table already exists")) {
            throw new AssertionError("Table creation failed for users: " + userResponse);
        }
        if (!orderResponse.startsWith("OK") && !orderResponse.contains("Table already exists")) {
            throw new AssertionError("Table creation failed for orders: " + orderResponse);
        }
    }



    private void populateTables(int numRecords) throws IOException {
        long startTime = System.currentTimeMillis();
        for (int i = 1; i <= numRecords; i++) {
            String userValues = i + ", User" + String.format("%05d", i) + ", " + (20 + (i % 60));
            String orderValues = i + ", " + i + ", " + (i % 1000) + ".00";
            client.insert(TABLE_USERS, "id, name, age", userValues);
            client.insert(TABLE_ORDERS, "order_id, user_id, amount", orderValues);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Populated " + numRecords + " records in " + (endTime - startTime) + " ms");
    }

    private void testBasicInsert(int numRecords) throws IOException {
        System.out.println("\nTesting basic INSERT for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        for (int i = numRecords + 1; i <= numRecords * 2; i++) {
            String values = i + ", InsertTest" + String.format("%05d", i) + ", " + (20 + (i % 60));
            String response = client.insert(TABLE_USERS, "id, name, age", values);
            assertResponseOK(response, "Insert failed at row " + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Inserted " + numRecords + " records in " + (endTime - startTime) + " ms");
    }

    private void testUniqueConstraints() throws IOException {
        System.out.println("\nTesting unique constraints...");
        String duplicateInsert = client.insert(TABLE_USERS, "id, name, age", "1, User00001, 30");
        if (!duplicateInsert.contains("ERROR: Duplicate")) {
            throw new AssertionError("Unique constraint violation not detected: " + duplicateInsert);
        }
        System.out.println("Unique constraint test passed: " + duplicateInsert);
    }

    private void testUpdate(int numRecords) throws IOException {
        System.out.println("\nTesting UPDATE for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        String response = client.update(TABLE_USERS, "age>30", "age:::integer:35");
        long endTime = System.currentTimeMillis();
        assertResponseOK(response, "Update failed");
        int updated = extractCount(response);
        System.out.println("Updated " + updated + " records in " + (endTime - startTime) + " ms");
    }
    private void testSelectWithWhereAndOrder(int numRecords) throws IOException {
        System.out.println("\nTesting SELECT with WHERE and ORDER BY...");
        long startTime = System.currentTimeMillis();
        String response = client.select(TABLE_USERS, "age>=25", "id ASC");
        long endTime = System.currentTimeMillis();
        int rows = countRows(response);
        System.out.println("Selected " + rows + " rows in " + (endTime - startTime) + " ms");
        assertResponseOK(response, "Select with WHERE and ORDER BY failed");
        verifyOrder(response, "id", true);
    }

    private void testJoinPerformance(int numRecords) throws IOException {
        System.out.println("\nTesting JOIN performance for " + numRecords + " records...");
        long startTime = System.currentTimeMillis();
        String response = client.select(TABLE_USERS, "JOIN " + TABLE_ORDERS + " id=user_id", null);
        long endTime = System.currentTimeMillis();
        int rows = countRows(response);
        System.out.println("Joined " + rows + " records in " + (endTime - startTime) + " ms");
        assertResponseOK(response, "Join failed");
        verifyJoin(response, "users.id", "orders.user_id");
    }

    private void testTransactions() throws IOException {
        System.out.println("\nTesting transactions with all isolation levels...");
        String[] isolationLevels = {"READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"};

        for (String level : isolationLevels) {
            System.out.println("Testing " + level + "...");
            String setResponse = client.setIsolation(level);
            assertResponseOK(setResponse, "Failed to set isolation level " + level);

            // Начало транзакции
            String beginResponse = client.begin();
            assertResponseOK(beginResponse, "Begin transaction failed for " + level);

            // Вставка в транзакции
            String insertResponse = client.insert(TABLE_USERS, "id, name, age", "9999, TransTest, 40");
            assertResponseOK(insertResponse, "Insert in transaction failed for " + level);

            // Проверка видимости в другой сессии
            DieselDBClient otherClient = new DieselDBClient("localhost", DieselDBConfig.PORT);
            String selectBefore = otherClient.select(TABLE_USERS, "id=9999", null);
            if (level.equals("READ_UNCOMMITTED")) {
                if (countRows(selectBefore) != 1) {
                    throw new AssertionError("READ_UNCOMMITTED should see uncommitted data");
                }
            } else {
                if (countRows(selectBefore) != 0) {
                    throw new AssertionError(level + " should not see uncommitted data");
                }
            }
            otherClient.close();

            // Откат транзакции
            String rollbackResponse = client.rollback();
            assertResponseOK(rollbackResponse, "Rollback failed for " + level);

            // Проверка после отката
            String selectAfter = client.select(TABLE_USERS, "id=9999", null);
            if (countRows(selectAfter) != 0) {
                throw new AssertionError("Rollback failed - data persists for " + level);
            }

            // Новая транзакция с коммитом
            client.begin();
            client.insert(TABLE_USERS, "id, name, age", "9999, TransTest, 40");
            String commitResponse = client.commit();
            assertResponseOK(commitResponse, "Commit failed for " + level);

            String finalSelect = client.select(TABLE_USERS, "id=9999", null);
            if (countRows(finalSelect) != 1) {
                throw new AssertionError("Commit failed - data not persisted for " + level);
            }
            client.delete(TABLE_USERS, "id=9999"); // Очистка
        }
        System.out.println("All transaction tests passed");
    }

    private void runEnhancedTests() throws IOException {
        // Убедимся, что начнем с чистого состояния
        createTables();

        int[] testSizes = {100, 1000, 10000};
        for (int size : testSizes) {
            System.out.println("\n=== Testing with " + size + " records ===");
            populateTables(size);

            testBasicInsert(size);
            testUniqueConstraints();
            testUpdate(size);
            testSelectWithWhereAndOrder(size);
            testJoinPerformance(size);

            // Очистка перед следующим тестом
            client.delete(TABLE_USERS, null);
            client.delete(TABLE_ORDERS, null);
        }

        testTransactions();
    }

    private void assertResponseOK(String response, String errorMessage) {
        if (!response.startsWith("OK")) {
            throw new AssertionError(errorMessage + ": " + response);
        }
    }

    private int extractCount(String response) {
        if (response.equals("OK: 0 rows") || response.equals("OK: 0")) return 0;
        if (!response.startsWith("OK: ")) {
            throw new AssertionError("Invalid response: " + response);
        }
        String[] parts = response.split("\\s+");
        return Integer.parseInt(parts[1]);
    }
    private int countRows(String response) {
        if (response.equals("OK: 0 rows")) return 0;
        if (!response.startsWith("OK: ")) return -1;
        String[] parts = response.split(";;;");
        return parts.length - 1; // Вычитаем заголовок
    }

    private void verifyOrder(String response, String column, boolean ascending) throws IOException {
        String[] parts = response.split(";;;");
        if (parts.length <= 1) return;

        int colIndex = -1;
        String[] headers = parts[0].replace("OK: ", "").split(":::");
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(column)) {
                colIndex = i;
                break;
            }
        }
        if (colIndex == -1) throw new AssertionError("Column " + column + " not found");

        Object prev = null;
        for (int i = 1; i < parts.length; i++) {
            String[] values = parts[i].split(":::");
            Object current = parseComparable(values[colIndex]);
            if (prev != null) {
                int comparison;
                if (prev instanceof Integer && current instanceof Integer) {
                    comparison = ((Integer) prev).compareTo((Integer) current);
                } else if (prev instanceof BigDecimal && current instanceof BigDecimal) {
                    comparison = ((BigDecimal) prev).compareTo((BigDecimal) current);
                } else {
                    // Приведение к строке как fallback
                    String prevStr = String.valueOf(prev);
                    String currStr = String.valueOf(current);
                    comparison = prevStr.compareTo(currStr);
                }
                if (ascending && comparison > 0) {
                    throw new AssertionError("ORDER BY ASC failed at row " + i);
                } else if (!ascending && comparison < 0) {
                    throw new AssertionError("ORDER BY DESC failed at row " + i);
                }
            }
            prev = current;
        }
    }

    private Object parseComparable(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e2) {
                return value; // Возвращаем строку как есть
            }
        }
    }
    private void verifyJoin(String response, String leftKey, String rightKey) {
        String[] parts = response.split(";;;");
        if (parts.length <= 1) return;

        int leftIndex = -1, rightIndex = -1;
        String[] headers = parts[0].replace("OK: ", "").split(":::");
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(leftKey)) leftIndex = i;
            if (headers[i].equals(rightKey)) rightIndex = i;
        }
        if (leftIndex == -1 || rightIndex == -1) {
            throw new AssertionError("Join keys not found in result");
        }

        for (int i = 1; i < parts.length; i++) {
            String[] values = parts[i].split(":::");
            if (!values[leftIndex].equals(values[rightIndex])) {
                throw new AssertionError("Join condition failed at row " + i + ": " + values[leftIndex] + " != " + values[rightIndex]);
            }
        }
    }

    public static void main(String[] args) {
        try {
            DieselDBEnhancedPerformanceTest test = new DieselDBEnhancedPerformanceTest();
            test.runEnhancedTests();
            test.client.close();
            System.out.println("All enhanced performance tests completed successfully");
        } catch (IOException e) {
            System.err.println("Error during test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}