import java.io.IOException;
import java.math.BigDecimal;
import java.util.Random;

public class DieselDBPerformanceTest {
    private static final String HOST = "localhost";
    private static final int PORT = 9090;
    private static final String TABLE_USERS = "perf_users";
    private static final String TABLE_ORDERS = "perf_orders";
    private static final int NUM_USERS = 10000;
    private static final int NUM_ORDERS = 20000;
    private static final Random RANDOM = new Random();

    private final DieselDBClient client;

    public DieselDBPerformanceTest() throws IOException {
        this.client = new DieselDBClient(HOST, PORT);
    }

    public void runTests() throws IOException {
        try {
            System.out.println("Starting performance tests...");

            testCreateTables();
            testInsertPerformance();
            testSelectPerformance();
            testUpdatePerformance();
            testDeletePerformance();
            testJoinPerformance();
            testMemoryCleanup();

            System.out.println("Performance tests completed.");
        } finally {
            client.close();
        }
    }

    private void testCreateTables() throws IOException {
        long startTime = System.currentTimeMillis();
        client.create(TABLE_USERS);
        client.create(TABLE_ORDERS);
        long endTime = System.currentTimeMillis();
        System.out.println("Table creation time: " + (endTime - startTime) + " ms");
    }

    private void testInsertPerformance() throws IOException {
        System.out.println("Testing insert performance...");

        long startTime = System.currentTimeMillis();
        for (int i = 1; i <= NUM_USERS; i++) {
            String data = String.format("id:::integer:%d:::name:::string:User%d:::age:::integer:%d",
                    i, i, 18 + RANDOM.nextInt(50));
            client.insert(TABLE_USERS, data);
            if (i % 1000 == 0) {
                System.out.println("Inserted " + i + " users...");
            }
        }
        long endTimeUsers = System.currentTimeMillis();

        for (int i = 1; i <= NUM_ORDERS; i++) {
            String data = String.format("order_id:::integer:%d:::user_id:::integer:%d:::amount:::bigdecimal:%s",
                    i, 1 + RANDOM.nextInt(NUM_USERS), BigDecimal.valueOf(RANDOM.nextDouble() * 1000).setScale(2, BigDecimal.ROUND_HALF_UP));
            client.insert(TABLE_ORDERS, data);
            if (i % 1000 == 0) {
                System.out.println("Inserted " + i + " orders...");
            }
        }
        long endTimeOrders = System.currentTimeMillis();

        System.out.println("Insert " + NUM_USERS + " users time: " + (endTimeUsers - startTime) + " ms");
        System.out.println("Insert " + NUM_ORDERS + " orders time: " + (endTimeOrders - endTimeUsers) + " ms");
        System.out.println("Total insert time: " + (endTimeOrders - startTime) + " ms");
    }

    private void testSelectPerformance() throws IOException {
        System.out.println("Testing select performance...");

        long startTime = System.currentTimeMillis();
        String result1 = client.select(TABLE_USERS, "id=" + (NUM_USERS / 2), null);
        long endTime1 = System.currentTimeMillis();
        System.out.println("Select by id time: " + (endTime1 - startTime) + " ms");
        System.out.println("Result: " + result1);

        startTime = System.currentTimeMillis();
        String result2 = client.select(TABLE_USERS, "age>=30 AND age<=40", "age ASC");
        long endTime2 = System.currentTimeMillis();
        System.out.println("Select by age range with ORDER BY age ASC time: " + (endTime2 - startTime) + " ms");
        System.out.println("Result sample: " + (result2.length() > 100 ? result2.substring(0, 100) + "..." : result2));
    }

    private void testUpdatePerformance() throws IOException {
        System.out.println("Testing update performance...");

        long startTime = System.currentTimeMillis();
        client.update(TABLE_USERS, "age>25", "age:::integer:" + (25 + RANDOM.nextInt(10)));
        long endTime = System.currentTimeMillis();
        System.out.println("Update users (age > 25) time: " + (endTime - startTime) + " ms");

        String result = client.select(TABLE_USERS, "age>25", "age DESC");
        System.out.println("Updated users sample (ordered by age DESC): " + (result.length() > 100 ? result.substring(0, 100) + "..." : result));
    }

    private void testDeletePerformance() throws IOException {
        System.out.println("Testing delete performance...");

        long startTime = System.currentTimeMillis();
        client.delete(TABLE_ORDERS, "amount<500.00");
        long endTime = System.currentTimeMillis();
        System.out.println("Delete orders (amount < 500.00) time: " + (endTime - startTime) + " ms");

        String result = client.select(TABLE_ORDERS, null, "amount ASC");
        System.out.println("Remaining orders sample (ordered by amount ASC): " + (result.length() > 100 ? result.substring(0, 100) + "..." : result));
    }

    private void testJoinPerformance() throws IOException {
        System.out.println("Testing join performance...");

        long startTime = System.currentTimeMillis();
        String result = client.join(TABLE_USERS, TABLE_ORDERS, "id=user_id", "amount>100.00", "perf_users.age DESC");
        long endTime = System.currentTimeMillis();
        System.out.println("Join users and orders (amount > 100.00, ordered by age DESC) time: " + (endTime - startTime) + " ms");
        System.out.println("Join result sample: " + (result.length() > 100 ? result.substring(0, 100) + "..." : result));
    }

    private void testMemoryCleanup() throws IOException {
        System.out.println("Testing memory cleanup...");

        long startTime = System.currentTimeMillis();
        client.clearMemory();
        long endTime = System.currentTimeMillis();
        System.out.println("Memory cleanup time: " + (endTime - startTime) + " ms");

        String result = client.select(TABLE_USERS, "id=1", null);
        System.out.println("Select after cleanup: " + result);
    }

    public static void main(String[] args) {
        try {
            DieselDBPerformanceTest test = new DieselDBPerformanceTest();
            test.runTests();
        } catch (IOException e) {
            System.err.println("Performance test error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}