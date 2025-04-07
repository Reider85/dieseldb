import java.io.*;
import java.net.*;
import java.text.ParseException;
import java.util.*;
import java.util.stream.*;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;

public class DieselDBClient {
    private static final String DELIMITER = "§§§";
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private boolean connected = false;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public DieselDBClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized void connect() throws IOException {
        if (connected) return;

        socket = new Socket();
        socket.setSoTimeout(5000);
        socket.setKeepAlive(true);
        socket.connect(new InetSocketAddress(host, port), 3000);

        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        String welcome = in.readLine();
        if (welcome == null || !welcome.startsWith("OK: ")) {
            throw new IOException("Connection failed: " + (welcome != null ? welcome : "No response"));
        }

        connected = true;
        System.out.println("Connected to DieselDB server at " + host + ":" + port);
    }

    public synchronized void disconnect() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Warning: Error closing connection - " + e.getMessage());
        } finally {
            connected = false;
            out = null;
            in = null;
            socket = null;
        }
    }

    private synchronized String sendCommand(String command) throws IOException {
        if (!connected) {
            throw new IOException("Not connected to server");
        }
        try {
            out.println(command);
            out.flush();
            String response = in.readLine();
            System.out.println("Command: " + command + " | Response: " + response);
            if (response == null) {
                throw new IOException("Connection closed by server");
            }
            return response;
        } catch (SocketException e) {
            connected = false;
            throw new IOException("Connection error: " + e.getMessage(), e);
        }
    }

    private String sendCommandWithRetry(String command) throws IOException {
        final int MAX_RETRIES = 2;
        IOException lastError = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                if (!connected) {
                    connect();
                }
                return sendCommand(command);
            } catch (IOException e) {
                lastError = e;
                disconnect();

                if (attempt < MAX_RETRIES - 1) {
                    try {
                        Thread.sleep(1000 * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Operation interrupted", ie);
                    }
                }
            }
        }
        throw new IOException("Command failed after " + MAX_RETRIES + " attempts: " + lastError.getMessage(), lastError);
    }

    private void checkResponse(String response) throws IOException {
        if (response == null) {
            throw new IOException("No response from server");
        }
        if (response.equals("ERROR: null")) {
            return; // Treat as "no rows affected"
        }
        if (response.startsWith("ERROR: ")) {
            throw new IOException("Server error: " + response.substring(7));
        }
        if (!response.startsWith("OK: ")) {
            throw new IOException("Unexpected response format: " + response);
        }
    }

    private String formatValue(Object value) {
        if (value instanceof Integer) {
            return "Integer:" + value;
        } else if (value instanceof BigDecimal) {
            return "BigDecimal:" + value;
        } else if (value instanceof Boolean) {
            return "Boolean:" + value;
        } else if (value instanceof Date) {
            return "Date:" + dateFormat.format((Date) value);
        } else {
            return "String:" + value; // Default to String
        }
    }

    private Object parseValue(String value, String column) {
        // Try to infer type based on value format or column context
        try {
            if (value.matches("-?\\d+")) {
                return Integer.parseInt(value); // Integer
            } else if (value.matches("-?\\d+\\.\\d+")) {
                return new BigDecimal(value); // BigDecimal
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                return Boolean.parseBoolean(value); // Boolean
            } else if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return dateFormat.parse(value); // Date
            } else {
                return value; // String
            }
        } catch (Exception e) {
            return value; // Fallback to String if parsing fails
        }
    }

    public void createTable(String tableName) throws IOException {
        String response = sendCommandWithRetry("CREATE" + DELIMITER + tableName);
        checkResponse(response);
    }

    public void insert(String tableName, Map<String, Object> row) throws IOException {
        if (tableName == null || row == null) {
            throw new IllegalArgumentException("Table name and row data cannot be null");
        }
        if (row.isEmpty()) {
            throw new IllegalArgumentException("Row data cannot be empty");
        }

        String data = row.entrySet().stream()
                .map(e -> e.getKey() + ":::" + formatValue(e.getValue()))
                .collect(Collectors.joining(":::"));
        String response = sendCommandWithRetry("INSERT" + DELIMITER + tableName + DELIMITER + data);
        checkResponse(response);
    }

    public List<Map<String, Object>> select(String tableName, String condition) throws IOException {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }

        String command = "SELECT" + DELIMITER + tableName;
        if (condition != null && !condition.isEmpty()) {
            command += DELIMITER + condition;
        }

        String response = sendCommandWithRetry(command);
        checkResponse(response);

        String data = response.substring(4); // Remove "OK: "
        if (data.isEmpty() || data.equals("0 rows")) {
            return Collections.emptyList();
        }

        String[] parts = data.split(";;;");
        String[] columns = parts[0].split(":::");
        List<Map<String, Object>> result = new ArrayList<>();

        for (int i = 1; i < parts.length; i++) {
            String[] values = parts[i].split(":::");
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < columns.length && j < values.length; j++) {
                row.put(columns[j], parseValue(values[j], columns[j]));
            }
            result.add(row);
        }

        return result;
    }

    public int update(String tableName, String condition, Map<String, Object> updates) throws IOException {
        if (tableName == null || condition == null || updates == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        if (updates.isEmpty()) {
            throw new IllegalArgumentException("Updates cannot be empty");
        }

        String updateData = updates.entrySet().stream()
                .map(e -> e.getKey() + ":::" + formatValue(e.getValue()))
                .collect(Collectors.joining(":::"));
        String data = condition + ";;;" + updateData;
        String response = sendCommandWithRetry("UPDATE" + DELIMITER + tableName + DELIMITER + data);
        checkResponse(response);

        try {
            return Integer.parseInt(response.substring(4).trim());
        } catch (NumberFormatException e) {
            throw new IOException("Invalid server response format: " + response, e);
        }
    }

    public int delete(String tableName, String condition) throws IOException {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        String command = "DELETE" + DELIMITER + tableName;
        if (condition != null && !condition.isEmpty()) {
            command += DELIMITER + condition;
        }
        String response = sendCommandWithRetry(command);
        if (response.equals("ERROR: null")) {
            return 0; // Assume no rows deleted
        }
        checkResponse(response);
        String result = response.substring(4).trim();
        try {
            return Integer.parseInt(result);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid count in server response: " + result, e);
        }
    }

    public void ping() throws IOException {
        try {
            if (!connected) {
                connect();
            }
            out.println("PING");
            out.flush();
            String response = in.readLine();

            if (response == null) {
                throw new IOException("No response to PING");
            }
            if (!"OK: PONG".equals(response)) {
                throw new IOException("Invalid PING response: " + response);
            }
        } catch (IOException e) {
            connected = false;
            throw new IOException("PING failed: " + e.getMessage(), e);
        }
    }

    public boolean checkConnection() {
        try {
            ping();
            return true;
        } catch (IOException e) {
            System.err.println("Connection check failed: " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
        DieselDBClient client = new DieselDBClient("localhost", 9090);

        try {
            client.connect();
            client.ping();
            System.out.println("Server is responsive");

            testDatabaseOperations(client);

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            client.disconnect();
        }
    }

    private static void testDatabaseOperations(DieselDBClient client) throws IOException, ParseException {
        // 1. Create table
        client.createTable("users");
        System.out.println("Table 'users' created");

        // 2. Insert test data with different types
        Map<String, Object> user1 = new LinkedHashMap<>();
        user1.put("id", 1); // Integer
        user1.put("name", "Alice"); // String
        user1.put("age", 25); // Integer
        user1.put("balance", new BigDecimal("123.45")); // BigDecimal
        user1.put("active", true); // Boolean
        user1.put("birthdate", dateFormat.parse("1998-05-20")); // Date
        client.insert("users", user1);
        System.out.println("Inserted user 1: " + client.select("users", "id=1"));

        Map<String, Object> user2 = new LinkedHashMap<>();
        user2.put("id", 2);
        user2.put("name", "Bob");
        user2.put("age", 30);
        user2.put("balance", new BigDecimal("567.89"));
        user2.put("active", false);
        user2.put("birthdate", dateFormat.parse("1993-08-15"));
        client.insert("users", user2);
        System.out.println("Inserted user 2: " + client.select("users", "id=2"));

        // 3. Select all users
        List<Map<String, Object>> users = client.select("users", null);
        System.out.println("\nAll users (" + users.size() + "):");
        users.forEach(System.out::println);

        // 4. Select with condition
        List<Map<String, Object>> activeUsers = client.select("users", "age>25 AND active=false");
        System.out.println("\nActive users with age > 25 (" + activeUsers.size() + "):");
        activeUsers.forEach(System.out::println);

        // 5. Update user
        Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("name", "Alice Updated");
        updates.put("age", 26);
        updates.put("balance", new BigDecimal("150.00"));
        int updated = client.update("users", "id=1", updates);
        System.out.println("\nUpdated " + updated + " rows");

        // 6. Verify update
        List<Map<String, Object>> alice = client.select("users", "id=1");
        System.out.println("\nUpdated Alice: " + alice);

        // 7. Delete user
        int deleted = client.delete("users", "id=2");
        System.out.println("\nDeleted " + deleted + " rows");

        // 8. Verify remaining users
        users = client.select("users", null);
        System.out.println("\nRemaining users (" + users.size() + "):");
        users.forEach(System.out::println);
    }
}