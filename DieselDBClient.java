import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.*;

public class DieselDBClient {
    private static final String DELIMITER = "§§§";
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private boolean connected = false;

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
            // Treat as "no rows affected" until server is fixed
            return;
        }
        if (response.startsWith("ERROR: ")) {
            throw new IOException("Server error: " + response.substring(7));
        }
        if (!response.startsWith("OK: ")) {
            throw new IOException("Unexpected response format: " + response);
        }
    }

    public void createTable(String tableName) throws IOException {
        String response = sendCommandWithRetry("CREATE" + DELIMITER + tableName);
        checkResponse(response);
    }

    public void insert(String tableName, Map<String, String> row) throws IOException {
        if (tableName == null || row == null) {
            throw new IllegalArgumentException("Table name and row data cannot be null");
        }
        if (row.isEmpty()) {
            throw new IllegalArgumentException("Row data cannot be empty");
        }

        String data = row.entrySet().stream()
                .map(e -> e.getKey() + ":::" + e.getValue())
                .collect(Collectors.joining(":::"));
        String response = sendCommandWithRetry("INSERT" + DELIMITER + tableName + DELIMITER + data);
        checkResponse(response);
    }

    public List<Map<String, String>> select(String tableName, String condition) throws IOException {
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
        List<Map<String, String>> result = new ArrayList<>();

        for (int i = 1; i < parts.length; i++) {
            String[] values = parts[i].split(":::");
            Map<String, String> row = new LinkedHashMap<>();
            for (int j = 0; j < columns.length && j < values.length; j++) {
                row.put(columns[j], values[j]);
            }
            result.add(row);
        }

        return result;
    }

    public int update(String tableName, String condition, Map<String, String> updates) throws IOException {
        if (tableName == null || condition == null || updates == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        if (updates.isEmpty()) {
            throw new IllegalArgumentException("Updates cannot be empty");
        }

        String updateData = updates.entrySet().stream()
                .map(e -> e.getKey() + ":::" + e.getValue())
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

            // Отправляем просто "PING" без разделителей
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
            // Test connection
            client.connect();
            client.ping();
            System.out.println("Server is responsive");

            // Test operations
            testDatabaseOperations(client);

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.disconnect();
        }
    }

    private static void testDatabaseOperations(DieselDBClient client) throws IOException {
        // 1. Create table
        client.createTable("users");
        System.out.println("Table 'users' created");

        // 2. Insert test data
        Map<String, String> user1 = Map.of("id", "1", "name", "Alice", "email", "alice@example.com");
        client.insert("users", user1);
        System.out.println("Inserted user 1: " + client.select("users", "id=1"));

        Map<String, String> user2 = Map.of("id", "2", "name", "Bob", "email", "bob@example.com");
        client.insert("users", user2);
        System.out.println("Inserted user 2: " + client.select("users", "id=2"));

        // 3. Select all users
        List<Map<String, String>> users = client.select("users", null);
        System.out.println("\nAll users (" + users.size() + "):");
        users.forEach(System.out::println);

        // 4. Update user
        int updated = client.update("users", "id=1", Map.of("name", "Alice Updated", "email", "alice.updated@example.com"));
        System.out.println("\nUpdated " + updated + " rows");

        // 5. Verify update
        List<Map<String, String>> alice = client.select("users", "id=1");
        System.out.println("\nUpdated Alice: " + alice);

        // 6. Delete user
        int deleted = client.delete("users", "id=2");
        System.out.println("\nDeleted " + deleted + " rows");

        // 7. Verify remaining users
        users = client.select("users", null);
        System.out.println("\nRemaining users (" + users.size() + "):");
        users.forEach(System.out::println);
    }
}