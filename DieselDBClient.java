import java.io.*;
import java.net.*;
import java.util.*;

public class DieselDBClient {
    private static final String DELIMITER = "|||";
    private static final String COL_DELIMITER = ":::";
    private static final String ROW_DELIMITER = ";;;";

    private final String host;
    private final int port;

    public DieselDBClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void createTable(String tableName) throws IOException {
        sendCommand("CREATE" + DELIMITER + tableName);
    }

    public List<Map<String, String>> select(String tableName, String condition) throws IOException {
        String cmd = "SELECT" + DELIMITER + tableName;
        if (condition != null && !condition.isEmpty()) {
            cmd += DELIMITER + condition;
        }

        String response = sendCommand(cmd);
        if (!response.startsWith("OK: ")) {
            throw new IOException(response);
        }

        String data = response.substring(4);
        if (data.isEmpty() || data.equals(ROW_DELIMITER)) {
            return Collections.emptyList();
        }

        String[] rows = data.split(ROW_DELIMITER);
        if (rows.length < 2) {
            return Collections.emptyList();
        }

        String[] columns = rows[0].split(COL_DELIMITER);
        List<Map<String, String>> result = new ArrayList<>();

        for (int i = 1; i < rows.length; i++) {
            String[] values = rows[i].split(COL_DELIMITER);
            if (values.length != columns.length) continue;

            Map<String, String> row = new LinkedHashMap<>();
            for (int j = 0; j < columns.length; j++) {
                row.put(columns[j], values[j]);
            }
            result.add(row);
        }

        return result;
    }

    public void insert(String tableName, Map<String, String> row) throws IOException {
        StringBuilder data = new StringBuilder();
        for (Map.Entry<String, String> entry : row.entrySet()) {
            data.append(entry.getKey()).append(COL_DELIMITER).append(entry.getValue()).append(COL_DELIMITER);
        }
        if (data.length() > 0) {
            data.setLength(data.length() - COL_DELIMITER.length());
        }

        String response = sendCommand("INSERT" + DELIMITER + tableName + DELIMITER + data);
        if (!response.startsWith("OK: ")) {
            throw new IOException(response);
        }
    }

    public int update(String tableName, String condition, Map<String, String> updates) throws IOException {
        StringBuilder data = new StringBuilder(condition).append(ROW_DELIMITER);
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            data.append(entry.getKey()).append(COL_DELIMITER).append(entry.getValue()).append(COL_DELIMITER);
        }
        if (data.length() > condition.length() + ROW_DELIMITER.length()) {
            data.setLength(data.length() - COL_DELIMITER.length());
        }

        String response = sendCommand("UPDATE" + DELIMITER + tableName + DELIMITER + data);
        if (!response.startsWith("OK: ")) {
            throw new IOException(response);
        }

        return Integer.parseInt(response.substring(4).split(" ")[0]);
    }

    public int delete(String tableName, String condition) throws IOException {
        String cmd = "DELETE" + DELIMITER + tableName;
        if (condition != null && !condition.isEmpty()) {
            cmd += DELIMITER + condition;
        }

        String response = sendCommand(cmd);
        if (!response.startsWith("OK: ")) {
            throw new IOException(response);
        }

        return Integer.parseInt(response.substring(4).split(" ")[0]);
    }

    private String sendCommand(String command) throws IOException {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(command);
            return in.readLine();
        }
    }

    public static void main(String[] args) {
        DieselDBClient client = new DieselDBClient("localhost", 9090);

        try {
            // Пример использования клиента
            client.createTable("users");

            // Вставка данных
            Map<String, String> user1 = new HashMap<>();
            user1.put("id", "1");
            user1.put("name", "Alice");
            user1.put("email", "alice@example.com");
            client.insert("users", user1);

            Map<String, String> user2 = new HashMap<>();
            user2.put("id", "2");
            user2.put("name", "Bob");
            user2.put("email", "bob@example.com");
            client.insert("users", user2);

            // Выборка данных
            List<Map<String, String>> users = client.select("users", null);
            System.out.println("All users:");
            users.forEach(System.out::println);

            // Обновление данных
            Map<String, String> updates = new HashMap<>();
            updates.put("email", "bob.new@example.com");
            int updated = client.update("users", "id=2", updates);
            System.out.println("Updated " + updated + " rows");

            // Проверка обновления
            List<Map<String, String>> bob = client.select("users", "id=2");
            System.out.println("Bob after update:");
            bob.forEach(System.out::println);

            // Удаление данных
            int deleted = client.delete("users", "id=1");
            System.out.println("Deleted " + deleted + " rows");

            // Проверка удаления
            users = client.select("users", null);
            System.out.println("Users after deletion:");
            users.forEach(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}