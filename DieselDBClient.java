import java.io.*;
import java.net.Socket;
import java.util.*;

public class DieselDBClient {
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public DieselDBClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String welcome = in.readLine();
        if (!welcome.startsWith("OK")) {
            throw new IOException("Failed to connect: " + welcome);
        }
        System.out.println(welcome);
    }

    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    String sendCommand(String command) throws IOException {
        Socket socket = new Socket(host, port);
        try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            out.println(command);
            String response = in.readLine();
            if (response == null) {
                throw new IOException("No response from server for command: " + command);
            }
            return response;
        } finally {
            socket.close();
        }
    }

    // Создание таблицы (поддерживает autoincrement)
    public String create(String tableName, String schemaDefinition) throws IOException {
        String command = "CREATE " + tableName;
        if (schemaDefinition != null && !schemaDefinition.isEmpty()) {
            command += " " + schemaDefinition;
        }
        return sendCommand(command);
    }

    // Вставка строки (autoincrement автоматически обрабатывается сервером)
    public String insert(String tableName, String columns, String values) throws IOException {
        return sendCommand("INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")");
    }

    // Выборка строк
    public List<Map<String, String>> select(String tableName, String condition, String orderBy) throws IOException {
        String command = "SELECT " + tableName;
        if (condition != null && !condition.isEmpty()) {
            command += " " + condition;
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            command += " ORDER BY " + orderBy;
        }
        String response = sendCommand(command);
        return parseResponse(response);
    }

    // Обновление строк
    public String update(String tableName, String condition, Map<String, String> updates) throws IOException {
        StringBuilder updateStr = new StringBuilder();
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            if (updateStr.length() > 0) updateStr.append(":::");
            updateStr.append(entry.getKey()).append(":::").append(entry.getValue());
        }
        String command = "UPDATE " + tableName + " " + condition + ";;;" + updateStr;
        return sendCommand(command);
    }

    // Удаление строк
    public String delete(String tableName, String condition) throws IOException {
        String command = "DELETE " + tableName;
        if (condition != null && !condition.isEmpty()) {
            command += " " + condition;
        }
        return sendCommand(command);
    }

    // Начать транзакцию
    public String begin() throws IOException {
        return sendCommand("BEGIN");
    }

    // Завершить транзакцию
    public String commit() throws IOException {
        return sendCommand("COMMIT");
    }

    // Откатить транзакцию
    public String rollback() throws IOException {
        return sendCommand("ROLLBACK");
    }

    // Установить уровень изоляции
    public String setIsolation(String level) throws IOException {
        return sendCommand("SET_ISOLATION " + level);
    }

    private List<Map<String, String>> parseResponse(String response) throws IOException {
        if (response == null) {
            throw new IOException("Server returned null response");
        }
        if (!response.startsWith("OK: ")) {
            throw new IOException("Server error: " + response);
        }
        String data = response.substring(4);
        if (data.equals("0 rows")) {
            return new ArrayList<>();
        }
        // Логика парсинга результата (например, split на ";;;" и ":::")
        List<Map<String, String>> result = new ArrayList<>();
        String[] rows = data.split(";;;");
        String[] headers = rows[0].split(":::");
        for (int i = 1; i < rows.length; i++) {
            String[] values = rows[i].split(":::");
            Map<String, String> row = new HashMap<>();
            for (int j = 0; j < headers.length; j++) {
                row.put(headers[j], values[j]);
            }
            result.add(row);
        }
        return result;
    }

    // Тестирование клиента
    public static void main(String[] args) throws IOException {
        DieselDBClient client = new DieselDBClient("localhost", DieselDBConfig.PORT);

        // Создаем таблицу с автоинкрементным первичным ключом
        System.out.println(client.create("users", "id:autoincrement:primary,name:string,age:integer"));

        // Вставляем строки (id автоматически увеличивается)
        System.out.println(client.insert("users", "name,age", "Alice,25"));
        System.out.println(client.insert("users", "name,age", "Bob,30"));
        System.out.println(client.insert("users", "name,age", "Charlie,35"));

        // Выборка всех строк
        List<Map<String, String>> users = client.select("users", null, "id ASC");
        System.out.println("Users:");
        for (Map<String, String> user : users) {
            System.out.println(user);
        }

        // Обновление строки
        Map<String, String> updates = new HashMap<>();
        updates.put("age", "26");
        System.out.println(client.update("users", "id=1", updates));

        // Удаление строки
        System.out.println(client.delete("users", "id=2"));

        // Выборка после изменений
        users = client.select("users", null, "id ASC");
        System.out.println("Users after update and delete:");
        for (Map<String, String> user : users) {
            System.out.println(user);
        }

        client.close();
    }
}