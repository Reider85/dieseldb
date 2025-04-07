import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DieselDBServer {
    private static final String DB_NAME = "dieseldb";
    private static final Map<String, List<Map<String, String>>> tables = new ConcurrentHashMap<>();
    private static final int PORT = 9090;
    private static final String DELIMITER = "|||";
    private static final String COL_DELIMITER = ":::";
    private static final String ROW_DELIMITER = ";;;";

    public static void main(String[] args) {
        System.out.println("Starting DieselDB server on port " + PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            ExecutorService executor = Executors.newCachedThreadPool();

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                String[] parts = inputLine.split(DELIMITER, 3);
                if (parts.length < 2) {
                    out.println("ERROR: Invalid command format");
                    continue;
                }

                String command = parts[0].toUpperCase();
                String tableName = parts[1];
                String response;

                switch (command) {
                    case "SELECT":
                        response = handleSelect(tableName, parts.length > 2 ? parts[2] : null);
                        break;
                    case "INSERT":
                        response = handleInsert(tableName, parts[2]);
                        break;
                    case "UPDATE":
                        response = handleUpdate(tableName, parts[2]);
                        break;
                    case "DELETE":
                        response = handleDelete(tableName, parts.length > 2 ? parts[2] : null);
                        break;
                    case "CREATE":
                        response = handleCreate(tableName);
                        break;
                    default:
                        response = "ERROR: Unknown command";
                }

                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("Client handling error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private static String handleCreate(String tableName) {
        tables.putIfAbsent(tableName, new CopyOnWriteArrayList<>());
        return "OK: Table " + tableName + " created";
    }

    private static String handleSelect(String tableName, String condition) {
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table not found";
        }

        List<Map<String, String>> table = tables.get(tableName);
        if (table.isEmpty()) {
            return "OK: " + ROW_DELIMITER;
        }

        StringBuilder result = new StringBuilder();
        Set<String> columns = table.get(0).keySet();

        // Build header
        result.append(String.join(COL_DELIMITER, columns)).append(ROW_DELIMITER);

        // Filter rows if condition is present
        for (Map<String, String> row : table) {
            if (condition == null || matchesCondition(row, condition)) {
                for (String col : columns) {
                    result.append(row.get(col)).append(COL_DELIMITER);
                }
                result.setLength(result.length() - COL_DELIMITER.length());
                result.append(ROW_DELIMITER);
            }
        }

        return "OK: " + result.toString();
    }

    private static boolean matchesCondition(Map<String, String> row, String condition) {
        String[] parts = condition.split("=", 2);
        if (parts.length != 2) return false;

        String column = parts[0].trim();
        String value = parts[1].trim();

        return row.containsKey(column) && row.get(column).equals(value);
    }

    private static String handleInsert(String tableName, String data) {
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table not found";
        }

        String[] columnsValues = data.split(COL_DELIMITER);
        if (columnsValues.length % 2 != 0) {
            return "ERROR: Invalid data format, expected column" + COL_DELIMITER + "value pairs";
        }

        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columnsValues.length; i += 2) {
            row.put(columnsValues[i], columnsValues[i+1]);
        }

        tables.get(tableName).add(row);
        return "OK: Row inserted";
    }

    private static String handleUpdate(String tableName, String data) {
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table not found";
        }

        String[] parts = data.split(ROW_DELIMITER, 2);
        if (parts.length != 2) {
            return "ERROR: Invalid update format, expected condition" + ROW_DELIMITER + "data";
        }

        String condition = parts[0];
        String[] columnsValues = parts[1].split(COL_DELIMITER);
        if (columnsValues.length % 2 != 0) {
            return "ERROR: Invalid data format, expected column" + COL_DELIMITER + "value pairs";
        }

        Map<String, String> updates = new HashMap<>();
        for (int i = 0; i < columnsValues.length; i += 2) {
            updates.put(columnsValues[i], columnsValues[i+1]);
        }

        int updated = 0;
        for (Map<String, String> row : tables.get(tableName)) {
            if (matchesCondition(row, condition)) {
                row.putAll(updates);
                updated++;
            }
        }

        return "OK: " + updated + " rows updated";
    }

    private static String handleDelete(String tableName, String condition) {
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table not found";
        }

        if (condition == null) {
            int size = tables.get(tableName).size();
            tables.get(tableName).clear();
            return "OK: " + size + " rows deleted";
        }

        int deleted = 0;
        Iterator<Map<String, String>> iterator = tables.get(tableName).iterator();
        while (iterator.hasNext()) {
            if (matchesCondition(iterator.next(), condition)) {
                iterator.remove();
                deleted++;
            }
        }

        return "OK: " + deleted + " rows deleted";
    }
}