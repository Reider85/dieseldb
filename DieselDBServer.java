import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DieselDBServer {
    private static final int PORT = 9090;
    private static final String DELIMITER = "§§§";
    private static final Map<String, List<Map<String, String>>> tables = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static volatile boolean isRunning = true;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
            executor.shutdown();
            System.out.println("Server shutting down...");
        }));

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            System.out.println("DieselDB server started on port " + PORT);

            while (isRunning) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setKeepAlive(true);
                    executor.submit(new ClientHandler(clientSocket));
                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Error accepting client connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        } finally {
            executor.shutdown();
            System.out.println("Server stopped");
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                out.println("OK: Welcome to DieselDB Server");
                out.flush();

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    System.out.println("Processing command: " + inputLine);
                    String response = processCommand(inputLine);
                    out.println(response);
                    out.flush();
                }
            } catch (SocketException e) {
                System.out.println("Client disconnected: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Client error: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }

        private String processCommand(String command) {
            try {
                // Special case for PING command
                if ("PING".equalsIgnoreCase(command.trim())) {
                    return "OK: PONG";
                }

                String[] parts = command.split(DELIMITER, 3);
                if (parts.length < 2) {
                    return "ERROR: Invalid command format. Use COMMAND§§§TABLE§§§DATA or PING";
                }

                String cmd = parts[0].toUpperCase();
                String tableName = parts[1];
                String data = parts.length > 2 ? parts[2] : null;

                switch (cmd) {
                    case "CREATE":
                        return createTable(tableName);
                    case "INSERT":
                        return data != null ? insertRow(tableName, data) : "ERROR: Missing data for INSERT";
                    case "SELECT":
                        return selectRows(tableName, data);
                    case "UPDATE":
                        return data != null ? updateRows(tableName, data) : "ERROR: Missing data for UPDATE";
                    case "DELETE":
                        return deleteRows(tableName, data);
                    default:
                        return "ERROR: Unknown command";
                }
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }

        private String createTable(String tableName) {
            tables.putIfAbsent(tableName, new CopyOnWriteArrayList<>());
            return "OK: Table '" + tableName + "' created";
        }

        private String insertRow(String tableName, String data) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            try {
                Map<String, String> row = new HashMap<>();
                String[] pairs = data.split(":::");
                for (int i = 0; i < pairs.length; i += 2) {
                    if (i + 1 >= pairs.length) {
                        return "ERROR: Invalid data format. Use key1:::value1:::key2:::value2";
                    }
                    row.put(pairs[i], pairs[i+1]);
                }

                tables.get(tableName).add(row);
                return "OK: 1 row inserted";
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }

        private String selectRows(String tableName, String condition) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            List<Map<String, String>> result = new ArrayList<>();
            for (Map<String, String> row : tables.get(tableName)) {
                if (condition == null || rowMatchesCondition(row, condition)) {
                    result.add(new LinkedHashMap<>(row));
                }
            }

            if (result.isEmpty()) {
                return "OK: 0 rows";
            }

            StringBuilder response = new StringBuilder("OK: ");
            response.append(String.join(":::", result.get(0).keySet()));
            for (Map<String, String> row : result) {
                response.append(";;;").append(String.join(":::", row.values()));
            }
            return response.toString();
        }

        private boolean rowMatchesCondition(Map<String, String> row, String condition) {
            String[] parts = condition.split("=", 2);
            if (parts.length != 2) return false;
            return row.getOrDefault(parts[0], "").equals(parts[1]);
        }

        private String updateRows(String tableName, String data) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            String[] parts = data.split(";;;", 2);
            if (parts.length != 2) {
                return "ERROR: Invalid update format. Use condition;;;key1:::value1:::key2:::value2";
            }

            int updated = 0;
            String condition = parts[0];
            String[] updates = parts[1].split(":::");

            for (Map<String, String> row : tables.get(tableName)) {
                if (rowMatchesCondition(row, condition)) {
                    for (int i = 0; i < updates.length; i += 2) {
                        if (i + 1 < updates.length) {
                            row.put(updates[i], updates[i+1]);
                        }
                    }
                    updated++;
                }
            }
            return "OK: " + updated;
        }

        private String deleteRows(String tableName, String condition) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            if (condition == null) {
                int count = tables.get(tableName).size();
                tables.get(tableName).clear();
                return "OK: " + count;
            }

            int deleted = 0;
            Iterator<Map<String, String>> it = tables.get(tableName).iterator();
            while (it.hasNext()) {
                if (rowMatchesCondition(it.next(), condition)) {
                    it.remove();
                    deleted++;
                }
            }
            return "OK: " + deleted;
        }
    }
}