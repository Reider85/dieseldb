import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.file.*;

public class DieselDBServer {
    private static final int PORT = 9090;
    private static final String DELIMITER = "§§§";
    private static final String DATA_DIR = "dieseldb_data";
    private static final Map<String, List<Map<String, String>>> tables = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static volatile boolean isRunning = true;

    public static void main(String[] args) {
        // Create data directory if it doesn't exist
        File dataDir = new File(DATA_DIR);
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }

        // Load existing data on startup
        loadTablesFromFiles();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
            saveTablesToFiles();
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

    private static void loadTablesFromFiles() {
        File dataDir = new File(DATA_DIR);
        File[] tableFiles = dataDir.listFiles((dir, name) -> name.endsWith(".ddb"));
        if (tableFiles == null) return;

        ExecutorService loadExecutor = Executors.newFixedThreadPool(
                Math.min(tableFiles.length, Runtime.getRuntime().availableProcessors())
        );

        for (File file : tableFiles) {
            loadExecutor.submit(() -> {
                String tableName = file.getName().replace(".ddb", "");
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                    List<Map<String, String>> tableData = (List<Map<String, String>>) ois.readObject();
                    tables.put(tableName, new CopyOnWriteArrayList<>(tableData));
                    System.out.println("Loaded table: " + tableName);
                } catch (Exception e) {
                    System.err.println("Error loading table " + tableName + ": " + e.getMessage());
                }
            });
        }

        loadExecutor.shutdown();
        try {
            loadExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Error waiting for table loading: " + e.getMessage());
        }
    }

    private static void saveTablesToFiles() {
        ExecutorService saveExecutor = Executors.newFixedThreadPool(
                Math.min(tables.size(), Runtime.getRuntime().availableProcessors())
        );

        for (String tableName : tables.keySet()) {
            saveExecutor.submit(() -> {
                try (ObjectOutputStream oos = new ObjectOutputStream(
                        new FileOutputStream(DATA_DIR + "/" + tableName + ".ddb"))) {
                    oos.writeObject(tables.get(tableName));
                    System.out.println("Saved table: " + tableName);
                } catch (IOException e) {
                    System.err.println("Error saving table " + tableName + ": " + e.getMessage());
                }
            });
        }

        saveExecutor.shutdown();
        try {
            saveExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Error waiting for table saving: " + e.getMessage());
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

                String response;
                switch (cmd) {
                    case "CREATE":
                        response = createTable(tableName);
                        break;
                    case "INSERT":
                        response = data != null ? insertRow(tableName, data) : "ERROR: Missing data for INSERT";
                        break;
                    case "SELECT":
                        response = selectRows(tableName, data);
                        break;
                    case "UPDATE":
                        response = data != null ? updateRows(tableName, data) : "ERROR: Missing data for UPDATE";
                        break;
                    case "DELETE":
                        response = deleteRows(tableName, data);
                        break;
                    default:
                        return "ERROR: Unknown command";
                }

                // Save changes to file after modifying operations
                if (!cmd.equals("SELECT") && response.startsWith("OK")) {
                    executor.submit(() -> {
                        try (ObjectOutputStream oos = new ObjectOutputStream(
                                new FileOutputStream(DATA_DIR + "/" + tableName + ".ddb"))) {
                            oos.writeObject(tables.get(tableName));
                        } catch (IOException e) {
                            System.err.println("Error saving table " + tableName + ": " + e.getMessage());
                        }
                    });
                }
                return response;

            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }

        // [Rest of the ClientHandler methods remain unchanged: createTable, insertRow, selectRows,
        // rowMatchesCondition, updateRows, deleteRows]

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