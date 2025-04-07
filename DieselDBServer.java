import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;

public class DieselDBServer {
    private static final int PORT = 9090;
    private static final String DELIMITER = "§§§";
    private static final String DATA_DIR = "dieseldb_data";
    private static final Map<String, List<Map<String, Object>>> tables = new ConcurrentHashMap<>();
    private static final ExecutorService clientExecutor = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService diskExecutor = Executors.newScheduledThreadPool(1);
    private static volatile boolean isRunning = true;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final Map<String, Boolean> dirtyTables = new ConcurrentHashMap<>(); // Флаг "грязных" таблиц

    public static void main(String[] args) {
        File dataDir = new File(DATA_DIR);
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }

        // Асинхронная загрузка таблиц
        loadTablesFromFilesAsync().thenRun(() -> {
            System.out.println("All tables loaded asynchronously");
            startDiskFlushing(); // Запуск периодической записи
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
            flushAllTablesToDisk(); // Сохранение перед завершением
            clientExecutor.shutdown();
            diskExecutor.shutdown();
            System.out.println("Server shutting down...");
        }));

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            System.out.println("DieselDB server started on port " + PORT);

            while (isRunning) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setKeepAlive(true);
                    clientExecutor.submit(new ClientHandler(clientSocket));
                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Error accepting client connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        } finally {
            clientExecutor.shutdown();
            diskExecutor.shutdown();
            System.out.println("Server stopped");
        }
    }

    // Асинхронная загрузка таблиц с диска
    private static CompletableFuture<Void> loadTablesFromFilesAsync() {
        File dataDir = new File(DATA_DIR);
        File[] tableFiles = dataDir.listFiles((dir, name) -> name.endsWith(".ddb"));
        if (tableFiles == null || tableFiles.length == 0) {
            System.out.println("No tables to load from " + DATA_DIR);
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (File file : tableFiles) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                String tableName = file.getName().replace(".ddb", "");
                try {
                    byte[] data = Files.readAllBytes(file.toPath());
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                        List<Map<String, Object>> tableData = (List<Map<String, Object>>) ois.readObject();
                        tables.put(tableName, new ArrayList<>(tableData)); // Используем ArrayList вместо CopyOnWrite
                        System.out.println("Loaded table: " + tableName);
                    }
                } catch (Exception e) {
                    System.err.println("Error loading table " + tableName + ": " + e.getMessage());
                }
            }, clientExecutor);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    // Периодическая асинхронная запись на диск
    private static void startDiskFlushing() {
        diskExecutor.scheduleAtFixedRate(() -> {
            for (String tableName : dirtyTables.keySet()) {
                if (Boolean.TRUE.equals(dirtyTables.get(tableName))) {
                    saveTableToDisk(tableName);
                    dirtyTables.put(tableName, false); // Сбрасываем флаг после записи
                }
            }
        }, 5, 5, TimeUnit.SECONDS); // Запись каждые 5 секунд
    }

    // Сохранение одной таблицы на диск
    private static void saveTableToDisk(String tableName) {
        try {
            Path filePath = Paths.get(DATA_DIR, tableName + ".ddb");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(tables.get(tableName));
            }
            Files.write(filePath, baos.toByteArray(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println("Saved table asynchronously: " + tableName);
        } catch (IOException e) {
            System.err.println("Error saving table " + tableName + ": " + e.getMessage());
        }
    }

    // Принудительное сохранение всех таблиц перед завершением
    private static void flushAllTablesToDisk() {
        for (String tableName : tables.keySet()) {
            saveTableToDisk(tableName);
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
                        if (data != null && data.startsWith("JOIN")) {
                            response = handleJoin(tableName, data);
                        } else {
                            response = selectRows(tableName, data);
                        }
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

                // Отмечаем таблицу как "грязную" только для операций модификации
                if (!cmd.equals("SELECT") && response.startsWith("OK")) {
                    dirtyTables.put(tableName, true);
                }
                return response;

            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }

        private Object parseValue(String typedValue) {
            String[] parts = typedValue.split(":", 2);
            if (parts.length != 2) return parts[0];

            String type = parts[0];
            String value = parts[1];

            switch (type.toLowerCase()) {
                case "integer": return Integer.parseInt(value);
                case "bigdecimal": return new BigDecimal(value);
                case "boolean": return Boolean.parseBoolean(value);
                case "date":
                    try {
                        return dateFormat.parse(value);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Invalid date format: " + value);
                    }
                default: return value;
            }
        }

        private String formatValue(Object value) {
            if (value instanceof Date) {
                return dateFormat.format((Date) value);
            }
            return value.toString();
        }

        private String createTable(String tableName) {
            tables.putIfAbsent(tableName, new ArrayList<>());
            return "OK: Table '" + tableName + "' created";
        }

        private String insertRow(String tableName, String data) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            Map<String, Object> row = new HashMap<>();
            String[] pairs = data.split(":::");
            for (int i = 0; i < pairs.length; i += 2) {
                if (i + 1 >= pairs.length) {
                    return "ERROR: Invalid data format";
                }
                row.put(pairs[i], parseValue(pairs[i + 1]));
            }
            synchronized (tables.get(tableName)) {
                tables.get(tableName).add(row);
            }
            return "OK: 1 row inserted";
        }

        private String selectRows(String tableName, String condition) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            List<Map<String, Object>> result = new ArrayList<>();
            synchronized (tables.get(tableName)) {
                for (Map<String, Object> row : tables.get(tableName)) {
                    if (condition == null || evaluateWhereCondition(row, condition)) {
                        result.add(new LinkedHashMap<>(row));
                    }
                }
            }
            if (result.isEmpty()) {
                return "OK: 0 rows";
            }
            StringBuilder response = new StringBuilder("OK: ");
            response.append(String.join(":::", result.get(0).keySet()));
            for (Map<String, Object> row : result) {
                response.append(";;;").append(String.join(":::", row.values().stream().map(this::formatValue).toArray(String[]::new)));
            }
            return response.toString();
        }

        private boolean evaluateWhereCondition(Map<String, Object> row, String condition) {
            // Оставляем как есть для простоты, можно оптимизировать отдельно
            String[] orParts = condition.split("\\s+OR\\s+");
            for (String orPart : orParts) {
                String[] andParts = orPart.split("\\s+AND\\s+");
                boolean andResult = true;
                for (String expression : andParts) {
                    expression = expression.trim();
                    if (expression.contains(">=")) {
                        String[] parts = expression.split(">=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= compareValues(value, parseValue("string:" + parts[1].trim()), ">=");
                    } else if (expression.contains("<=")) {
                        String[] parts = expression.split("<=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= compareValues(value, parseValue("string:" + parts[1].trim()), "<=");
                    } else if (expression.contains(">")) {
                        String[] parts = expression.split(">");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= compareValues(value, parseValue("string:" + parts[1].trim()), ">");
                    } else if (expression.contains("<")) {
                        String[] parts = expression.split("<");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= compareValues(value, parseValue("string:" + parts[1].trim()), "<");
                    } else if (expression.contains("!=")) {
                        String[] parts = expression.split("!=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= !value.equals(parseValue("string:" + parts[1].trim()));
                    } else if (expression.contains("=")) {
                        String[] parts = expression.split("=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        andResult &= value.equals(parseValue("string:" + parts[1].trim()));
                    } else {
                        andResult = false;
                    }
                    if (!andResult) break;
                }
                if (andResult) return true;
            }
            return false;
        }

        private boolean compareValues(Object value1, Object value2, String operator) {
            // Оставляем как есть для простоты
            if (value1 == null || value2 == null) return false;
            if (value1 instanceof Integer && value2 instanceof Integer) {
                int v1 = (Integer) value1;
                int v2 = (Integer) value2;
                switch (operator) {
                    case ">": return v1 > v2;
                    case "<": return v1 < v2;
                    case ">=": return v1 >= v2;
                    case "<=": return v1 <= v2;
                    default: return false;
                }
            } else if (value1 instanceof BigDecimal && value2 instanceof BigDecimal) {
                BigDecimal v1 = (BigDecimal) value1;
                BigDecimal v2 = (BigDecimal) value2;
                switch (operator) {
                    case ">": return v1.compareTo(v2) > 0;
                    case "<": return v1.compareTo(v2) < 0;
                    case ">=": return v1.compareTo(v2) >= 0;
                    case "<=": return v1.compareTo(v2) <= 0;
                    default: return false;
                }
            } else if (value1 instanceof Date && value2 instanceof Date) {
                Date v1 = (Date) value1;
                Date v2 = (Date) value2;
                switch (operator) {
                    case ">": return v1.compareTo(v2) > 0;
                    case "<": return v1.compareTo(v2) < 0;
                    case ">=": return v1.compareTo(v2) >= 0;
                    case "<=": return v1.compareTo(v2) <= 0;
                    default: return false;
                }
            } else {
                String v1 = formatValue(value1);
                String v2 = formatValue(value2);
                int compare = v1.compareTo(v2);
                switch (operator) {
                    case ">": return compare > 0;
                    case "<": return compare < 0;
                    case ">=": return compare >= 0;
                    case "<=": return compare <= 0;
                    default: return false;
                }
            }
        }

        private String handleJoin(String leftTable, String joinData) {
            // Оставляем как есть, только добавляем синхронизацию
            String[] joinParts = joinData.split("§§§");
            if (joinParts.length < 3) return "ERROR: Invalid JOIN format";
            if (!joinParts[0].equals("JOIN")) return "ERROR: JOIN command must start with JOIN";
            String rightTable = joinParts[1];
            String joinCondition = joinParts[2];
            String whereCondition = joinParts.length > 3 ? joinParts[3] : null;
            if (!tables.containsKey(leftTable) || !tables.containsKey(rightTable)) {
                return "ERROR: One or both tables not found";
            }
            String[] keys = joinCondition.split("=");
            if (keys.length != 2) return "ERROR: Invalid join condition";
            String leftKey = keys[0];
            String rightKey = keys[1];

            List<Map<String, Object>> result = new ArrayList<>();
            synchronized (tables.get(leftTable)) {
                synchronized (tables.get(rightTable)) {
                    for (Map<String, Object> leftRow : tables.get(leftTable)) {
                        Object leftValue = leftRow.get(leftKey);
                        if (leftValue == null) continue;
                        for (Map<String, Object> rightRow : tables.get(rightTable)) {
                            Object rightValue = rightRow.get(rightKey);
                            if (rightValue != null && leftValue.equals(rightValue)) {
                                Map<String, Object> joinedRow = new LinkedHashMap<>();
                                leftRow.forEach((k, v) -> joinedRow.put(leftTable + "." + k, v));
                                rightRow.forEach((k, v) -> joinedRow.put(rightTable + "." + k, v));
                                if (whereCondition == null || evaluateWhereCondition(joinedRow, whereCondition)) {
                                    result.add(joinedRow);
                                }
                            }
                        }
                    }
                }
            }
            if (result.isEmpty()) return "OK: 0 rows";
            StringBuilder response = new StringBuilder("OK: ");
            response.append(String.join(":::", result.get(0).keySet()));
            for (Map<String, Object> row : result) {
                response.append(";;;").append(String.join(":::", row.values().stream().map(this::formatValue).toArray(String[]::new)));
            }
            return response.toString();
        }

        private String updateRows(String tableName, String data) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            String[] parts = data.split(";;;", 2);
            if (parts.length != 2) return "ERROR: Invalid update format";
            int updated = 0;
            String condition = parts[0];
            String[] updates = parts[1].split(":::");
            synchronized (tables.get(tableName)) {
                for (Map<String, Object> row : tables.get(tableName)) {
                    if (evaluateWhereCondition(row, condition)) {
                        for (int i = 0; i < updates.length; i += 2) {
                            if (i + 1 < updates.length) {
                                row.put(updates[i], parseValue(updates[i + 1]));
                            }
                        }
                        updated++;
                    }
                }
            }
            return "OK: " + updated;
        }

        private String deleteRows(String tableName, String condition) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            int deleted = 0;
            synchronized (tables.get(tableName)) {
                if (condition == null) {
                    deleted = tables.get(tableName).size();
                    tables.get(tableName).clear();
                } else {
                    Iterator<Map<String, Object>> it = tables.get(tableName).iterator();
                    while (it.hasNext()) {
                        if (evaluateWhereCondition(it.next(), condition)) {
                            it.remove();
                            deleted++;
                        }
                    }
                }
            }
            return "OK: " + deleted;
        }
    }
}