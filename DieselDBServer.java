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
    private static final Map<String, Long> lastAccessTimes = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> dirtyTables = new ConcurrentHashMap<>();
    private static final ExecutorService clientExecutor = Executors.newCachedThreadPool();
    private static final ScheduledExecutorService diskExecutor = Executors.newScheduledThreadPool(2);
    private static volatile boolean isRunning = true;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final long MEMORY_THRESHOLD = 512 * 1024 * 1024; // 512 MB
    private static final long INACTIVE_TIMEOUT = 30_000; // 30 секунд

    private static final Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        File dataDir = new File(DATA_DIR);
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }

        loadTablesFromFilesAsync().thenRun(() -> {
            System.out.println("All tables loaded asynchronously");
            startDiskFlushing();
            startMemoryCleanup();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false;
            flushAllTablesToDisk();
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
                        tables.put(tableName, new ArrayList<>(tableData));
                        lastAccessTimes.put(tableName, System.currentTimeMillis());
                        rebuildIndexes(tableName);
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

    private static void startDiskFlushing() {
        diskExecutor.scheduleAtFixedRate(() -> {
            for (String tableName : dirtyTables.keySet()) {
                if (Boolean.TRUE.equals(dirtyTables.get(tableName))) {
                    saveTableToDisk(tableName);
                    dirtyTables.put(tableName, false);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private static void startMemoryCleanup() {
        diskExecutor.scheduleAtFixedRate(() -> {
            long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            if (usedMemory > MEMORY_THRESHOLD) {
                System.out.println("Memory usage exceeded threshold: " + usedMemory / (1024 * 1024) + " MB");
                cleanupInactiveTables();
                System.gc();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private static void cleanupInactiveTables() {
        long currentTime = System.currentTimeMillis();
        for (String tableName : tables.keySet()) {
            long lastAccess = lastAccessTimes.getOrDefault(tableName, 0L);
            if (currentTime - lastAccess > INACTIVE_TIMEOUT) {
                synchronized (tables.get(tableName)) {
                    if (dirtyTables.getOrDefault(tableName, false)) {
                        saveTableToDisk(tableName);
                        dirtyTables.put(tableName, false);
                    }
                    tables.remove(tableName);
                    hashIndexes.remove(tableName);
                    btreeIndexes.remove(tableName);
                    lastAccessTimes.remove(tableName);
                    System.out.println("Unloaded inactive table from memory: " + tableName);
                }
            }
        }
    }

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

    private static void flushAllTablesToDisk() {
        for (String tableName : tables.keySet()) {
            saveTableToDisk(tableName);
        }
    }

    private static void rebuildIndexes(String tableName) {
        hashIndexes.putIfAbsent(tableName, new HashMap<>());
        btreeIndexes.putIfAbsent(tableName, new HashMap<>());
        List<Map<String, Object>> table = tables.get(tableName);
        if (table == null) return;

        synchronized (table) {
            for (Map<String, Object> row : table) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String column = entry.getKey();
                    Object value = entry.getValue();
                    hashIndexes.get(tableName).computeIfAbsent(column, k -> new HashMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                    btreeIndexes.get(tableName).computeIfAbsent(column, k -> new TreeMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                }
            }
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private boolean inTransaction = false;
        private Map<String, List<Map<String, Object>>> transactionTables; // Буфер изменений
        private Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> transactionHashIndexes;
        private Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> transactionBtreeIndexes;

        ClientHandler(Socket socket) {
            this.clientSocket = socket;
            this.transactionTables = new HashMap<>();
            this.transactionHashIndexes = new HashMap<>();
            this.transactionBtreeIndexes = new HashMap<>();
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
                if (inTransaction) {
                    rollbackTransaction();
                }
            } catch (IOException e) {
                System.err.println("Client error: " + e.getMessage());
                if (inTransaction) {
                    rollbackTransaction();
                }
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
                if ("CLEAR_MEMORY".equalsIgnoreCase(command.trim())) {
                    if (inTransaction) {
                        return "ERROR: Cannot clear memory during transaction";
                    }
                    cleanupInactiveTables();
                    System.gc();
                    return "OK: Memory cleanup triggered";
                }
                if ("BEGIN".equalsIgnoreCase(command.trim())) {
                    if (inTransaction) {
                        return "ERROR: Transaction already in progress";
                    }
                    beginTransaction();
                    return "OK: Transaction started";
                }
                if ("COMMIT".equalsIgnoreCase(command.trim())) {
                    if (!inTransaction) {
                        return "ERROR: No transaction in progress";
                    }
                    commitTransaction();
                    return "OK: Transaction committed";
                }
                if ("ROLLBACK".equalsIgnoreCase(command.trim())) {
                    if (!inTransaction) {
                        return "ERROR: No transaction in progress";
                    }
                    rollbackTransaction();
                    return "OK: Transaction rolled back";
                }

                String[] parts = command.split(DELIMITER, 3);
                if (parts.length < 2) {
                    return "ERROR: Invalid command format. Use COMMAND§§§TABLE§§§DATA or PING or CLEAR_MEMORY or BEGIN/COMMIT/ROLLBACK";
                }

                String cmd = parts[0].toUpperCase();
                String tableName = parts[1];
                String data = parts.length > 2 ? parts[2] : null;

                if (!inTransaction && !tables.containsKey(tableName) && !cmd.equals("CREATE")) {
                    loadTableFromDisk(tableName);
                }

                lastAccessTimes.put(tableName, System.currentTimeMillis());

                Map<String, List<Map<String, Object>>> workingTables = inTransaction ? transactionTables : tables;
                Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> workingHashIndexes = inTransaction ? transactionHashIndexes : hashIndexes;
                Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> workingBtreeIndexes = inTransaction ? transactionBtreeIndexes : btreeIndexes;

                String response;
                switch (cmd) {
                    case "CREATE":
                        response = createTable(tableName, workingTables, workingHashIndexes, workingBtreeIndexes);
                        break;
                    case "INSERT":
                        response = data != null ? insertRow(tableName, data, workingTables, workingHashIndexes, workingBtreeIndexes) : "ERROR: Missing data for INSERT";
                        break;
                    case "SELECT":
                        if (data != null && data.startsWith("JOIN")) {
                            response = handleJoin(tableName, data, workingTables, workingHashIndexes);
                        } else {
                            response = selectRows(tableName, data, workingTables);
                        }
                        break;
                    case "UPDATE":
                        response = data != null ? updateRows(tableName, data, workingTables, workingHashIndexes, workingBtreeIndexes) : "ERROR: Missing data for UPDATE";
                        break;
                    case "DELETE":
                        response = deleteRows(tableName, data, workingTables, workingHashIndexes, workingBtreeIndexes);
                        break;
                    default:
                        return "ERROR: Unknown command";
                }

                if (!cmd.equals("SELECT") && response.startsWith("OK") && !inTransaction) {
                    dirtyTables.put(tableName, true);
                }
                return response;

            } catch (Exception e) {
                if (inTransaction) {
                    rollbackTransaction();
                }
                return "ERROR: " + e.getMessage();
            }
        }

        private void beginTransaction() {
            inTransaction = true;
            transactionTables.clear();
            transactionHashIndexes.clear();
            transactionBtreeIndexes.clear();
            // Копируем текущие таблицы и индексы в буфер транзакции
            for (String tableName : tables.keySet()) {
                List<Map<String, Object>> tableCopy = new ArrayList<>();
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        tableCopy.add(new HashMap<>(row));
                    }
                }
                transactionTables.put(tableName, tableCopy);

                Map<String, Map<Object, Set<Map<String, Object>>>> hashCopy = new HashMap<>();
                Map<String, TreeMap<Object, Set<Map<String, Object>>>> btreeCopy = new HashMap<>();
                rebuildIndexesForTransaction(tableName, tableCopy, hashCopy, btreeCopy);
                transactionHashIndexes.put(tableName, hashCopy);
                transactionBtreeIndexes.put(tableName, btreeCopy);
            }
        }

        private void commitTransaction() {
            synchronized (tables) {
                for (String tableName : transactionTables.keySet()) {
                    tables.put(tableName, new ArrayList<>(transactionTables.get(tableName)));
                    hashIndexes.put(tableName, new HashMap<>(transactionHashIndexes.get(tableName)));
                    btreeIndexes.put(tableName, new HashMap<>(transactionBtreeIndexes.get(tableName)));
                    dirtyTables.put(tableName, true);
                }
            }
            inTransaction = false;
            transactionTables.clear();
            transactionHashIndexes.clear();
            transactionBtreeIndexes.clear();
        }

        private void rollbackTransaction() {
            inTransaction = false;
            transactionTables.clear();
            transactionHashIndexes.clear();
            transactionBtreeIndexes.clear();
        }

        private void rebuildIndexesForTransaction(String tableName, List<Map<String, Object>> table,
                                                  Map<String, Map<Object, Set<Map<String, Object>>>> hashIndex,
                                                  Map<String, TreeMap<Object, Set<Map<String, Object>>>> btreeIndex) {
            for (Map<String, Object> row : table) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String column = entry.getKey();
                    Object value = entry.getValue();
                    hashIndex.computeIfAbsent(column, k -> new HashMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                    btreeIndex.computeIfAbsent(column, k -> new TreeMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                }
            }
        }

        private void loadTableFromDisk(String tableName) {
            Path filePath = Paths.get(DATA_DIR, tableName + ".ddb");
            if (Files.exists(filePath)) {
                try {
                    byte[] data = Files.readAllBytes(filePath);
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                        List<Map<String, Object>> tableData = (List<Map<String, Object>>) ois.readObject();
                        tables.put(tableName, new ArrayList<>(tableData));
                        rebuildIndexes(tableName);
                        lastAccessTimes.put(tableName, System.currentTimeMillis());
                        System.out.println("Reloaded table from disk: " + tableName);
                    }
                } catch (Exception e) {
                    System.err.println("Error reloading table " + tableName + ": " + e.getMessage());
                }
            } else if (!tables.containsKey(tableName)) {
                throw new IllegalStateException("Table " + tableName + " not found on disk or in memory");
            }
        }

        private Object parseValue(String typedValue) {
            String[] parts = typedValue.split(":", 2);
            if (parts.length != 2) {
                try {
                    return Integer.parseInt(parts[0]);
                } catch (NumberFormatException e1) {
                    try {
                        return new BigDecimal(parts[0]);
                    } catch (NumberFormatException e2) {
                        return parts[0];
                    }
                }
            }
            String type = parts[0].toLowerCase();
            String value = parts[1];
            try {
                switch (type) {
                    case "integer": return Integer.parseInt(value);
                    case "bigdecimal": return new BigDecimal(value);
                    case "boolean": return Boolean.parseBoolean(value);
                    case "date": return dateFormat.parse(value);
                    default: return value;
                }
            } catch (Exception e) {
                System.err.println("Error parsing value: " + typedValue + " - " + e.getMessage());
                return value;
            }
        }

        private String formatValue(Object value) {
            if (value == null) return "NULL";
            if (value instanceof Date) {
                return dateFormat.format((Date) value);
            }
            return value.toString();
        }

        private String createTable(String tableName,
                                   Map<String, List<Map<String, Object>>> tables,
                                   Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                   Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            tables.putIfAbsent(tableName, new ArrayList<>());
            hashIndexes.putIfAbsent(tableName, new HashMap<>());
            btreeIndexes.putIfAbsent(tableName, new HashMap<>());
            lastAccessTimes.put(tableName, System.currentTimeMillis());
            return "OK: Table '" + tableName + "' created";
        }

        private String insertRow(String tableName, String data,
                                 Map<String, List<Map<String, Object>>> tables,
                                 Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                 Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
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
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String column = entry.getKey();
                    Object value = entry.getValue();
                    hashIndexes.get(tableName).computeIfAbsent(column, k -> new HashMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                    btreeIndexes.get(tableName).computeIfAbsent(column, k -> new TreeMap<>())
                            .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                }
            }
            return "OK: 1 row inserted";
        }

        private String selectRows(String tableName, String conditionAndOrder,
                                  Map<String, List<Map<String, Object>>> tables) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            String condition = null;
            String orderBy = null;
            if (conditionAndOrder != null) {
                String[] parts = conditionAndOrder.split("\\s+ORDER\\s+BY\\s+", 2);
                condition = parts[0].trim();
                if (parts.length > 1) {
                    orderBy = parts[1].trim();
                }
            }

            List<Map<String, Object>> result;
            try {
                if (condition == null || condition.isEmpty()) {
                    synchronized (tables.get(tableName)) {
                        result = new ArrayList<>(tables.get(tableName));
                    }
                } else {
                    result = evaluateWithIndexes(tableName, condition, tables);
                    if (result == null) {
                        System.err.println("evaluateWithIndexes returned null for condition: " + condition);
                        result = new ArrayList<>();
                    }
                }

                if (orderBy != null) {
                    String[] orderParts = orderBy.split("\\s+");
                    String column = orderParts[0];
                    boolean ascending = orderParts.length == 1 || orderParts[1].equalsIgnoreCase("ASC");

                    result.sort((row1, row2) -> {
                        Object value1 = row1.get(column);
                        Object value2 = row2.get(column);
                        if (value1 == null && value2 == null) return 0;
                        if (value1 == null) return ascending ? -1 : 1;
                        if (value2 == null) return ascending ? 1 : -1;

                        int comparison;
                        if (value1 instanceof Integer && value2 instanceof Integer) {
                            comparison = Integer.compare((Integer) value1, (Integer) value2);
                        } else if (value1 instanceof BigDecimal && value2 instanceof BigDecimal) {
                            comparison = ((BigDecimal) value1).compareTo((BigDecimal) value2);
                        } else if (value1 instanceof Date && value2 instanceof Date) {
                            comparison = ((Date) value1).compareTo((Date) value2);
                        } else {
                            comparison = formatValue(value1).compareTo(formatValue(value2));
                        }
                        return ascending ? comparison : -comparison;
                    });
                }
            } catch (Exception e) {
                System.err.println("Error in selectRows for table " + tableName + ": " + e.getMessage());
                return "ERROR: Server exception - " + e.getMessage();
            }

            if (result.isEmpty()) {
                return "OK: 0 rows";
            }

            try {
                StringBuilder response = new StringBuilder("OK: ");
                response.append(String.join(":::", result.get(0).keySet()));
                for (Map<String, Object> row : result) {
                    response.append(";;;").append(String.join(":::", row.values().stream().map(this::formatValue).toArray(String[]::new)));
                }
                return response.toString();
            } catch (Exception e) {
                System.err.println("Error formatting select response: " + e.getMessage());
                return "ERROR: Response formatting failed - " + e.getMessage();
            }
        }

        private List<Map<String, Object>> evaluateWithIndexes(String tableName, String condition,
                                                              Map<String, List<Map<String, Object>>> tables) {
            List<Map<String, Object>> result = new ArrayList<>();
            String[] orParts = condition.split("\\s+OR\\s+");

            for (String orPart : orParts) {
                String[] andParts = orPart.split("\\s+AND\\s+");
                Set<Map<String, Object>> candidates = null;

                for (String expression : andParts) {
                    expression = expression.trim();
                    Set<Map<String, Object>> rows = evaluateSingleCondition(tableName, expression, tables);
                    if (rows == null) {
                        System.err.println("evaluateSingleCondition returned null for: " + expression);
                        rows = new HashSet<>();
                    }
                    if (candidates == null) {
                        candidates = new HashSet<>(rows);
                    } else {
                        candidates.retainAll(rows);
                    }
                    if (candidates.isEmpty()) break;
                }

                if (candidates != null) {
                    result.addAll(candidates);
                }
            }

            return new ArrayList<>(new LinkedHashSet<>(result));
        }

        private Set<Map<String, Object>> evaluateSingleCondition(String tableName, String expression,
                                                                 Map<String, List<Map<String, Object>>> tables) {
            if (expression.contains(">=")) {
                String[] parts = expression.split(">=");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), ">=", tables);
            } else if (expression.contains("<=")) {
                String[] parts = expression.split("<=");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), "<=", tables);
            } else if (expression.contains(">")) {
                String[] parts = expression.split(">");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), ">", tables);
            } else if (expression.contains("<")) {
                String[] parts = expression.split("<");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), "<", tables);
            } else if (expression.contains("!=")) {
                String[] parts = expression.split("!=");
                return getHashNotEquals(tableName, parts[0].trim(), parts[1].trim(), tables);
            } else if (expression.contains("=")) {
                String[] parts = expression.split("=");
                return getHashEquals(tableName, parts[0].trim(), parts[1].trim(), tables);
            } else {
                Set<Map<String, Object>> result = new HashSet<>();
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (evaluateWhereCondition(row, expression)) {
                            result.add(row);
                        }
                    }
                }
                return result;
            }
        }

        private Set<Map<String, Object>> getHashEquals(String tableName, String column, String valueStr,
                                                       Map<String, List<Map<String, Object>>> tables) {
            Object value = parseValue(valueStr);
            Map<Object, Set<Map<String, Object>>> index = (inTransaction ? transactionHashIndexes : hashIndexes).get(tableName).get(column);
            Set<Map<String, Object>> result = new HashSet<>();

            if (index == null) {
                System.err.println("No hash index for column " + column + " in table " + tableName);
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (row.get(column) != null && row.get(column).equals(value)) {
                            result.add(row);
                        }
                    }
                }
                return result;
            }

            Set<Map<String, Object>> rows = index.get(value);
            if (rows != null) {
                return new HashSet<>(rows);
            }

            try {
                Integer intValue = Integer.parseInt(valueStr);
                rows = index.get(intValue);
                if (rows != null) {
                    return new HashSet<>(rows);
                }
            } catch (NumberFormatException ignored) {}

            return result;
        }

        private Set<Map<String, Object>> getHashNotEquals(String tableName, String column, String valueStr,
                                                          Map<String, List<Map<String, Object>>> tables) {
            Object value = parseValue(valueStr);
            Set<Map<String, Object>> result = new HashSet<>();
            Map<Object, Set<Map<String, Object>>> index = (inTransaction ? transactionHashIndexes : hashIndexes).get(tableName).get(column);
            if (index != null) {
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        Object rowValue = row.get(column);
                        if (rowValue != null && !rowValue.equals(value)) {
                            result.add(row);
                        }
                    }
                }
            }
            return result;
        }

        private Set<Map<String, Object>> getBtreeRange(String tableName, String column, String valueStr, String operator,
                                                       Map<String, List<Map<String, Object>>> tables) {
            Object value = parseValue(valueStr);
            TreeMap<Object, Set<Map<String, Object>>> index = (inTransaction ? transactionBtreeIndexes : btreeIndexes).get(tableName).get(column);
            if (index == null) {
                return evaluateSingleCondition(tableName, column + operator + valueStr, tables);
            }
            Set<Map<String, Object>> result = new HashSet<>();
            switch (operator) {
                case ">":
                    index.tailMap(value, false).values().forEach(result::addAll);
                    break;
                case ">=":
                    index.tailMap(value, true).values().forEach(result::addAll);
                    break;
                case "<":
                    index.headMap(value, false).values().forEach(result::addAll);
                    break;
                case "<=":
                    index.headMap(value, true).values().forEach(result::addAll);
                    break;
            }
            if (result.isEmpty()) {
                try {
                    Integer intValue = Integer.parseInt(valueStr);
                    switch (operator) {
                        case ">": index.tailMap(intValue, false).values().forEach(result::addAll); break;
                        case ">=": index.tailMap(intValue, true).values().forEach(result::addAll); break;
                        case "<": index.headMap(intValue, false).values().forEach(result::addAll); break;
                        case "<=": index.headMap(intValue, true).values().forEach(result::addAll); break;
                    }
                } catch (NumberFormatException ignored) {}
            }
            return result;
        }

        private boolean evaluateWhereCondition(Map<String, Object> row, String condition) {
            String[] orParts = condition.split("\\s+OR\\s+");
            for (String orPart : orParts) {
                String[] andParts = orPart.split("\\s+AND\\s+");
                boolean andResult = true;
                for (String expression : andParts) {
                    expression = expression.trim();
                    if (expression.contains(">=")) {
                        String[] parts = expression.split(">=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= compareValues(value, conditionValue, ">=");
                    } else if (expression.contains("<=")) {
                        String[] parts = expression.split("<=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= compareValues(value, conditionValue, "<=");
                    } else if (expression.contains(">")) {
                        String[] parts = expression.split(">");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= compareValues(value, conditionValue, ">");
                    } else if (expression.contains("<")) {
                        String[] parts = expression.split("<");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= compareValues(value, conditionValue, "<");
                    } else if (expression.contains("!=")) {
                        String[] parts = expression.split("!=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= !Objects.equals(value, conditionValue);
                    } else if (expression.contains("=")) {
                        String[] parts = expression.split("=");
                        Object value = row.getOrDefault(parts[0].trim(), "");
                        Object conditionValue = parseValue(parts[1].trim());
                        andResult &= Objects.equals(value, conditionValue);
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
            if (value1 == null || value2 == null) return false;

            if (value1 instanceof Integer && value2 instanceof String) {
                try {
                    value2 = Integer.parseInt((String) value2);
                } catch (NumberFormatException e) {
                    return false;
                }
            } else if (value1 instanceof String && value2 instanceof Integer) {
                try {
                    value1 = Integer.parseInt((String) value1);
                } catch (NumberFormatException e) {
                    return false;
                }
            } else if (value1 instanceof Integer && value2 instanceof BigDecimal) {
                value1 = BigDecimal.valueOf((Integer) value1);
            } else if (value1 instanceof BigDecimal && value2 instanceof Integer) {
                value2 = BigDecimal.valueOf((Integer) value2);
            }

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
                if (!(value1 instanceof String && value2 instanceof String)) {
                    return false;
                }
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

        private String handleJoin(String leftTable, String joinData,
                                  Map<String, List<Map<String, Object>>> tables,
                                  Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes) {
            String[] joinParts = joinData.split("§§§");
            if (joinParts.length < 3) return "ERROR: Invalid JOIN format";
            if (!joinParts[0].equals("JOIN")) return "ERROR: JOIN command must start with JOIN";
            String rightTable = joinParts[1];
            String joinCondition = joinParts[2];
            String whereConditionAndOrder = joinParts.length > 3 ? joinParts[3] : null;

            String whereCondition = null;
            String orderBy = null;
            if (whereConditionAndOrder != null) {
                String[] parts = whereConditionAndOrder.split("\\s+ORDER\\s+BY\\s+", 2);
                whereCondition = parts[0].trim();
                if (parts.length > 1) {
                    orderBy = parts[1].trim();
                }
            }

            if (!tables.containsKey(leftTable)) loadTableFromDisk(leftTable);
            if (!tables.containsKey(rightTable)) loadTableFromDisk(rightTable);

            lastAccessTimes.put(rightTable, System.currentTimeMillis());

            String[] keys = joinCondition.split("=");
            if (keys.length != 2) return "ERROR: Invalid join condition";
            String leftKey = keys[0];
            String rightKey = keys[1];

            List<Map<String, Object>> result = new ArrayList<>();
            Map<Object, Set<Map<String, Object>>> rightIndex = hashIndexes.get(rightTable).get(rightKey);

            synchronized (tables.get(leftTable)) {
                synchronized (tables.get(rightTable)) {
                    if (rightIndex != null) {
                        for (Map<String, Object> leftRow : tables.get(leftTable)) {
                            Object leftValue = leftRow.get(leftKey);
                            if (leftValue != null && rightIndex.containsKey(leftValue)) {
                                for (Map<String, Object> rightRow : rightIndex.get(leftValue)) {
                                    Map<String, Object> joinedRow = new LinkedHashMap<>();
                                    leftRow.forEach((k, v) -> joinedRow.put(leftTable + "." + k, v));
                                    rightRow.forEach((k, v) -> joinedRow.put(rightTable + "." + k, v));
                                    if (whereCondition == null || evaluateWhereCondition(joinedRow, whereCondition)) {
                                        result.add(joinedRow);
                                    }
                                }
                            }
                        }
                    } else {
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
            }

            if (orderBy != null) {
                String[] orderParts = orderBy.split("\\s+");
                String column = orderParts[0];
                boolean ascending = orderParts.length == 1 || orderParts[1].equalsIgnoreCase("ASC");

                result.sort((row1, row2) -> {
                    Object value1 = row1.get(column);
                    Object value2 = row2.get(column);
                    if (value1 == null && value2 == null) return 0;
                    if (value1 == null) return ascending ? -1 : 1;
                    if (value2 == null) return ascending ? 1 : -1;

                    int comparison;
                    if (value1 instanceof Integer && value2 instanceof Integer) {
                        comparison = Integer.compare((Integer) value1, (Integer) value2);
                    } else if (value1 instanceof BigDecimal && value2 instanceof BigDecimal) {
                        comparison = ((BigDecimal) value1).compareTo((BigDecimal) value2);
                    } else if (value1 instanceof Date && value2 instanceof Date) {
                        comparison = ((Date) value1).compareTo((Date) value2);
                    } else {
                        comparison = formatValue(value1).compareTo(formatValue(value2));
                    }
                    return ascending ? comparison : -comparison;
                });
            }

            if (result.isEmpty()) return "OK: 0 rows";
            StringBuilder response = new StringBuilder("OK: ");
            response.append(String.join(":::", result.get(0).keySet()));
            for (Map<String, Object> row : result) {
                response.append(";;;").append(String.join(":::", row.values().stream().map(this::formatValue).toArray(String[]::new)));
            }
            return response.toString();
        }

        private String updateRows(String tableName, String data,
                                  Map<String, List<Map<String, Object>>> tables,
                                  Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                  Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            String[] parts = data.split(";;;", 2);
            if (parts.length != 2) return "ERROR: Invalid update format";
            String condition = parts[0];
            String[] updates = parts[1].split(":::");
            Map<String, Object> updateMap = new HashMap<>();
            for (int i = 0; i < updates.length; i += 2) {
                if (i + 1 < updates.length) {
                    updateMap.put(updates[i], parseValue(updates[i + 1]));
                }
            }

            List<Map<String, Object>> toUpdate = condition.isEmpty() ? tables.get(tableName) : evaluateWithIndexes(tableName, condition, tables);
            int updated = 0;
            synchronized (tables.get(tableName)) {
                for (Map<String, Object> row : toUpdate) {
                    for (String column : updateMap.keySet()) {
                        Object oldValue = row.get(column);
                        if (oldValue != null) {
                            hashIndexes.get(tableName).get(column).getOrDefault(oldValue, Collections.emptySet()).remove(row);
                            btreeIndexes.get(tableName).get(column).getOrDefault(oldValue, Collections.emptySet()).remove(row);
                        }
                    }
                    row.putAll(updateMap);
                    updated++;
                    for (Map.Entry<String, Object> entry : updateMap.entrySet()) {
                        String column = entry.getKey();
                        Object newValue = entry.getValue();
                        hashIndexes.get(tableName).computeIfAbsent(column, k -> new HashMap<>())
                                .computeIfAbsent(newValue, k -> new HashSet<>()).add(row);
                        btreeIndexes.get(tableName).computeIfAbsent(column, k -> new TreeMap<>())
                                .computeIfAbsent(newValue, k -> new HashSet<>()).add(row);
                    }
                }
            }
            return "OK: " + updated;
        }

        private String deleteRows(String tableName, String condition,
                                  Map<String, List<Map<String, Object>>> tables,
                                  Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                  Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            int deleted = 0;
            synchronized (tables.get(tableName)) {
                List<Map<String, Object>> toDelete;
                if (condition == null) {
                    toDelete = new ArrayList<>(tables.get(tableName));
                    tables.get(tableName).clear();
                    hashIndexes.get(tableName).clear();
                    btreeIndexes.get(tableName).clear();
                    deleted = toDelete.size();
                } else {
                    toDelete = evaluateWithIndexes(tableName, condition, tables);
                    for (Map<String, Object> row : toDelete) {
                        tables.get(tableName).remove(row);
                        for (Map.Entry<String, Object> entry : row.entrySet()) {
                            String column = entry.getKey();
                            Object value = entry.getValue();
                            hashIndexes.get(tableName).get(column).getOrDefault(value, Collections.emptySet()).remove(row);
                            btreeIndexes.get(tableName).get(column).getOrDefault(value, Collections.emptySet()).remove(row);
                        }
                        deleted++;
                    }
                }
            }
            return "OK: " + deleted;
        }
    }
}