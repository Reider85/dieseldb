import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
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
    private static final int BATCH_SIZE = 1000; // Размер пакета для удаления

    private static final Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes = new ConcurrentHashMap<>();
    private static final Set<Map<String, Object>> deletedRows = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Map<String, Map<String, String>> tableSchemas = new ConcurrentHashMap<>();
    private static final Map<String, String> primaryKeys = new ConcurrentHashMap<>();
    private static final Map<String, Set<String>> uniqueConstraints = new ConcurrentHashMap<>();
    // Хранение активных операций записи для ожидания завершения при выключении
    private static final Map<String, Future<?>> pendingWrites = new ConcurrentHashMap<>();

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
            waitForPendingWrites(); // Дожидаемся завершения всех операций записи
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
                    }
                    Path schemaPath = Paths.get(DATA_DIR, tableName + ".schema");
                    if (Files.exists(schemaPath)) {
                        byte[] schemaData = Files.readAllBytes(schemaPath);
                        ByteArrayInputStream schemaBais = new ByteArrayInputStream(schemaData);
                        try (ObjectInputStream schemaOis = new ObjectInputStream(schemaBais)) {
                            tableSchemas.put(tableName, (Map<String, String>) schemaOis.readObject());
                            primaryKeys.put(tableName, (String) schemaOis.readObject());
                            uniqueConstraints.put(tableName, (Set<String>) schemaOis.readObject());
                        }
                    }
                    System.out.println("Loaded table and schema: " + tableName);
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
                    synchronized (tables.get(tableName)) {
                        cleanIndexes(tableName);
                        saveTableToDisk(tableName);
                        dirtyTables.put(tableName, false);
                    }
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
                    tableSchemas.remove(tableName);
                    primaryKeys.remove(tableName);
                    uniqueConstraints.remove(tableName);
                    pendingWrites.remove(tableName); // Удаляем завершенные операции
                    System.out.println("Unloaded inactive table from memory: " + tableName);
                }
            }
        }
    }

    private static void saveTableToDisk(String tableName) {
        try {
            // Сохранение данных таблицы
            Path filePath = Paths.get(DATA_DIR, tableName + ".ddb");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(tables.get(tableName));
            }
            byte[] data = baos.toByteArray();
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ByteBuffer buffer = ByteBuffer.wrap(data);
            Future<Integer> writeFuture = channel.write(buffer, 0);
            pendingWrites.put(tableName + ".ddb", writeFuture);

            // Сохранение схемы
            Path schemaPath = Paths.get(DATA_DIR, tableName + ".schema");
            ByteArrayOutputStream schemaBaos = new ByteArrayOutputStream();
            try (ObjectOutputStream schemaOos = new ObjectOutputStream(schemaBaos)) {
                schemaOos.writeObject(tableSchemas.get(tableName));
                schemaOos.writeObject(primaryKeys.get(tableName));
                schemaOos.writeObject(uniqueConstraints.get(tableName));
            }
            byte[] schemaData = schemaBaos.toByteArray();
            AsynchronousFileChannel schemaChannel = AsynchronousFileChannel.open(
                    schemaPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ByteBuffer schemaBuffer = ByteBuffer.wrap(schemaData);
            Future<Integer> schemaWriteFuture = schemaChannel.write(schemaBuffer, 0);
            pendingWrites.put(tableName + ".schema", schemaWriteFuture);

            // Асинхронное логирование завершения
            diskExecutor.submit(() -> {
                try {
                    writeFuture.get(); // Ожидание завершения записи данных
                    schemaWriteFuture.get(); // Ожидание завершения записи схемы
                    channel.close();
                    schemaChannel.close();
                    pendingWrites.remove(tableName + ".ddb");
                    pendingWrites.remove(tableName + ".schema");
                    System.out.println("Saved table and schema asynchronously: " + tableName);
                } catch (Exception e) {
                    System.err.println("Error completing async write for " + tableName + ": " + e.getMessage());
                }
            });
        } catch (IOException e) {
            System.err.println("Error initiating async save for " + tableName + ": " + e.getMessage());
        }
    }

    private static void flushAllTablesToDisk() {
        for (String tableName : tables.keySet()) {
            synchronized (tables.get(tableName)) {
                cleanIndexes(tableName);
                saveTableToDisk(tableName);
            }
        }
        waitForPendingWrites(); // Дожидаемся завершения всех операций
    }

    private static void waitForPendingWrites() {
        for (Map.Entry<String, Future<?>> entry : pendingWrites.entrySet()) {
            try {
                entry.getValue().get(5, TimeUnit.SECONDS); // Ожидание до 5 секунд
                System.out.println("Completed pending write for " + entry.getKey());
            } catch (Exception e) {
                System.err.println("Error waiting for write completion of " + entry.getKey() + ": " + e.getMessage());
            }
        }
        pendingWrites.clear();
    }

    private static void rebuildIndexes(String tableName) {
        hashIndexes.putIfAbsent(tableName, new HashMap<>());
        btreeIndexes.putIfAbsent(tableName, new HashMap<>());
        List<Map<String, Object>> table = tables.get(tableName);
        if (table == null) return;

        synchronized (table) {
            for (Map<String, Object> row : table) {
                if (!deletedRows.contains(row)) {
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
    }

    private static void cleanIndexes(String tableName) {
        Map<String, Map<Object, Set<Map<String, Object>>>> hashIndex = hashIndexes.get(tableName);
        Map<String, TreeMap<Object, Set<Map<String, Object>>>> btreeIndex = btreeIndexes.get(tableName);
        if (hashIndex == null || btreeIndex == null) return;

        Iterator<Map<String, Object>> iterator = deletedRows.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> row = iterator.next();
            if (!tables.getOrDefault(tableName, Collections.emptyList()).contains(row)) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String column = entry.getKey();
                    Object value = entry.getValue();
                    Map<Object, Set<Map<String, Object>>> columnHashIndex = hashIndex.get(column);
                    if (columnHashIndex != null) {
                        Set<Map<String, Object>> rows = columnHashIndex.get(value);
                        if (rows != null) {
                            rows.remove(row);
                            if (rows.isEmpty()) {
                                columnHashIndex.remove(value);
                            }
                        }
                    }
                    TreeMap<Object, Set<Map<String, Object>>> columnBtreeIndex = btreeIndex.get(column);
                    if (columnBtreeIndex != null) {
                        Set<Map<String, Object>> btreeRows = columnBtreeIndex.get(value);
                        if (btreeRows != null) {
                            btreeRows.remove(row);
                            if (btreeRows.isEmpty()) {
                                columnBtreeIndex.remove(value);
                            }
                        }
                    }
                }
                iterator.remove();
            }
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private boolean inTransaction = false;
        private Map<String, List<Map<String, Object>>> transactionTables;
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
                    return "ERROR: Invalid command format";
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
                        response = createTable(tableName, data, workingTables, workingHashIndexes, workingBtreeIndexes);
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
            for (String tableName : tables.keySet()) {
                List<Map<String, Object>> tableCopy = new ArrayList<>();
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (!deletedRows.contains(row)) {
                            tableCopy.add(new HashMap<>(row));
                        }
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

                        Path schemaPath = Paths.get(DATA_DIR, tableName + ".schema");
                        if (Files.exists(schemaPath)) {
                            byte[] schemaData = Files.readAllBytes(schemaPath);
                            ByteArrayInputStream schemaBais = new ByteArrayInputStream(schemaData);
                            try (ObjectInputStream schemaOis = new ObjectInputStream(schemaBais)) {
                                tableSchemas.put(tableName, (Map<String, String>) schemaOis.readObject());
                                primaryKeys.put(tableName, (String) schemaOis.readObject());
                                uniqueConstraints.put(tableName, (Set<String>) schemaOis.readObject());
                            }
                        }
                        System.out.println("Reloaded table and schema from disk: " + tableName);
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

        private boolean isValidType(Object value, String type) {
            switch (type.toLowerCase()) {
                case "integer": return value instanceof Integer;
                case "bigdecimal": return value instanceof BigDecimal;
                case "boolean": return value instanceof Boolean;
                case "date": return value instanceof Date;
                case "string": return value instanceof String;
                default: return true;
            }
        }

        private String createTable(String tableName, String schemaDefinition,
                                   Map<String, List<Map<String, Object>>> tables,
                                   Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                   Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (tables.containsKey(tableName)) {
                return "ERROR: Table already exists";
            }
            tables.put(tableName, new ArrayList<>());
            hashIndexes.put(tableName, new HashMap<>());
            btreeIndexes.put(tableName, new HashMap<>());

            if (schemaDefinition == null || schemaDefinition.isEmpty()) {
                return "OK: Table '" + tableName + "' created without schema";
            }

            Map<String, String> schema = new HashMap<>();
            Set<String> uniqueCols = new HashSet<>();
            String primaryKey = null;

            String[] columns = schemaDefinition.split(",");
            for (String col : columns) {
                String[] parts = col.split(":");
                if (parts.length < 2) return "ERROR: Invalid schema format";
                String colName = parts[0];
                String type = parts[1];
                String constraint = parts.length > 2 ? parts[2].toLowerCase() : "";

                schema.put(colName, type + (constraint.isEmpty() ? "" : ":" + constraint));
                if ("primary".equals(constraint)) {
                    if (primaryKey != null) return "ERROR: Multiple primary keys defined";
                    primaryKey = colName;
                } else if ("unique".equals(constraint)) {
                    uniqueCols.add(colName);
                }
            }
            tableSchemas.put(tableName, schema);
            if (primaryKey != null) {
                primaryKeys.put(tableName, primaryKey);
                uniqueCols.add(primaryKey);
            }
            if (!uniqueCols.isEmpty()) {
                uniqueConstraints.put(tableName, uniqueCols);
            }

            lastAccessTimes.put(tableName, System.currentTimeMillis());
            return "OK: Table '" + tableName + "' created with schema";
        }

        private String insertRow(String tableName, String data,
                                 Map<String, List<Map<String, Object>>> tables,
                                 Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                 Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }
            Map<String, String> schema = tableSchemas.get(tableName);
            Map<String, Object> row = new HashMap<>();
            String[] pairs = data.split(":::");
            for (int i = 0; i < pairs.length; i += 2) {
                if (i + 1 >= pairs.length) {
                    return "ERROR: Invalid data format";
                }
                String col = pairs[i];
                Object value = parseValue(pairs[i + 1]);
                if (schema != null) {
                    String colDef = schema.get(col);
                    if (colDef == null) return "ERROR: Unknown column " + col;
                    String[] defParts = colDef.split(":");
                    if (!isValidType(value, defParts[0])) return "ERROR: Type mismatch for " + col;
                }
                row.put(col, value);
            }

            synchronized (tables.get(tableName)) {
                String pkCol = primaryKeys.get(tableName);
                if (pkCol != null) {
                    Object pkValue = row.get(pkCol);
                    if (pkValue == null) return "ERROR: Primary key " + pkCol + " cannot be null";
                    Map<Object, Set<Map<String, Object>>> pkIndex = hashIndexes.get(tableName).get(pkCol);
                    if (pkIndex != null && pkIndex.containsKey(pkValue)) {
                        if (pkIndex.get(pkValue).stream().anyMatch(r -> !deletedRows.contains(r))) {
                            return "ERROR: Duplicate primary key value " + pkValue + " for " + pkCol;
                        }
                    }
                }

                Set<String> uniqueCols = uniqueConstraints.get(tableName);
                if (uniqueCols != null) {
                    for (String col : uniqueCols) {
                        if (!col.equals(pkCol)) {
                            Object value = row.get(col);
                            if (value != null) {
                                Map<Object, Set<Map<String, Object>>> colIndex = hashIndexes.get(tableName).get(col);
                                if (colIndex != null && colIndex.containsKey(value)) {
                                    if (colIndex.get(value).stream().anyMatch(r -> !deletedRows.contains(r))) {
                                        return "ERROR: Duplicate unique value " + value + " for " + col;
                                    }
                                }
                            }
                        }
                    }
                }

                tables.get(tableName).add(row);
                if (!inTransaction) {
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
            return "OK: 1 row inserted";
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

            Map<String, String> schema = tableSchemas.get(tableName);
            if (schema != null) {
                for (Map.Entry<String, Object> entry : updateMap.entrySet()) {
                    String col = entry.getKey();
                    Object value = entry.getValue();
                    String colDef = schema.get(col);
                    if (colDef == null) return "ERROR: Unknown column " + col;
                    String[] defParts = colDef.split(":");
                    if (!isValidType(value, defParts[0])) return "ERROR: Type mismatch for " + col;
                }
            }

            List<Map<String, Object>> toUpdate = condition.isEmpty() ? tables.get(tableName) : evaluateWithIndexes(tableName, condition, tables);
            int updated = 0;
            synchronized (tables.get(tableName)) {
                String pkCol = primaryKeys.get(tableName);
                Set<String> uniqueCols = uniqueConstraints.get(tableName);

                for (Map<String, Object> row : toUpdate) {
                    if (!deletedRows.contains(row)) {
                        Map<String, Object> newRow = new HashMap<>(row);
                        newRow.putAll(updateMap);

                        if (pkCol != null) {
                            Object newPkValue = newRow.get(pkCol);
                            if (newPkValue == null) return "ERROR: Primary key " + pkCol + " cannot be null";
                            Map<Object, Set<Map<String, Object>>> pkIndex = hashIndexes.get(tableName).get(pkCol);
                            if (pkIndex != null && pkIndex.containsKey(newPkValue)) {
                                if (pkIndex.get(newPkValue).stream().anyMatch(r -> !r.equals(row) && !deletedRows.contains(r))) {
                                    return "ERROR: Duplicate primary key value " + newPkValue + " for " + pkCol;
                                }
                            }
                        }

                        if (uniqueCols != null) {
                            for (String col : uniqueCols) {
                                if (!col.equals(pkCol)) {
                                    Object newValue = newRow.get(col);
                                    if (newValue != null) {
                                        Map<Object, Set<Map<String, Object>>> colIndex = hashIndexes.get(tableName).get(col);
                                        if (colIndex != null && colIndex.containsKey(newValue)) {
                                            if (colIndex.get(newValue).stream().anyMatch(r -> !r.equals(row) && !deletedRows.contains(r))) {
                                                return "ERROR: Duplicate unique value " + newValue + " for " + col;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if (!inTransaction) {
                            for (String column : updateMap.keySet()) {
                                Object oldValue = row.get(column);
                                if (oldValue != null) {
                                    hashIndexes.get(tableName).get(column).getOrDefault(oldValue, Collections.emptySet()).remove(row);
                                    btreeIndexes.get(tableName).get(column).getOrDefault(oldValue, Collections.emptySet()).remove(row);
                                }
                            }
                        }
                        row.putAll(updateMap);
                        updated++;
                        if (!inTransaction) {
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
                }
            }
            return "OK: " + updated;
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
                        result = new ArrayList<>();
                        for (Map<String, Object> row : tables.get(tableName)) {
                            if (!deletedRows.contains(row)) {
                                result.add(row);
                            }
                        }
                    }
                } else {
                    result = evaluateWithIndexes(tableName, condition, tables);
                    if (result == null) {
                        System.err.println("evaluateWithIndexes returned null for condition: " + condition);
                        result = new ArrayList<>();
                    }
                    result.removeIf(deletedRows::contains);
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
                System.err.println("Error in selectRows: " + e.getMessage());
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
            String[] parts;
            if (expression.contains(">=")) {
                parts = expression.split(">=");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), ">=", tables);
            } else if (expression.contains("<=")) {
                parts = expression.split("<=");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), "<=", tables);
            } else if (expression.contains(">")) {
                parts = expression.split(">");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), ">", tables);
            } else if (expression.contains("<")) {
                parts = expression.split("<");
                return getBtreeRange(tableName, parts[0].trim(), parts[1].trim(), "<", tables);
            } else if (expression.contains("!=")) {
                parts = expression.split("!=");
                return getHashNotEquals(tableName, parts[0].trim(), parts[1].trim(), tables);
            } else if (expression.contains("=")) {
                parts = expression.split("=");
                return getHashEquals(tableName, parts[0].trim(), parts[1].trim(), tables);
            } else {
                Set<Map<String, Object>> result = new HashSet<>();
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (!deletedRows.contains(row) && evaluateWhereCondition(row, expression)) {
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
                synchronized (tables.get(tableName)) {
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (!deletedRows.contains(row) && row.get(column) != null && row.get(column).equals(value)) {
                            result.add(row);
                        }
                    }
                }
                return result;
            }

            Set<Map<String, Object>> rows = index.get(value);
            if (rows != null) {
                result.addAll(rows);
                result.removeIf(deletedRows::contains);
            }

            try {
                Integer intValue = Integer.parseInt(valueStr);
                rows = index.get(intValue);
                if (rows != null) {
                    result.addAll(rows);
                    result.removeIf(deletedRows::contains);
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
                        if (!deletedRows.contains(row)) {
                            Object rowValue = row.get(column);
                            if (rowValue != null && !rowValue.equals(value)) {
                                result.add(row);
                            }
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
                    index.tailMap(value, false).values().forEach(s -> result.addAll(s));
                    break;
                case ">=":
                    index.tailMap(value, true).values().forEach(s -> result.addAll(s));
                    break;
                case "<":
                    index.headMap(value, false).values().forEach(s -> result.addAll(s));
                    break;
                case "<=":
                    index.headMap(value, true).values().forEach(s -> result.addAll(s));
                    break;
            }
            result.removeIf(deletedRows::contains);
            if (result.isEmpty()) {
                try {
                    Integer intValue = Integer.parseInt(valueStr);
                    switch (operator) {
                        case ">": index.tailMap(intValue, false).values().forEach(s -> result.addAll(s)); break;
                        case ">=": index.tailMap(intValue, true).values().forEach(s -> result.addAll(s)); break;
                        case "<": index.headMap(intValue, false).values().forEach(s -> result.addAll(s)); break;
                        case "<=": index.headMap(intValue, true).values().forEach(s -> result.addAll(s)); break;
                    }
                    result.removeIf(deletedRows::contains);
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
                            if (deletedRows.contains(leftRow)) continue;
                            Object leftValue = leftRow.get(leftKey);
                            if (leftValue != null && rightIndex.containsKey(leftValue)) {
                                for (Map<String, Object> rightRow : rightIndex.get(leftValue)) {
                                    if (!deletedRows.contains(rightRow)) {
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
                    } else {
                        for (Map<String, Object> leftRow : tables.get(leftTable)) {
                            if (deletedRows.contains(leftRow)) continue;
                            Object leftValue = leftRow.get(leftKey);
                            if (leftValue == null) continue;
                            for (Map<String, Object> rightRow : tables.get(rightTable)) {
                                if (!deletedRows.contains(rightRow)) {
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

        private String deleteRows(String tableName, String condition,
                                  Map<String, List<Map<String, Object>>> tables,
                                  Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                  Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            List<Map<String, Object>> table = tables.get(tableName);
            int deleted = 0;

            if (condition == null) {
                synchronized (table) {
                    deleted = table.size();
                    if (!inTransaction) {
                        deletedRows.addAll(table);
                        table.clear();
                        hashIndexes.get(tableName).clear();
                        btreeIndexes.get(tableName).clear();
                    } else {
                        table.clear();
                    }
                }
            } else {
                List<Map<String, Object>> toDelete = evaluateWithIndexes(tableName, condition, tables);
                if (toDelete.isEmpty()) {
                    return "OK: 0";
                }

                if (toDelete.size() > BATCH_SIZE && !inTransaction) {
                    CompletableFuture.runAsync(() -> {
                        int batchDeleted = 0;
                        synchronized (table) {
                            for (int i = 0; i < toDelete.size(); i += BATCH_SIZE) {
                                int end = Math.min(i + BATCH_SIZE, toDelete.size());
                                List<Map<String, Object>> batch = toDelete.subList(i, end);
                                for (Map<String, Object> row : batch) {
                                    if (!deletedRows.contains(row) && table.remove(row)) {
                                        deletedRows.add(row);
                                        batchDeleted++;
                                    }
                                }
                            }
                            if (batchDeleted > 0) {
                                dirtyTables.put(tableName, true);
                            }
                        }
                    }, clientExecutor);
                    deleted = toDelete.size();
                } else {
                    synchronized (table) {
                        for (Map<String, Object> row : toDelete) {
                            if (!deletedRows.contains(row) && table.remove(row)) {
                                if (!inTransaction) {
                                    deletedRows.add(row);
                                }
                                deleted++;
                            }
                        }
                        if (!inTransaction && deleted > 0) {
                            dirtyTables.put(tableName, true);
                        }
                    }
                }
            }

            return "OK: " + deleted;
        }
    }
}