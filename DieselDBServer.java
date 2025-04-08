import java.io.*;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.text.SimpleDateFormat;

public class DieselDBServer {
    private static final int PORT = DieselDBConfig.PORT;
    private static final String DATA_DIR = "data";
    private final Map<String, List<Map<String, Object>>> tables = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> tableSchemas = new ConcurrentHashMap<>();
    private final Map<String, String> primaryKeys = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> uniqueConstraints = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes = new ConcurrentHashMap<>();
    private final Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes = new ConcurrentHashMap<>();
    private final Map<String, Boolean> dirtyTables = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public DieselDBServer() {
        new File(DATA_DIR).mkdirs();
        loadTablesAndSchemas();
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("DieselDB Server started on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket));
            }
        }
    }

    private void loadTablesAndSchemas() {
        File dir = new File(DATA_DIR);
        for (File file : dir.listFiles(f -> f.getName().endsWith(".dat"))) {
            String tableName = file.getName().replace(".dat", "");
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                List<Map<String, Object>> table = (List<Map<String, Object>>) ois.readObject();
                tables.put(tableName, table);
                hashIndexes.put(tableName, new ConcurrentHashMap<>());
                btreeIndexes.put(tableName, new ConcurrentHashMap<>());
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
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error loading table " + tableName + ": " + e.getMessage());
            }
        }
        for (File file : dir.listFiles(f -> f.getName().endsWith(".schema"))) {
            String tableName = file.getName().replace(".schema", "");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                Map<String, String> schema = new HashMap<>();
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("=");
                    if (parts.length == 2) {
                        schema.put(parts[0], parts[1]);
                    }
                }
                tableSchemas.put(tableName, schema);
                String pk = schema.get("primaryKey");
                if (pk != null) primaryKeys.put(tableName, pk);
                String uniqueCols = schema.get("uniqueConstraints");
                if (uniqueCols != null) {
                    uniqueConstraints.put(tableName, new HashSet<>(Arrays.asList(uniqueCols.split(","))));
                }
            } catch (IOException e) {
                System.err.println("Error loading schema for " + tableName + ": " + e.getMessage());
            }
        }
    }

    private void saveTablesAndSchemasAsync() {
        for (String tableName : dirtyTables.keySet()) {
            if (dirtyTables.get(tableName)) {
                executor.submit(() -> {
                    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(DATA_DIR + "/" + tableName + ".dat"))) {
                        oos.writeObject(tables.get(tableName));
                        System.out.println("Saved table and schema asynchronously: " + tableName);
                    } catch (IOException e) {
                        System.err.println("Error saving table " + tableName + ": " + e.getMessage());
                    }
                    try (BufferedWriter bw = new BufferedWriter(new FileWriter(DATA_DIR + "/" + tableName + ".schema"))) {
                        Map<String, String> schema = tableSchemas.get(tableName);
                        if (schema != null) {
                            for (Map.Entry<String, String> entry : schema.entrySet()) {
                                bw.write(entry.getKey() + "=" + entry.getValue());
                                bw.newLine();
                            }
                        }
                        String pk = primaryKeys.get(tableName);
                        if (pk != null) {
                            bw.write("primaryKey=" + pk);
                            bw.newLine();
                        }
                        Set<String> uniques = uniqueConstraints.get(tableName);
                        if (uniques != null && !uniques.isEmpty()) {
                            bw.write("uniqueConstraints=" + String.join(",", uniques));
                            bw.newLine();
                        }
                    } catch (IOException e) {
                        System.err.println("Error saving schema for " + tableName + ": " + e.getMessage());
                    }
                });
                dirtyTables.put(tableName, false);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        new DieselDBServer().start();
    }

    enum IsolationLevel {
        READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
    }

    class ClientHandler implements Runnable {
        private final Socket socket;
        private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
        private boolean inTransaction = false;
        private final Map<String, List<Map<String, Object>>> transactionTables = new ConcurrentHashMap<>();
        private final Set<Map<String, Object>> transactionDirtyRows = ConcurrentHashMap.newKeySet();
        private final Set<Map<String, Object>> deletedRows = ConcurrentHashMap.newKeySet();
        private final Set<Map<String, Object>> uncommittedInserts = ConcurrentHashMap.newKeySet();

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println("OK: Welcome to DieselDB Server");
                String command;
                while ((command = in.readLine()) != null) {
                    System.out.println("Processing command: " + command);
                    String response = processCommand(command);
                    out.println(response);
                    System.out.println("Response sent: " + response);
                }
            } catch (IOException e) {
                System.err.println("Client disconnected: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                    saveTablesAndSchemasAsync();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }

        private String processCommand(String command) {
            System.out.println("Received: " + command);
            try {
                String[] parts = command.split("\\s+", 2);
                String cmd = parts[0].toUpperCase();
                String args = parts.length > 1 ? parts[1] : "";

                switch (cmd) {
                    case "CREATE":
                        String[] createParts = args.split("\\s+", 2);
                        if (createParts.length < 1) {
                            return "ERROR: Invalid CREATE syntax - missing table name";
                        }
                        String tableName = createParts[0];
                        String schemaDef = createParts.length > 1 ? createParts[1] : "";
                        return createTable(tableName + " " + schemaDef);
                    case "INSERT":
                        if (!args.startsWith("INTO ")) {
                            return "ERROR: Invalid INSERT syntax - missing INTO";
                        }
                        String[] insertParts = args.substring(5).split("\\s+VALUES\\s+", 2);
                        if (insertParts.length < 2) {
                            return "ERROR: Invalid INSERT syntax - missing VALUES";
                        }
                        String tablePart = insertParts[0].trim();
                        String valuesPart = insertParts[1].trim();
                        String insertTableName = tablePart.split("\\s+", 2)[0];
                        return insertRow(insertTableName, tablePart.substring(insertTableName.length()).trim() + " VALUES " + valuesPart,
                                getWorkingTables(cmd), hashIndexes, btreeIndexes);
                    case "SELECT":
                        String[] selectParts = args.split("\\s+", 2);
                        return selectRows(selectParts[0], selectParts.length > 1 && (args.contains("WHERE") || args.contains("ORDER")) ? selectParts[1] : null,
                                getWorkingTables(cmd));
                    case "UPDATE":
                        if (!args.contains("SET")) {
                            return "ERROR: Invalid UPDATE syntax - missing SET";
                        }
                        String[] updateParts = args.split("\\s+SET\\s+", 2);
                        if (updateParts.length < 2) {
                            return "ERROR: Invalid UPDATE syntax - missing SET clause";
                        }
                        String tableNameUpdate = updateParts[0].trim();
                        return updateRows(tableNameUpdate, updateParts[1],
                                getWorkingTables(cmd), hashIndexes, btreeIndexes);
                    case "DELETE":
                        if (!args.startsWith("FROM ")) {
                            return "ERROR: Invalid DELETE syntax - missing FROM";
                        }
                        String[] deleteParts = args.substring(5).split("\\s+WHERE\\s+", 2);
                        String deleteTableName = deleteParts[0].trim();
                        String condition = deleteParts.length > 1 ? deleteParts[1].trim() : null;
                        return deleteRows(deleteTableName, condition, getWorkingTables(cmd));
                    case "BEGIN":
                        return beginTransaction();
                    case "COMMIT":
                        return commitTransaction();
                    case "ROLLBACK":
                        return rollbackTransaction();
                    case "SET_ISOLATION":
                        return setIsolationLevel(args);
                    default:
                        return "ERROR: Unknown command - " + cmd;
                }
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }
        private String createTable(String args) {
            String[] parts = args.trim().split("\\s+", 2);
            if (parts.length < 1) {
                return "ERROR: Invalid CREATE syntax - missing table name";
            }
            String tableName = parts[0];
            if (tables.containsKey(tableName)) {
                return "ERROR: Table already exists";
            }
            String schemaDef = parts.length > 1 ? parts[1] : "";
            Map<String, String> schema = new HashMap<>();
            if (!schemaDef.isEmpty()) {
                if (!schemaDef.startsWith("(") || !schemaDef.endsWith(")")) {
                    return "ERROR: Invalid schema syntax - use (column type [constraints], ...)";
                }
                String[] columns = schemaDef.substring(1, schemaDef.length() - 1).split(",");
                for (String col : columns) {
                    String[] colParts = col.trim().split("\\s+");
                    if (colParts.length < 2) {
                        return "ERROR: Invalid column definition - " + col;
                    }
                    schema.put(colParts[0], colParts[1]);
                    if (colParts.length > 2 && colParts[2].equalsIgnoreCase("PRIMARY")) {
                        primaryKeys.put(tableName, colParts[0]);
                    }
                    if (colParts.length > 2 && colParts[2].equalsIgnoreCase("UNIQUE")) {
                        uniqueConstraints.computeIfAbsent(tableName, k -> new HashSet<>()).add(colParts[0]);
                    }
                }
            }
            tables.put(tableName, new ArrayList<>());
            tableSchemas.put(tableName, schema);
            hashIndexes.put(tableName, new ConcurrentHashMap<>());
            btreeIndexes.put(tableName, new ConcurrentHashMap<>());
            dirtyTables.put(tableName, true);
            return "OK: Table '" + tableName + "' created with schema";
        }
        private String insertRow(String tableName, String data,
                                 Map<String, List<Map<String, Object>>> tables,
                                 Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                 Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            String trimmedData = data.trim();
            String[] parts = trimmedData.split("\\s+VALUES\\s+", 2);
            if (parts.length != 2) {
                return "ERROR: Invalid INSERT format - missing VALUES clause";
            }

            String columnsPart = parts[0].trim();
            if (!columnsPart.startsWith("(") || !columnsPart.endsWith(")")) {
                return "ERROR: Invalid columns specification - use (column1, column2, ...)";
            }
            String columnsStr = columnsPart.substring(1, columnsPart.length() - 1).trim();
            String[] columns = columnsStr.split("\\s*,\\s*");
            if (columns.length == 0 || columns[0].isEmpty()) {
                return "ERROR: No columns specified";
            }

            String valuesPart = parts[1].trim();
            if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
                return "ERROR: Invalid values specification - use (value1, value2, ...)";
            }
            String valuesStr = valuesPart.substring(1, valuesPart.length() - 1).trim();
            String[] values = valuesStr.split("\\s*,\\s*");
            if (values.length == 0 || values[0].isEmpty()) {
                return "ERROR: No values specified";
            }

            if (columns.length != values.length) {
                return "ERROR: Number of columns and values must match";
            }

            Map<String, String> schema = tableSchemas.get(tableName);
            Map<String, Object> row = new HashMap<>();

            for (int i = 0; i < columns.length; i++) {
                String col = columns[i].trim();
                String valueStr = values[i].trim();
                Object value = parseValue(valueStr);

                if (schema != null) {
                    String colDef = schema.get(col);
                    if (colDef == null) return "ERROR: Unknown column " + col;
                    String[] defParts = colDef.split(":");
                    if (!isValidType(value, defParts[0])) return "ERROR: Type mismatch for " + col;
                }
                row.put(col, value);
            }

            Map<String, List<Map<String, Object>>> targetTables = inTransaction && isolationLevel != IsolationLevel.READ_UNCOMMITTED
                    ? transactionTables
                    : tables;

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

                targetTables.computeIfAbsent(tableName, k -> new ArrayList<>()).add(row);
                System.out.println("Inserted row into " + tableName + ": " + row);

                if (inTransaction && isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                    uncommittedInserts.add(row);
                    System.out.println("Added to uncommittedInserts: " + row);
                    System.out.println("Current uncommittedInserts: " + uncommittedInserts);
                }

                if (!inTransaction || isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        String column = entry.getKey();
                        Object value = entry.getValue();
                        hashIndexes.get(tableName).computeIfAbsent(column, k -> new HashMap<>())
                                .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                        btreeIndexes.get(tableName).computeIfAbsent(column, k -> new TreeMap<>())
                                .computeIfAbsent(value, k -> new HashSet<>()).add(row);
                    }
                } else {
                    transactionDirtyRows.add(row);
                    transactionTables.computeIfAbsent(tableName, k -> new ArrayList<>());
                }
            }

            dirtyTables.put(tableName, true);
            return "OK: 1 row inserted";
        }

        private String selectRows(String tableName, String conditionAndOrder,
                                  Map<String, List<Map<String, Object>>> tables) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            if (isolationLevel == IsolationLevel.READ_COMMITTED && !inTransaction) {
                synchronized (tables.get(tableName)) {
                    cleanIndexes(tableName);
                }
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
            synchronized (tables.get(tableName)) {
                if (condition == null || condition.isEmpty()) {
                    result = new ArrayList<>();
                    for (Map<String, Object> row : tables.get(tableName)) {
                        if (!deletedRows.contains(row)) {
                            result.add(row);
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
            }

            System.out.println("Selecting from " + tableName + " with condition: " + conditionAndOrder);
            System.out.println("Result rows: " + result);

            if (result.isEmpty()) {
                return "OK: 0 rows";
            }

            try {
                StringBuilder response = new StringBuilder("OK: ");
                response.append(String.join(":::", result.get(0).keySet()));
                for (Map<String, Object> row : result) {
                    response.append(";;;").append(String.join(":::", row.values().stream().map(this::formatValue).toArray(String[]::new)));
                }
                System.out.println("Select response: " + response);
                return response.toString();
            } catch (Exception e) {
                System.err.println("Error formatting select response: " + e.getMessage());
                return "ERROR: Response formatting failed - " + e.getMessage();
            }
        }

        private String updateRows(String tableName, String setAndWhere,
                                  Map<String, List<Map<String, Object>>> tables,
                                  Map<String, Map<String, Map<Object, Set<Map<String, Object>>>>> hashIndexes,
                                  Map<String, Map<String, TreeMap<Object, Set<Map<String, Object>>>>> btreeIndexes) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            String[] parts = setAndWhere.split("\\s+WHERE\\s+", 2);
            String setClause = parts[0].trim();
            String condition = parts.length > 1 ? parts[1].trim() : null;

            Map<String, Object> updates = new HashMap<>();
            for (String set : setClause.split(",")) {
                String[] kv = set.split("=");
                if (kv.length != 2) {
                    return "ERROR: Invalid SET clause - use column=value";
                }
                updates.put(kv[0].trim(), parseValue(kv[1].trim()));
            }

            List<Map<String, Object>> rowsToUpdate;
            synchronized (tables.get(tableName)) {
                if (condition == null) {
                    rowsToUpdate = new ArrayList<>(tables.get(tableName));
                } else {
                    rowsToUpdate = evaluateWithIndexes(tableName, condition, tables);
                }
                if (rowsToUpdate == null) rowsToUpdate = new ArrayList<>();
                rowsToUpdate.removeIf(deletedRows::contains);

                int updated = 0;
                for (Map<String, Object> row : rowsToUpdate) {
                    if (!deletedRows.contains(row)) {
                        for (Map.Entry<String, Object> update : updates.entrySet()) {
                            String column = update.getKey();
                            Object newValue = update.getValue();
                            Object oldValue = row.get(column);
                            if (oldValue != null && !oldValue.equals(newValue)) {
                                Map<Object, Set<Map<String, Object>>> hashIndex = hashIndexes.get(tableName).get(column);
                                if (hashIndex != null && hashIndex.get(oldValue) != null) {
                                    hashIndex.get(oldValue).remove(row);
                                    if (hashIndex.get(oldValue).isEmpty()) {
                                        hashIndex.remove(oldValue);
                                    }
                                }
                                TreeMap<Object, Set<Map<String, Object>>> btreeIndex = btreeIndexes.get(tableName).get(column);
                                if (btreeIndex != null && btreeIndex.get(oldValue) != null) {
                                    btreeIndex.get(oldValue).remove(row);
                                    if (btreeIndex.get(oldValue).isEmpty()) {
                                        btreeIndex.remove(oldValue);
                                    }
                                }
                            }
                            row.put(column, newValue);
                            hashIndexes.get(tableName).computeIfAbsent(column, k -> new HashMap<>())
                                    .computeIfAbsent(newValue, k -> new HashSet<>()).add(row);
                            btreeIndexes.get(tableName).computeIfAbsent(column, k -> new TreeMap<>())
                                    .computeIfAbsent(newValue, k -> new HashSet<>()).add(row);
                        }
                        updated++;
                        if (inTransaction && isolationLevel != IsolationLevel.READ_UNCOMMITTED) {
                            transactionDirtyRows.add(row);
                        }
                    }
                }
                if (updated > 0) {
                    dirtyTables.put(tableName, true);
                }
                return "OK: " + updated + " rows updated";
            }
        }

        private String deleteRows(String tableName, String condition,
                                  Map<String, List<Map<String, Object>>> tables) {
            if (!tables.containsKey(tableName)) {
                return "ERROR: Table not found";
            }

            List<Map<String, Object>> rowsToDelete;
            synchronized (tables.get(tableName)) {
                if (condition == null) {
                    rowsToDelete = new ArrayList<>(tables.get(tableName));
                } else {
                    rowsToDelete = evaluateWithIndexes(tableName, condition, tables);
                }
                if (rowsToDelete == null) rowsToDelete = new ArrayList<>();
                rowsToDelete.removeIf(deletedRows::contains);

                int deleted = 0;
                for (Map<String, Object> row : rowsToDelete) {
                    if (!deletedRows.contains(row)) {
                        if (inTransaction && isolationLevel != IsolationLevel.READ_UNCOMMITTED) {
                            deletedRows.add(row);
                        } else {
                            tables.get(tableName).remove(row);
                            for (Map.Entry<String, Object> entry : row.entrySet()) {
                                String column = entry.getKey();
                                Object value = entry.getValue();
                                hashIndexes.get(tableName).get(column).get(value).remove(row);
                                btreeIndexes.get(tableName).get(column).get(value).remove(row);
                            }
                        }
                        deleted++;
                    }
                }
                if (deleted > 0) {
                    dirtyTables.put(tableName, true);
                }
                return "OK: " + deleted + " rows deleted";
            }
        }

        private String beginTransaction() {
            if (inTransaction) {
                return "ERROR: Already in transaction";
            }
            inTransaction = true;
            return "OK: Transaction begun";
        }

        private String commitTransaction() {
            if (!inTransaction) {
                return "ERROR: No transaction in progress";
            }
            commitTransactionImpl();
            return "OK: Transaction committed";
        }

        private void commitTransactionImpl() {
            if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                synchronized (tables) {
                    System.out.println("Committing transaction for READ_UNCOMMITTED");
                    System.out.println("Uncommitted inserts to commit: " + uncommittedInserts);
                    for (String tableName : tables.keySet()) {
                        List<Map<String, Object>> table = tables.get(tableName);
                        System.out.println("Table " + tableName + " before commit: " + table);
                        for (Map<String, Object> row : uncommittedInserts) {
                            if (table.contains(row)) {
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
                        System.out.println("Table " + tableName + " after commit: " + table);
                    }
                    uncommittedInserts.clear();
                    System.out.println("Uncommitted inserts cleared after commit");
                }
            } else {
                synchronized (tables) {
                    System.out.println("Committing transaction for " + isolationLevel);
                    for (String tableName : transactionTables.keySet()) {
                        List<Map<String, Object>> transTable = transactionTables.get(tableName);
                        List<Map<String, Object>> mainTable = tables.computeIfAbsent(tableName, k -> new ArrayList<>());
                        for (Map<String, Object> row : transTable) {
                            if (!deletedRows.contains(row) && !mainTable.contains(row)) {
                                mainTable.add(row);
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
                        dirtyTables.put(tableName, true);
                    }
                    transactionTables.clear();
                    transactionDirtyRows.clear();
                    deletedRows.clear();
                }
            }
            inTransaction = false;
            System.out.println("Transaction committed, inTransaction set to false");
        }

        private String rollbackTransaction() {
            if (!inTransaction) {
                return "ERROR: No transaction in progress";
            }
            rollbackTransactionImpl();
            return "OK: Transaction rolled back";
        }

        private void rollbackTransactionImpl() {
            if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                synchronized (tables) {
                    System.out.println("Starting rollback for READ_UNCOMMITTED");
                    System.out.println("Uncommitted inserts: " + uncommittedInserts);
                    for (String tableName : tables.keySet()) {
                        List<Map<String, Object>> table = tables.get(tableName);
                        System.out.println("Table " + tableName + " before rollback: " + table);
                        Iterator<Map<String, Object>> iterator = table.iterator();
                        int removedCount = 0;
                        while (iterator.hasNext()) {
                            Map<String, Object> row = iterator.next();
                            if (uncommittedInserts.contains(row)) {
                                System.out.println("Removing row: " + row);
                                iterator.remove();
                                removedCount++;
                                for (Map.Entry<String, Object> entry : row.entrySet()) {
                                    String column = entry.getKey();
                                    Object value = entry.getValue();
                                    Map<Object, Set<Map<String, Object>>> hashIndex = hashIndexes.get(tableName).get(column);
                                    if (hashIndex != null && hashIndex.get(value) != null) {
                                        hashIndex.get(value).remove(row);
                                        if (hashIndex.get(value).isEmpty()) {
                                            hashIndex.remove(value);
                                        }
                                    }
                                    TreeMap<Object, Set<Map<String, Object>>> btreeIndex = btreeIndexes.get(tableName).get(column);
                                    if (btreeIndex != null && btreeIndex.get(value) != null) {
                                        btreeIndex.get(value).remove(row);
                                        if (btreeIndex.get(value).isEmpty()) {
                                            btreeIndex.remove(value);
                                        }
                                    }
                                }
                            }
                        }
                        if (removedCount > 0) {
                            dirtyTables.put(tableName, true);
                        }
                        System.out.println("Removed " + removedCount + " rows from " + tableName);
                        System.out.println("Table " + tableName + " after rollback: " + table);
                    }
                    uncommittedInserts.clear();
                    System.out.println("Uncommitted inserts cleared");
                }
            }
            inTransaction = false;
            cleanupTransaction();
        }

        private void cleanupTransaction() {
            transactionTables.clear();
            transactionDirtyRows.clear();
            deletedRows.clear();
            uncommittedInserts.clear();
        }

        private String setIsolationLevel(String level) {
            try {
                isolationLevel = IsolationLevel.valueOf(level.trim());
                return "OK: Isolation level set to " + isolationLevel;
            } catch (IllegalArgumentException e) {
                return "ERROR: Invalid isolation level - " + level;
            }
        }

        private Map<String, List<Map<String, Object>>> getWorkingTables(String cmd) {
            if (!inTransaction) {
                return tables;
            }
            if (cmd.equals("SELECT") && isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                return tables;
            }
            return transactionTables;
        }

        private Object parseValue(String value) {
            try {
                if (value.startsWith("'") && value.endsWith("'")) {
                    return value.substring(1, value.length() - 1);
                }
                if (value.contains(".")) {
                    return new BigDecimal(value);
                }
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    return sdf.parse(value);
                } catch (Exception ex) {
                    return value;
                }
            }
        }

        private boolean isValidType(Object value, String type) {
            switch (type.toUpperCase()) {
                case "INT": return value instanceof Integer;
                case "DECIMAL": return value instanceof BigDecimal;
                case "VARCHAR": return value instanceof String;
                case "DATE": return value instanceof Date;
                default: return false;
            }
        }

        private String formatValue(Object value) {
            if (value == null) return "NULL";
            if (value instanceof String) return "'" + value + "'";
            if (value instanceof Date) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format((Date) value);
            }
            return value.toString();
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
                    System.out.println("Condition: " + expression + ", Rows: " + rows);
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

            System.out.println("Final result from evaluateWithIndexes: " + result);
            return new ArrayList<>(new LinkedHashSet<>(result));
        }

        private Set<Map<String, Object>> evaluateSingleCondition(String tableName, String expression,
                                                                 Map<String, List<Map<String, Object>>> tables) {
            String[] parts = expression.split("=");
            if (parts.length != 2) return null;

            String column = parts[0].trim();
            Object value = parseValue(parts[1].trim());
            Map<Object, Set<Map<String, Object>>> hashIndex = hashIndexes.get(tableName).get(column);
            if (hashIndex != null && hashIndex.containsKey(value)) {
                return new HashSet<>(hashIndex.get(value));
            }
            TreeMap<Object, Set<Map<String, Object>>> btreeIndex = btreeIndexes.get(tableName).get(column);
            if (btreeIndex != null && btreeIndex.containsKey(value)) {
                return new HashSet<>(btreeIndex.get(value));
            }

            Set<Map<String, Object>> result = new HashSet<>();
            for (Map<String, Object> row : tables.get(tableName)) {
                if (!deletedRows.contains(row) && value.equals(row.get(column))) {
                    result.add(row);
                }
            }
            return result;
        }

        private void cleanIndexes(String tableName) {
            for (Map<String, ? extends Map<Object, Set<Map<String, Object>>>> index : Arrays.asList(hashIndexes.get(tableName), btreeIndexes.get(tableName))) {
                for (Map<Object, Set<Map<String, Object>>> colIndex : index.values()) {
                    colIndex.values().forEach(set -> set.removeIf(deletedRows::contains));
                    colIndex.entrySet().removeIf(e -> e.getValue().isEmpty());
                }
            }
        }
    }
}