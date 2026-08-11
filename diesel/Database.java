package diesel;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central database engine. Owns the shared table map, the active client
 * transactions and the auto-commit flag, parses incoming SQL and dispatches
 * each parsed query to its dedicated execution path.
 */
class Database {
    private static final Logger LOGGER = Logger.getLogger(Database.class.getName());
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<UUID, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_UNCOMMITTED;
    private boolean autoCommit = true;
    private String dataDir = ".";

    public Database() {
    }

    public Database(String dataDir) {
        if (dataDir != null && !dataDir.isEmpty()) {
            this.dataDir = dataDir;
        }
        File dir = new File(this.dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        if (dataDir != null && !dataDir.isEmpty()) {
            this.dataDir = dataDir;
        }
        File dir = new File(this.dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn) {
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " already exists");
        }
        Table newTable = new Table(this, tableName, columns, columnTypes, primaryKeyColumn, new HashMap<String, Sequence>());
        tables.put(tableName, newTable);
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, newTable);
            }
        }
        LOGGER.log(Level.INFO, "Created table {0} with primary key {1}", new Object[]{tableName, primaryKeyColumn});
    }

    /**
     * Parses and executes a query. {@code transactionId} is the caller's active
     * transaction session, or null when the caller is not in a transaction.
     */
    public Object executeQuery(String query, UUID transactionId) {
        LOGGER.log(Level.FINE, "Executing query: {0}", query);
        Query<?> parsedQuery = parse(query);
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;

        try {
            if (parsedQuery instanceof SetIsolationLevelQuery) {
                return executeSetIsolationLevel((SetIsolationLevelQuery) parsedQuery);
            }
            if (parsedQuery instanceof SetAutoCommitQuery) {
                return executeSetAutoCommit((SetAutoCommitQuery) parsedQuery);
            }
            if (parsedQuery instanceof BeginTransactionQuery) {
                return executeBeginTransaction((BeginTransactionQuery) parsedQuery, currentTransaction);
            }
            if (parsedQuery instanceof CommitTransactionQuery) {
                return executeCommit(currentTransaction, transactionId);
            }
            if (parsedQuery instanceof RollbackTransactionQuery) {
                return executeRollback(currentTransaction, transactionId);
            }
            if (parsedQuery instanceof CreateTableQuery) {
                return executeCreateTable((CreateTableQuery) parsedQuery);
            }
            if (parsedQuery instanceof CreateIndexQueryBase) {
                return executeCreateIndex((CreateIndexQueryBase) parsedQuery, currentTransaction);
            }
            return executeDataQuery(parsedQuery, query, currentTransaction);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}", e.getMessage());
            throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
        }
    }

    private Query<?> parse(String query) {
        SubqueryParser subqueryParser = new SubqueryParser();
        return subqueryParser.containsSubquery(query)
                ? subqueryParser.parse(query, this)
                : new QueryParser().parse(query, this);
    }

    private Object executeSetIsolationLevel(SetIsolationLevelQuery isolationQuery) {
        defaultIsolationLevel = isolationQuery.getIsolationLevel();
        return "Isolation level set to " + defaultIsolationLevel;
    }

    private Object executeSetAutoCommit(SetAutoCommitQuery autoCommitQuery) {
        setAutoCommit(autoCommitQuery.isAutoCommit());
        return "AUTOCOMMIT set to " + (autoCommitQuery.isAutoCommit() ? "ON" : "OFF");
    }

    private Object executeBeginTransaction(BeginTransactionQuery beginQuery, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            throw new IllegalStateException("Another transaction is already active for this client");
        }
        IsolationLevel isolationLevel = beginQuery.getIsolationLevel() != null
                ? beginQuery.getIsolationLevel()
                : defaultIsolationLevel;
        Transaction transaction = new Transaction(isolationLevel);
        UUID newTransactionId = transaction.getTransactionId();
        activeTransactions.put(newTransactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        setAutoCommit(false);
        return "Transaction started: " + newTransactionId;
    }

    private Object executeCommit(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new IllegalStateException("No active transaction to commit");
        }
        persistModifiedTables(currentTransaction.getModifiedTables(), true);
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return "Transaction committed";
    }

    private Object executeRollback(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new IllegalStateException("No active transaction to rollback");
        }
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return "Transaction rolled back";
    }

    private Object executeCreateTable(CreateTableQuery createQuery) {
        createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes(), createQuery.getPrimaryKeyColumn());
        Table table = getTable(createQuery.getTableName());
        for (Map.Entry<String, Sequence> entry : createQuery.getSequences().entrySet()) {
            table.getSequences().put(entry.getKey(), entry.getValue());
        }
        return "Table created successfully";
    }

    private Object executeCreateIndex(CreateIndexQueryBase indexQuery, Transaction currentTransaction) {
        Table table = getTable(indexQuery.getTableName());
        indexQuery.execute(table);
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(indexQuery.getTableName(), table);
        }
        return indexDescription(indexQuery) + " created successfully on "
                + indexQuery.getTableName() + "." + indexQuery.getColumnName();
    }

    private String indexDescription(CreateIndexQueryBase indexQuery) {
        if (indexQuery instanceof CreateIndexQuery) {
            return "B-tree index";
        }
        if (indexQuery instanceof CreateHashIndexQuery) {
            return "Hash index";
        }
        if (indexQuery instanceof CreateUniqueIndexQuery) {
            return "Unique index";
        }
        if (indexQuery instanceof CreateUniqueClusteredIndexQuery) {
            return "Unique clustered index";
        }
        throw new IllegalArgumentException("Unsupported index query type: " + indexQuery.getClass().getSimpleName());
    }

    /**
     * Executes queries that operate on table data (INSERT/UPDATE/DELETE/SELECT),
     * applying the transaction view, auto-commit DML and persistence rules.
     */
    private Object executeDataQuery(Query<?> parsedQuery, String query, Transaction currentTransaction) {
        String tableName = extractTableName(query);
        Table table = getTableForQuery(tableName, currentTransaction);
        if (table == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }

        boolean isDml = parsedQuery instanceof InsertQuery
                || parsedQuery instanceof UpdateQuery
                || parsedQuery instanceof DeleteQuery;

        // Auto-commit DML runs in a short-lived implicit transaction and is persisted immediately.
        if (autoCommit && isDml && (currentTransaction == null || !currentTransaction.isActive())) {
            Transaction implicitTransaction = new Transaction(defaultIsolationLevel);
            try {
                Object implicitResult = parsedQuery.execute(table);
                implicitTransaction.registerModifiedTable(tableName, table);
                persistModifiedTables(implicitTransaction.getModifiedTables(), false);
                return implicitResult;
            } finally {
                implicitTransaction.setInactive();
            }
        }

        // DML inside an explicit transaction records a copy for the eventual COMMIT.
        if (isDml) {
            Object dmlResult = parsedQuery.execute(table);
            if (currentTransaction != null && currentTransaction.isActive()) {
                currentTransaction.updateTable(tableName, table);
            } else {
                table.saveToFile(tableName);
            }
            return dmlResult;
        }

        return parsedQuery.execute(table);
    }

    /**
     * Publishes transaction-modified tables back into the shared table map and
     * writes them to disk. A null value means the table was dropped in the
     * transaction. Auto-commit DML persists only the CSV, while an explicit
     * COMMIT also writes the serialized table file.
     */
    private void persistModifiedTables(Map<String, Table> modifiedTables, boolean writeSerialized) {
        for (Map.Entry<String, Table> entry : modifiedTables.entrySet()) {
            String tableName = entry.getKey();
            Table modifiedTable = entry.getValue();
            if (modifiedTable != null) {
                tables.put(tableName, modifiedTable);
                modifiedTable.saveToFile(tableName);
                if (writeSerialized) {
                    modifiedTable.saveToSerializedFile(tableName);
                }
            } else {
                tables.remove(tableName);
                deleteTableFiles(tableName);
            }
        }
    }

    /**
     * Returns the table a query operates on, honoring the caller's transaction:
     * an active transaction first sees its own modified copy, then the copies of
     * other READ_UNCOMMITTED transactions, and finally its BEGIN-time snapshot.
     */
    private Table getTableForQuery(String tableName, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            Table modifiedTable = currentTransaction.getModifiedTables().get(tableName);
            if (modifiedTable != null) {
                return modifiedTable;
            }
            if (currentTransaction.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                for (Transaction otherTransaction : activeTransactions.values()) {
                    if (otherTransaction != currentTransaction && otherTransaction.isActive()) {
                        Table otherModifiedTable = otherTransaction.getModifiedTables().get(tableName);
                        if (otherModifiedTable != null) {
                            return otherModifiedTable;
                        }
                    }
                }
            }
            return currentTransaction.getOriginalTables().get(tableName);
        }
        return tables.get(tableName);
    }

    /** Extracts the name of the table a query operates on from the normalized query text. */
    private String extractTableName(String query) {
        String normalized = QueryParser.toUpperCasePreservingQuotedIdentifiers(query.trim());
        if (normalized.startsWith("SELECT")) {
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid SELECT format");
            }
            // The first table appears before any INNER JOIN or WHERE clause and may carry an alias.
            return firstIdentifier(parts[1].split("(?i)(INNER JOIN|WHERE)\\s")[0].trim().split("\\s+")[0]);
        }
        if (normalized.startsWith("INSERT INTO")) {
            String[] parts = normalized.split("(?i)INSERT INTO\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid INSERT format");
            }
            return firstIdentifier(parts[1].split("\\s+|\\(")[0]);
        }
        if (normalized.startsWith("UPDATE")) {
            String[] parts = normalized.split("(?i)UPDATE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid UPDATE format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        if (normalized.startsWith("DELETE FROM")) {
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid DELETE format");
            }
            return firstIdentifier(parts[1].split("(?i)WHERE\\s*", 2)[0]);
        }
        if (normalized.startsWith("CREATE TABLE")) {
            String[] parts = normalized.split("(?i)CREATE TABLE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE TABLE format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        if (normalized.startsWith("CREATE INDEX") || normalized.startsWith("CREATE HASH INDEX")
                || normalized.startsWith("CREATE UNIQUE INDEX") || normalized.startsWith("CREATE UNIQUE CLUSTERED INDEX")) {
            String[] parts = normalized.split("(?i)ON\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE INDEX format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        throw new IllegalArgumentException("Cannot extract table name from query: unsupported query type");
    }

    private String firstIdentifier(String token) {
        String tableName = QueryParser.unquoteIdentifier(token.trim());
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("Cannot extract table name from query: table name missing");
        }
        return tableName;
    }

    public Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        return table;
    }

    public void dropTable(String tableName) {
        if (tables.remove(tableName) == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        deleteTableFiles(tableName);
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, null);
            }
        }
    }

    public void saveTablesToDisk() {
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            entry.getValue().saveToSerializedFile(entry.getKey());
        }
        LOGGER.log(Level.INFO, "Saved {0} tables to disk", tables.size());
    }

    public void loadTablesFromDisk() {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".table"));
        if (files == null) {
            return;
        }
        for (File file : files) {
            String tableName = file.getName().substring(0, file.getName().length() - ".table".length());
            Table table = Table.loadFromFile(this, tableName);
            if (table != null) {
                tables.put(tableName, table);
                LOGGER.log(Level.INFO, "Loaded table {0} from disk with {1} rows",
                        new Object[]{tableName, table.getRows().size()});
            }
        }
    }

    private void deleteTableFiles(String tableName) {
        new File(dataDir + File.separator + tableName + ".csv").delete();
        new File(dataDir + File.separator + tableName + ".table").delete();
    }

    public boolean isInTransaction(UUID transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        return transaction != null && transaction.isActive();
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public UUID beginTransaction(IsolationLevel isolationLevel) {
        Transaction transaction = new Transaction(isolationLevel);
        UUID transactionId = transaction.getTransactionId();
        activeTransactions.put(transactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        return transactionId;
    }
}
