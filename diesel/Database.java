package diesel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class Database {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<UUID, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_UNCOMMITTED;

    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        if (!tables.containsKey(tableName)) {
            Table newTable = new Table(columns, columnTypes);
            tables.put(tableName, newTable);
            for (Transaction transaction : activeTransactions.values()) {
                if (transaction.isActive()) {
                    transaction.updateTable(tableName, newTable);
                }
            }
        } else {
            throw new IllegalArgumentException("Table " + tableName + " already exists");
        }
    }

    public Object executeQuery(String query, UUID transactionId) {
        Query<?> parsedQuery = new QueryParser().parse(query);
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;

        if (parsedQuery instanceof SetIsolationLevelQuery) {
            SetIsolationLevelQuery isolationQuery = (SetIsolationLevelQuery) parsedQuery;
            defaultIsolationLevel = isolationQuery.getIsolationLevel();
            return "Isolation level set to " + defaultIsolationLevel;
        } else if (parsedQuery instanceof BeginTransactionQuery) {
            if (currentTransaction != null && currentTransaction.isActive()) {
                throw new IllegalStateException("Another transaction is already active for this client");
            }
            IsolationLevel isolationLevel = ((BeginTransactionQuery) parsedQuery).getIsolationLevel() != null
                    ? ((BeginTransactionQuery) parsedQuery).getIsolationLevel()
                    : defaultIsolationLevel;
            currentTransaction = new Transaction(isolationLevel);
            transactionId = currentTransaction.getTransactionId();
            activeTransactions.put(transactionId, currentTransaction);
            for (Map.Entry<String, Table> entry : tables.entrySet()) {
                currentTransaction.snapshotTable(entry.getKey(), entry.getValue());
            }
            return "Transaction started: " + transactionId;
        } else if (parsedQuery instanceof CommitTransactionQuery) {
            if (currentTransaction == null || !currentTransaction.isActive()) {
                throw new IllegalStateException("No active transaction to commit");
            }
            // Apply modified tables
            for (Map.Entry<String, Table> entry : currentTransaction.getModifiedTables().entrySet()) {
                if (entry.getValue() != null) {
                    tables.put(entry.getKey(), entry.getValue());
                    entry.getValue().saveToFile(entry.getKey());
                } else {
                    tables.remove(entry.getKey());
                }
            }
            currentTransaction.setInactive();
            activeTransactions.remove(transactionId);
            return "Transaction committed";
        } else if (parsedQuery instanceof RollbackTransactionQuery) {
            if (currentTransaction == null || !currentTransaction.isActive()) {
                throw new IllegalStateException("No active transaction to rollback");
            }
            currentTransaction.setInactive();
            activeTransactions.remove(transactionId);
            return "Transaction rolled back";
        } else if (parsedQuery instanceof CreateTableQuery) {
            CreateTableQuery createQuery = (CreateTableQuery) parsedQuery;
            createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes());
            return "Table created successfully";
        }

        String tableName = extractTableName(query);
        Table table = getTableForQuery(tableName, currentTransaction);
        if (table == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        Object result = parsedQuery.execute(table);
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(tableName, table);
        } else {
            table.saveToFile(tableName);
        }
        return result;
    }

    private Table getTableForQuery(String tableName, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            // If in transaction, prefer modified table
            Table modifiedTable = currentTransaction.getModifiedTables().get(tableName);
            if (modifiedTable != null) {
                return modifiedTable;
            }
            // For READ_UNCOMMITTED, check other transactions' modified tables
            if (currentTransaction.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                for (Transaction otherTransaction : activeTransactions.values()) {
                    if (otherTransaction != currentTransaction && otherTransaction.isActive()) {
                        Table otherModifiedTable = otherTransaction.getModifiedTables().get(tableName);
                        if (otherModifiedTable != null) {
                            return otherModifiedTable; // Dirty read
                        }
                    }
                }
            }
            // Fallback to original table
            return currentTransaction.getOriginalTables().get(tableName);
        }
        // Outside transaction, use committed table
        return tables.get(tableName);
    }

    private String extractTableName(String query) {
        String normalized = query.trim().toUpperCase();
        if (normalized.startsWith("SELECT")) {
            return normalized.split("FROM")[1].trim().split(" ")[0];
        } else if (normalized.startsWith("INSERT INTO")) {
            return normalized.split(" ")[2].split("\\(")[0];
        } else if (normalized.startsWith("UPDATE")) {
            return normalized.split(" ")[1].split(" ")[0];
        }
        throw new IllegalArgumentException("Cannot extract table name from query");
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
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, null);
            }
        }
    }

    public boolean isInTransaction(UUID transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        return transaction != null && transaction.isActive();
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