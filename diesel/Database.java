package diesel;
import java.util.*;

class Database {
    private final Map<String, Table> tables = new HashMap<>();
    private Transaction currentTransaction = null;

    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        if (!tables.containsKey(tableName)) {
            Table newTable = new Table(columns, columnTypes);
            tables.put(tableName, newTable);
            if (currentTransaction != null && currentTransaction.isActive()) {
                currentTransaction.updateTable(tableName, newTable);
            }
        } else {
            throw new IllegalArgumentException("Table " + tableName + " already exists");
        }
    }

    public Object executeQuery(String query) {
        Query<?> parsedQuery = new QueryParser().parse(query);
        if (parsedQuery instanceof BeginTransactionQuery) {
            if (currentTransaction != null && currentTransaction.isActive()) {
                throw new IllegalStateException("Another transaction is already active");
            }
            currentTransaction = new Transaction();
            for (Map.Entry<String, Table> entry : tables.entrySet()) {
                currentTransaction.snapshotTable(entry.getKey(), entry.getValue());
            }
            return "Transaction started: " + currentTransaction.getTransactionId();
        } else if (parsedQuery instanceof CommitTransactionQuery) {
            if (currentTransaction == null || !currentTransaction.isActive()) {
                throw new IllegalStateException("No active transaction to commit");
            }
            // Apply modified tables
            for (Map.Entry<String, Table> entry : currentTransaction.getModifiedTables().entrySet()) {
                tables.put(entry.getKey(), entry.getValue());
                entry.getValue().saveToFile(entry.getKey());
            }
            currentTransaction.setInactive();
            currentTransaction = null;
            return "Transaction committed";
        } else if (parsedQuery instanceof RollbackTransactionQuery) {
            if (currentTransaction == null || !currentTransaction.isActive()) {
                throw new IllegalStateException("No active transaction to rollback");
            }
            // Restore original tables
            tables.clear();
            for (Map.Entry<String, Table> entry : currentTransaction.getOriginalTables().entrySet()) {
                tables.put(entry.getKey(), entry.getValue());
            }
            currentTransaction.setInactive();
            currentTransaction = null;
            return "Transaction rolled back";
        } else if (parsedQuery instanceof CreateTableQuery) {
            CreateTableQuery createQuery = (CreateTableQuery) parsedQuery;
            createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes());
            return "Table created successfully";
        }
        String tableName = extractTableName(query);
        Table table = tables.get(tableName);
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
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(tableName, null);
        }
    }

    public boolean isInTransaction() {
        return currentTransaction != null && currentTransaction.isActive();
    }
}