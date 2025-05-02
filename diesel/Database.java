package diesel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.util.logging.Level;
import java.util.logging.Logger;

class Database {
    private static final Logger LOGGER = Logger.getLogger(Database.class.getName());
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<UUID, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_UNCOMMITTED;

    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn) {
        if (!tables.containsKey(tableName)) {
            Table newTable = new Table(this, tableName, columns, columnTypes, primaryKeyColumn, new HashMap<String, Sequence>());
            tables.put(tableName, newTable);
            for (Transaction transaction : activeTransactions.values()) {
                if (transaction.isActive()) {
                    transaction.updateTable(tableName, newTable);
                }
            }
            LOGGER.log(Level.INFO, "Created table {0} with primary key {1}", new Object[]{tableName, primaryKeyColumn});
        } else {
            throw new IllegalArgumentException("Table " + tableName + " already exists");
        }
    }

    public Object executeQuery(String query, UUID transactionId) {
        LOGGER.log(Level.FINE, "Executing query: {0}", query);
        Query<?> parsedQuery = new QueryParser().parse(query, this);
        LOGGER.log(Level.FINE, "Parsed query type: {0}", parsedQuery.getClass().getSimpleName());
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;

        try {
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
                createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes(), createQuery.getPrimaryKeyColumn());
                Table table = getTable(createQuery.getTableName());
                for (Map.Entry<String, Sequence> entry : createQuery.getSequences().entrySet()) {
                    table.getSequences().put(entry.getKey(), entry.getValue());
                }
                return "Table created successfully";
            } else if (parsedQuery instanceof CreateIndexQuery) {
                CreateIndexQuery indexQuery = (CreateIndexQuery) parsedQuery;
                Table table = getTable(indexQuery.getTableName());
                indexQuery.execute(table);
                if (currentTransaction != null && currentTransaction.isActive()) {
                    currentTransaction.updateTable(indexQuery.getTableName(), table);
                }
                return "B-tree index created successfully on " + indexQuery.getTableName() + "." + indexQuery.getColumnName();
            } else if (parsedQuery instanceof CreateHashIndexQuery) {
                CreateHashIndexQuery indexQuery = (CreateHashIndexQuery) parsedQuery;
                Table table = getTable(indexQuery.getTableName());
                indexQuery.execute(table);
                if (currentTransaction != null && currentTransaction.isActive()) {
                    currentTransaction.updateTable(indexQuery.getTableName(), table);
                }
                return "Hash index created successfully on " + indexQuery.getTableName() + "." + indexQuery.getColumnName();
            } else if (parsedQuery instanceof CreateUniqueIndexQuery) {
                CreateUniqueIndexQuery indexQuery = (CreateUniqueIndexQuery) parsedQuery;
                Table table = getTable(indexQuery.getTableName());
                indexQuery.execute(table);
                if (currentTransaction != null && currentTransaction.isActive()) {
                    currentTransaction.updateTable(indexQuery.getTableName(), table);
                }
                return "Unique index created successfully on " + indexQuery.getTableName() + "." + indexQuery.getColumnName();
            } else if (parsedQuery instanceof CreateUniqueClusteredIndexQuery) {
                CreateUniqueClusteredIndexQuery indexQuery = (CreateUniqueClusteredIndexQuery) parsedQuery;
                Table table = getTable(indexQuery.getTableName());
                indexQuery.execute(table);
                if (currentTransaction != null && currentTransaction.isActive()) {
                    currentTransaction.updateTable(indexQuery.getTableName(), table);
                }
                return "Unique clustered index created successfully on " + indexQuery.getTableName() + "." + indexQuery.getColumnName();
            }

            LOGGER.log(Level.FINE, "Calling extractTableName for query: {0}", query);
            String tableName = extractTableName(query);
            LOGGER.log(Level.FINE, "Extracted table name: {0}", tableName);
            Table table = getTableForQuery(tableName, currentTransaction);
            if (table == null) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            Object result = parsedQuery.execute(table);
            if (currentTransaction != null && currentTransaction.isActive()) {
                currentTransaction.updateTable(tableName, table);
            } else if (parsedQuery instanceof InsertQuery || parsedQuery instanceof UpdateQuery || parsedQuery instanceof DeleteQuery) {
                table.saveToFile(tableName);
            }
            return (parsedQuery instanceof DeleteQuery) ? result : result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}", e.getMessage());
            throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
        }
    }

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

    private String extractTableName(String query) {
        LOGGER.log(Level.FINE, "Raw query for table name extraction: {0}", query);
        String normalized = query.trim().toUpperCase();
        LOGGER.log(Level.FINE, "Extracting table name from normalized query: {0}", normalized);
        if (normalized.startsWith("SELECT")) {
            LOGGER.log(Level.FINE, "Processing SELECT query");
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid SELECT format");
            }
            LOGGER.log(Level.FINE, "SELECT query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String tablePart = parts[1].split("(?i)WHERE")[0].trim();
            LOGGER.log(Level.FINE, "Table part after WHERE split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in SELECT");
            }
            return tablePart;
        } else if (normalized.startsWith("INSERT INTO")) {
            LOGGER.log(Level.FINE, "Processing INSERT query");
            String[] parts = normalized.split("(?i)INSERT INTO\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid INSERT format");
            }
            LOGGER.log(Level.FINE, "INSERT query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String tablePart = parts[1].split("\\s+|\\(")[0].trim();
            LOGGER.log(Level.FINE, "Table part after split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in INSERT");
            }
            return tablePart;
        } else if (normalized.startsWith("UPDATE")) {
            LOGGER.log(Level.FINE, "Processing UPDATE query");
            String[] parts = normalized.split("(?i)UPDATE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid UPDATE format");
            }
            LOGGER.log(Level.FINE, "UPDATE query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String tablePart = parts[1].split("\\s+")[0].trim();
            LOGGER.log(Level.FINE, "Table part after split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in UPDATE");
            }
            return tablePart;
        } else if (normalized.startsWith("DELETE FROM")) {
            LOGGER.log(Level.FINE, "Processing DELETE query");
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid DELETE format");
            }
            LOGGER.log(Level.FINE, "DELETE query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String[] whereParts = parts[1].split("(?i)WHERE\\s*", 2);
            String tablePart = whereParts[0].trim();
            LOGGER.log(Level.FINE, "Table part after WHERE split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in DELETE");
            }
            return tablePart;
        } else if (normalized.startsWith("CREATE TABLE")) {
            LOGGER.log(Level.FINE, "Processing CREATE TABLE query");
            String[] parts = normalized.split("(?i)CREATE TABLE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE TABLE format");
            }
            LOGGER.log(Level.FINE, "CREATE TABLE query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String tablePart = parts[1].split("\\s+")[0].trim();
            LOGGER.log(Level.FINE, "Table part after split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in CREATE TABLE");
            }
            return tablePart;
        } else if (normalized.startsWith("CREATE INDEX") || normalized.startsWith("CREATE HASH INDEX") ||
                normalized.startsWith("CREATE UNIQUE INDEX") || normalized.startsWith("CREATE UNIQUE CLUSTERED INDEX")) {
            LOGGER.log(Level.FINE, "Processing CREATE INDEX query");
            String[] parts = normalized.split("(?i)ON\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE INDEX format");
            }
            LOGGER.log(Level.FINE, "CREATE INDEX query parts: part0={0}, part1={1}", new Object[]{parts[0], parts[1]});
            String tablePart = parts[1].split("\\s+")[0].trim();
            LOGGER.log(Level.FINE, "Table part after split: {0}", tablePart);
            if (tablePart.isEmpty()) {
                throw new IllegalArgumentException("Cannot extract table name from query: table name missing in CREATE INDEX");
            }
            return tablePart;
        }
        throw new IllegalArgumentException("Cannot extract table name from query: unsupported query type");
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

    public boolean isInTransaction(UUID transactionId)  {
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