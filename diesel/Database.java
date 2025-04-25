package diesel;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class Database {
    private static final Logger LOGGER = Logger.getLogger(Database.class.getName());
    private final Map<String, Table> tables = new HashMap<>();
    private final QueryParser parser = new QueryParser();

    public void createTable(String name, List<String> columns) {
        Table table = new Table(columns);
        tables.put(name, table);
        table.loadFromFile(name); // Load existing data if any
        LOGGER.log(Level.INFO, "Created table: {0} with columns: {1}", new Object[]{name, columns});
    }

    public void insert(String tableName, Map<String, String> row) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.addRow(row);
            table.saveToFile(tableName); // Save after insert
            LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{tableName, row});
        } else {
            LOGGER.log(Level.WARNING, "Table not found for insert: {0}", tableName);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T executeQuery(String query) {
        try {
            LOGGER.log(Level.INFO, "Executing query: {0}", query);
            Query<T> parsedQuery = (Query<T>) parser.parse(query);
            String tableName = extractTableName(query);
            Table table = tables.get(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
            T result = parsedQuery.execute(table);
            // Save table after insert or update queries
            if (parsedQuery instanceof InsertQuery || parsedQuery instanceof UpdateQuery) {
                table.saveToFile(tableName);
            }
            if (result instanceof List) {
                LOGGER.log(Level.INFO, "Query returned {0} rows", new Object[]{((List<?>) result).size()});
            } else {
                LOGGER.log(Level.INFO, "Insert or Update query executed successfully");
            }
            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    private String extractTableName(String query) {
        String normalized = query.toUpperCase().trim();

        if (normalized.startsWith("INSERT INTO")) {
            String afterInsert = normalized.replace("INSERT INTO", "").trim();
            int parenIndex = afterInsert.indexOf("(");
            if (parenIndex == -1) {
                return "";
            }
            return afterInsert.substring(0, parenIndex).trim();
        } else if (normalized.startsWith("UPDATE")) {
            String afterUpdate = normalized.replace("UPDATE", "").trim();
            int setIndex = afterUpdate.indexOf("SET");
            if (setIndex == -1) {
                return "";
            }
            return afterUpdate.substring(0, setIndex).trim();
        } else if (normalized.contains("FROM")) {
            String[] parts = normalized.split("FROM");
            if (parts.length < 2) {
                return "";
            }
            String tablePart = parts[1].trim();
            return tablePart.split(" ")[0].trim();
        }

        return "";
    }
}