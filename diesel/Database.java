package diesel;
import java.util.*;

class Database {
    private final Map<String, Table> tables = new HashMap<>();

    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        if (!tables.containsKey(tableName)) {
            tables.put(tableName, new Table(columns, columnTypes));
        } else {
            throw new IllegalArgumentException("Table " + tableName + " already exists");
        }
    }

    public Object executeQuery(String query) {
        Query<?> parsedQuery = new QueryParser().parse(query);
        if (parsedQuery instanceof CreateTableQuery) {
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
        table.saveToFile(tableName);
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
}