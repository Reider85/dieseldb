package diesel;

import java.util.List;
import java.util.Map;

/**
 * Query that returns the CREATE TABLE DDL statement for a specific table.
 */
class ShowCreateTableQuery implements Query<List<Map<String, Object>>> {

    private final String tableName;

    /**
     * Creates a new SHOW CREATE TABLE query.
     *
     * @param tableName the table name to get DDL for
     */
    public ShowCreateTableQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return execute(table.getDatabase(), -1);
    }
    
    @Override
    public List<Map<String, Object>> execute(Database database, long txid) {
        Table t = database.getTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Generate CREATE TABLE DDL
        String ddl = generateCreateTableDdl(t);
        
        // Return as list with "table" and "create_table" columns
        java.util.List<Map<String, Object>> results = new java.util.ArrayList<>();
        java.util.Map<String, Object> row = new java.util.HashMap<>();
        row.put("table", tableName);
        row.put("create_table", ddl);
        results.add(row);
        
        return results;
    }

    /**
     * Generates the CREATE TABLE DDL statement for the given table.
     *
     * @param table the table to generate DDL for
     * @return the CREATE TABLE statement
     */
    private String generateCreateTableDdl(Table table) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(table.getName()).append(" (\n");
        
        // Add columns
        for (String column : table.getColumns()) {
            Class<?> type = table.getColumnTypes().get(column);
            ddl.append("    ").append(column).append(" ").append(getSqlTypeName(type));
            
            // Add PRIMARY KEY constraint
            if (column.equals(table.getPrimaryKeyColumn())) {
                ddl.append(" PRIMARY KEY");
            }
            
            // Add SEQUENCE constraint
            Map<String, Sequence> sequences = table.getSequences();
            if (sequences.containsKey(column)) {
                Sequence sequence = sequences.get(column);
                ddl.append(" SEQUENCE(").append(sequence.getName()).append(" ");
                ddl.append(sequence.getStartValue()).append(" ");
                ddl.append(sequence.getIncrement()).append(")");
            }
            
            ddl.append(",\n");
        }
        
        // Remove trailing comma and add closing parenthesis
        if (table.getColumns().size() > 0) {
            ddl.setLength(ddl.length() - 2);
            ddl.append("\n");
        }
        ddl.append(")");
        
        return ddl.toString();
    }

    /**
     * Converts Java type names to SQL type names.
     *
     * @param type the Java type class
     * @return the SQL type name
     */
    private String getSqlTypeName(Class<?> type) {
        if (type == String.class) {
            return "STRING";
        } else if (type == Integer.class || type == int.class) {
            return "INTEGER";
        } else if (type == Long.class || type == long.class) {
            return "LONG";
        } else if (type == Short.class || type == short.class) {
            return "SHORT";
        } else if (type == Byte.class || type == byte.class) {
            return "BYTE";
        } else if (type == java.math.BigDecimal.class) {
            return "BIGDECIMAL";
        } else if (type == Float.class || type == float.class) {
            return "FLOAT";
        } else if (type == Double.class || type == double.class) {
            return "DOUBLE";
        } else if (type == Character.class || type == char.class) {
            return "CHAR";
        } else if (type == java.util.UUID.class) {
            return "UUID";
        } else if (type == Boolean.class || type == boolean.class) {
            return "BOOLEAN";
        } else if (type == java.time.LocalDate.class) {
            return "DATE";
        } else if (type == java.time.LocalDateTime.class) {
            return "DATETIME";
        } else if (type == java.time.Instant.class) {
            return "DATETIME_MS";
        } else {
            return "STRING"; // Default fallback
        }
    }

    @Override
    public String toString() {
        return "SHOW CREATE TABLE " + tableName;
    }
}