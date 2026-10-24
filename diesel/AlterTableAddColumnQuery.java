package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Query DTO for ALTER TABLE ... ADD COLUMN (step 1: nullable metadata-only).
 * Routes through internal Database.applySchemaChange API.
 * 
 * @see Database#applySchemaChange(String, List, Map, String)
 * @see diesel.schema.SchemaVersion
 */
public final class AlterTableAddColumnQuery implements Query<String> {
    private final String tableName;
    private final String columnName;
    private final Class<?> columnType;
    private final diesel.schema.AlterAlgorithm algorithm;
    
    /**
     * Creates a metadata-only ALTER TABLE ADD COLUMN query (R3-010 step 1).
     * 
     * @param tableName    Name of the table to modify
     * @param columnName   Name of the new column (must not exist)
     * @param columnType   Type of the new column (nullable metadata-only)
     * @throws IllegalArgumentException if any argument is null
     */
    public AlterTableAddColumnQuery(String tableName, String columnName, Class<?> columnType) {
        this(tableName, columnName, columnType, diesel.schema.AlterAlgorithm.DEFAULT);
    }
    
    /**
     * Creates an ALTER TABLE ADD COLUMN query with an explicit algorithm hint.
     * 
     * @param tableName    Name of the table to modify
     * @param columnName   Name of the new column (must not exist)
     * @param columnType   Type of the new column (nullable)
     * @param algorithm    DEFAULT (instant) or COPY (background rewrite, prompt4 #38)
     * @throws IllegalArgumentException if any argument is null
     */
    public AlterTableAddColumnQuery(String tableName, String columnName, Class<?> columnType,
                                    diesel.schema.AlterAlgorithm algorithm) {
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        this.columnName = Objects.requireNonNull(columnName, "Column name cannot be null");
        this.columnType = Objects.requireNonNull(columnType, "Column type cannot be null");
        this.algorithm = Objects.requireNonNull(algorithm, "Algorithm cannot be null");
        
        if (tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        if (columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be empty");
        }
        
        // Validate supported types for step 1 (same as CREATE TABLE)
        validateColumnType(columnType);
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public String getColumnName() {
        return columnName;
    }
    
    public Class<?> getColumnType() {
        return columnType;
    }
    
    /** DEFAULT (instant) or COPY (background rewrite). */
    public diesel.schema.AlterAlgorithm getAlgorithm() {
        return algorithm;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterTableAddColumnQuery that = (AlterTableAddColumnQuery) o;
        return tableName.equals(that.tableName) &&
               columnName.equals(that.columnName) &&
               columnType.equals(that.columnType) &&
               algorithm == that.algorithm;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName, columnType, algorithm);
    }
    
    @Override
    public String execute(Table table) {
        // This is called by Database.execute() - the actual work is done there
        // Return a placeholder that will be replaced by the actual result
        return "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnType.getSimpleName();
    }
    
    @Override
    public String toString() {
        return "AlterTableAddColumnQuery{" +
               "tableName='" + tableName + '\'' +
               ", columnName='" + columnName + '\'' +
               ", columnType=" + columnType.getSimpleName() +
               '}';
    }
    
    /**
     * Validates column type for step 1 (nullable metadata-only ADD COLUMN).
     * Same types as CREATE TABLE for consistency.
     */
    private static void validateColumnType(Class<?> type) {
        if (type == null) {
            throw new IllegalArgumentException("Column type cannot be null");
        }
        
        // Supported types (same as QueryParser.parseColumnDefinition)
        if (type != String.class &&
            type != Integer.class &&
            type != Long.class &&
            type != Double.class &&
            type != Float.class && 
            type != Boolean.class &&
            type != BigDecimal.class &&
            type != LocalDate.class &&
            type != LocalDateTime.class &&
            type != UUID.class &&
            type != byte[].class) {
            throw new IllegalArgumentException("Unsupported column type: " + type.getName() + 
                ". Supported: String, Integer, Long, Double, Float, Boolean, BigDecimal, " +
                "LocalDate, LocalDateTime, UUID, byte[]");
        }
    }
}