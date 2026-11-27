package diesel;

/**
 * Information about table partitioning.
 */
public class PartitionInfo {
    public enum PartitionType {
        RANGE,
        LIST,
        HASH
    }
    
    public final PartitionType type;
    public final String column;
    public final String tableName;
    
    public PartitionInfo(PartitionType type, String column, String tableName) {
        this.type = type;
        this.column = column;
        this.tableName = tableName;
    }
}