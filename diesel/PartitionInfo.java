package diesel;

import java.util.List;
import java.util.Map;

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
    public final List<Map<String, Object>> partitionDefinitions;
    
    public PartitionInfo(PartitionType type, String column, String tableName, List<Map<String, Object>> partitionDefinitions) {
        this.type = type;
        this.column = column;
        this.tableName = tableName;
        this.partitionDefinitions = partitionDefinitions;
    }
    
    public PartitionInfo(PartitionType type, String column, String tableName) {
        this(type, column, tableName, null);
    }
}