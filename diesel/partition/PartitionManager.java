package diesel.partition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diesel.Table;
import diesel.Database;

/**
 * Manages partitioned tables and routes operations to the appropriate partition.
 * Supports RANGE, LIST, and HASH partitioning strategies.
 */
public class PartitionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);
    
    private final Database database;
    private final String parentTableName;
    private final PartitionType partitionType;
    private final String partitionColumn;
    private List<Partition> partitions;
    private Partition defaultPartition;
    
    /**
     * Types of partitioning supported.
     */
    public enum PartitionType {
        RANGE,
        LIST,
        HASH
    }
    
    /**
     * Base interface for all partition types.
     */
    public interface Partition {
        String getName();
        Table getTable();
        boolean containsValue(Object value);
        Object getPartitionKey();
    }
    
    /**
     * Creates a new PartitionManager for a partitioned table.
     * 
     * @param database the database instance
     * @param parentTableName the name of the parent partitioned table
     * @param partitionType the type of partitioning
     * @param partitionColumn the column used for partitioning
     * @param partitions the list of partitions
     * @param defaultPartition the default partition for non-matching values
     */
    public PartitionManager(Database database, String parentTableName, 
                           PartitionType partitionType, String partitionColumn,
                           List<Partition> partitions, Partition defaultPartition) {
        this.database = database;
        this.parentTableName = parentTableName;
        this.partitionType = partitionType;
        this.partitionColumn = partitionColumn;
        this.partitions = new ArrayList<>(partitions);
        this.defaultPartition = defaultPartition;
        
        LOGGER.info("Created PartitionManager for table {} with {} partitions of type {} on column {}",
                parentTableName, partitions.size(), partitionType, partitionColumn);
    }
    
    /**
     * Finds the appropriate partition for the given row values.
     * 
     * @param rowValues the row values to partition
     * @return the appropriate partition, or default partition if no match
     */
    public Partition findPartition(Map<String, Object> rowValues) {
        Object partitionValue = rowValues.get(partitionColumn);
        if (partitionValue == null) {
            LOGGER.warn("Partition column {} is null for row in table {}, using default partition",
                    partitionColumn, parentTableName);
            return defaultPartition;
        }
        
        // Find the first partition that contains this value
        for (Partition partition : partitions) {
            if (partition.containsValue(partitionValue)) {
                return partition;
            }
        }
        
        // If no partition matches, use the default partition
        if (defaultPartition != null) {
            LOGGER.debug("No partition found for value {} in table {}, using default partition",
                    partitionValue, parentTableName);
            return defaultPartition;
        }
        
        throw new IllegalArgumentException("No partition found for value " + partitionValue + 
                " in table " + parentTableName + " and no default partition configured");
    }
    
    /**
     * Routes an insert operation to the appropriate partition.
     * 
     * @param rowValues the row values to insert
     * @return the partition where the row was inserted
     */
    public Partition routeInsert(Map<String, Object> rowValues) {
        Partition targetPartition = findPartition(rowValues);
        LOGGER.debug("Routing insert for table {} to partition {} based on column {} = {}",
                parentTableName, targetPartition.getName(), partitionColumn, 
                rowValues.get(partitionColumn));
        return targetPartition;
    }
    
    /**
     * Routes a select operation to the appropriate partition(s) using partition pruning.
     * This method uses the PartitionPruner to determine which partitions can be skipped.
     * 
     * @param filterValues the filter values from WHERE clause
     * @return PruningResult containing partitions to scan and pruning information
     */
    public PartitionPruningResult routeSelectWithPruning(Map<String, Object> filterValues) {
        LOGGER.info("=== routeSelectWithPruning called for table {} with filters {} ===", 
                parentTableName, filterValues);
        
        // Use PartitionPruner to determine which partitions to scan
        PartitionPruner.PruningResult pruningResult = PartitionPruner.prunePartitions(this, filterValues);
        
        LOGGER.info("=== Pruning result: {} partitions to scan, {} skipped, ratio {} ===", 
                pruningResult.getPartitionsToScan().size(), 
                pruningResult.getPartitionsSkipped(),
                pruningResult.getPruningRatio());
        
        // Create enhanced result with additional information
        return new PartitionPruningResult(
                pruningResult.getPartitionsToScan(),
                pruningResult.getPartitionsSkipped(),
                pruningResult.getTotalPartitions(),
                pruningResult.getReason(),
                pruningResult.getPruningRatio()
        );
    }
    
    /**
     * Routes a select operation to the appropriate partition(s) using partition pruning.
     * This method uses the PartitionPruner to determine which partitions can be skipped.
     * 
     * @param filterValues the filter values from WHERE clause
     * @return list of partitions that might contain matching rows
     * @deprecated Use routeSelectWithPruning() for better pruning information
     */
    @Deprecated
    public List<Partition> routeSelect(Map<String, Object> filterValues) {
        // Use the new pruning method but return only the partition list for backward compatibility
        PartitionPruningResult pruningResult = routeSelectWithPruning(filterValues);
        return pruningResult.getPartitionsToScan();
    }
    
    /**
     * Returns all partitions managed by this manager.
     * 
     * @return unmodifiable list of all partitions
     */
    public List<Partition> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }
    
    /**
     * Returns the default partition, if any.
     * 
     * @return the default partition, or null
     */
    public Partition getDefaultPartition() {
        return defaultPartition;
    }
    
    /**
     * Returns the partition type.
     * 
     * @return the partition type
     */
    public PartitionType getPartitionType() {
        return partitionType;
    }
    
    /**
     * Returns the partition column name.
     * 
     * @return the partition column name
     */
    public String getPartitionColumn() {
        return partitionColumn;
    }
    
    /**
     * Returns the parent table name.
     * 
     * @return the parent table name
     */
    public String getParentTableName() {
        return parentTableName;
    }
    
    /**
     * Adds a new partition to this manager.
     * 
     * @param partition the partition to add
     */
    public void addPartition(Partition partition) {
        partitions.add(partition);
        LOGGER.info("Added partition {} to table {}", partition.getName(), parentTableName);
    }
    
    /**
     * Removes a partition from this manager.
     * 
     * @param partitionName the name of the partition to remove
     * @return the removed partition, or null if not found
     */
    public Partition removePartition(String partitionName) {
        Iterator<Partition> it = partitions.iterator();
        while (it.hasNext()) {
            Partition partition = it.next();
            if (partition.getName().equals(partitionName)) {
                it.remove();
                LOGGER.info("Removed partition {} from table {}", partitionName, parentTableName);
                return partition;
            }
        }
        return null;
    }
    
    /**
     * Sets the default partition for this manager.
     * 
     * @param defaultPartition the default partition
     */
    public void setDefaultPartition(Partition defaultPartition) {
        this.defaultPartition = defaultPartition;
        LOGGER.info("Set default partition {} for table {}", defaultPartition.getName(), parentTableName);
    }
    
    /**
     * Sets the partitions list for this manager.
     * 
     * @param partitions the partitions list
     */
    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
        LOGGER.info("Set partitions for table {}: {}", parentTableName, partitions.size());
    }
    
    /**
     * Enhanced result from partition pruning operation with additional metadata.
     */
    public static class PartitionPruningResult {
        private final List<Partition> partitionsToScan;
        private final int partitionsSkipped;
        private final int totalPartitions;
        private final String reason;
        private final double pruningRatio;
        
        public PartitionPruningResult(List<Partition> partitionsToScan, 
                                   int partitionsSkipped, int totalPartitions, 
                                   String reason, double pruningRatio) {
            this.partitionsToScan = Collections.unmodifiableList(new ArrayList<>(partitionsToScan));
            this.partitionsSkipped = partitionsSkipped;
            this.totalPartitions = totalPartitions;
            this.reason = reason;
            this.pruningRatio = pruningRatio;
        }
        
        public List<Partition> getPartitionsToScan() {
            return partitionsToScan;
        }
        
        public int getPartitionsSkipped() {
            return partitionsSkipped;
        }
        
        public int getTotalPartitions() {
            return totalPartitions;
        }
        
        public String getReason() {
            return reason;
        }
        
        public double getPruningRatio() {
            return pruningRatio;
        }
        
        public String getPruningSummary() {
            return String.format("Scan: %d, Skip: %d, Total: %d, Ratio: %.2f%%, Reason: %s", 
                    partitionsToScan.size(), partitionsSkipped, totalPartitions, 
                    pruningRatio * 100, reason);
        }
        
        @Override
        public String toString() {
            return getPruningSummary();
        }
    }
}