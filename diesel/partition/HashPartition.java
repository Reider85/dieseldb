package diesel.partition;

import java.util.*;
import diesel.Table;

/**
 * Implements hash-based partitioning where values are distributed across partitions
 * based on their hash code. This partitioning strategy doesn't support pruning
 * since values can be in any partition based on their hash.
 */
public class HashPartition implements PartitionManager.Partition {
    private final String name;
    private final Table table;
    private final int partitionNumber;
    private final int totalPartitions;
    
    /**
     * Creates a new hash partition.
     * 
     * @param name the partition name
     * @param table the underlying table for this partition
     * @param partitionNumber the partition number (0-based)
     * @param totalPartitions the total number of partitions
     */
    public HashPartition(String name, Table table, int partitionNumber, int totalPartitions) {
        this.name = name;
        this.table = table;
        this.partitionNumber = partitionNumber;
        this.totalPartitions = totalPartitions;
        
        if (partitionNumber < 0 || partitionNumber >= totalPartitions) {
            throw new IllegalArgumentException("Partition number " + partitionNumber + 
                    " is invalid for " + totalPartitions + " partitions");
        }
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public Table getTable() {
        return table;
    }
    
    /**
     * Returns the partition number.
     * 
     * @return the partition number (0-based)
     */
    public int getPartitionNumber() {
        return partitionNumber;
    }
    
    /**
     * Returns the total number of partitions.
     * 
     * @return the total number of partitions
     */
    public int getTotalPartitions() {
        return totalPartitions;
    }
    
    /**
     * Determines if a value belongs to this partition based on hash code.
     * 
     * @param value the value to check
     * @return true if the value belongs to this partition
     */
    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            // Null values always go to partition 0
            return partitionNumber == 0;
        }
        
        // Use hash code modulo total partitions to determine partition
        int hashCode = Math.abs(value.hashCode());
        int targetPartition = hashCode % totalPartitions;
        
        return targetPartition == partitionNumber;
    }
    
    @Override
    public Object getPartitionKey() {
        // For hash partitions, return the partition number
        return partitionNumber;
    }
    
    /**
     * Calculates which partition a value would belong to.
     * 
     * @param value the value to check
     * @param totalPartitions the total number of partitions
     * @return the partition number (0-based)
     */
    public static int getPartitionForValue(Object value, int totalPartitions) {
        if (value == null) {
            return 0;
        }
        
        int hashCode = Math.abs(value.hashCode());
        return hashCode % totalPartitions;
    }
    
    @Override
    public String toString() {
        return "HashPartition{" +
                "name='" + name + '\'' +
                ", table=" + table.getName() +
                ", partitionNumber=" + partitionNumber +
                ", totalPartitions=" + totalPartitions +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashPartition that = (HashPartition) o;
        return partitionNumber == that.partitionNumber &&
               totalPartitions == that.totalPartitions &&
               Objects.equals(name, that.name) &&
               Objects.equals(table, that.table);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, table, partitionNumber, totalPartitions);
    }
}