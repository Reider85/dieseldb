package diesel.partition;

import java.util.*;
import java.util.stream.Collectors;
import diesel.Table;

/**
 * Implements list-based partitioning where partitions contain specific discrete values.
 * Each partition is associated with a list of values that belong to it.
 */
public class ListPartition implements PartitionManager.Partition {
    private final String name;
    private final Table table;
    private final List<Object> partitionValues;
    private final Set<Object> valueSet;
    
    /**
     * Creates a new list partition.
     * 
     * @param name the partition name
     * @param table the underlying table for this partition
     * @param partitionValues the list of values that belong to this partition, or null for default partitions
     */
    public ListPartition(String name, Table table, List<Object> partitionValues) {
        this.name = name;
        this.table = table;
        if (partitionValues == null) {
            // For default partitions, accept any value
            this.partitionValues = null;
            this.valueSet = null;
        } else {
            this.partitionValues = new ArrayList<>(partitionValues);
            this.valueSet = new HashSet<>(partitionValues);
            
            if (this.partitionValues.isEmpty()) {
                throw new IllegalArgumentException("Partition values cannot be empty for list partition");
            }
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
     * Returns the list of values that belong to this partition.
     * 
     * @return unmodifiable list of partition values, or null for default partitions
     */
    public List<Object> getPartitionValues() {
        if (partitionValues == null) {
            return null;
        }
        return Collections.unmodifiableList(partitionValues);
    }
    
    @Override
    public boolean containsValue(Object value) {
        // For default partitions, accept any value
        if (partitionValues == null) {
            return true;
        }
        
        if (value == null) {
            // Check if null is in the partition values
            return valueSet.contains(null);
        }
        
        return valueSet.contains(value);
    }
    
    @Override
    public Object getPartitionKey() {
        // For list partitions, return the list of values as a string
        if (partitionValues == null) {
            return "DEFAULT";
        }
        return partitionValues.toString();
    }
    
    @Override
    public String toString() {
        return "ListPartition{" +
                "name='" + name + '\'' +
                ", table=" + table.getName() +
                ", values=" + partitionValues +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListPartition that = (ListPartition) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(table, that.table) &&
               Objects.equals(partitionValues, that.partitionValues);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, table, partitionValues);
    }
}