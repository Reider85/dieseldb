package diesel.partition;

import java.util.*;
import diesel.Table;
import diesel.Database;

/**
 * Implements range-based partitioning where partitions are defined by value ranges.
 * Each partition contains values within a specific range [min, max).
 */
public class RangePartition implements PartitionManager.Partition {
    private final String name;
    private final Table table;
    private final Object minValue;
    private final Object maxValue;
    private final Comparable<Object> comparator;
    
    /**
     * Creates a new range partition.
     * 
     * @param name the partition name
     * @param table the underlying table for this partition
     * @param minValue the minimum value (inclusive) for this partition
     * @param maxValue the maximum value (exclusive) for this partition
     * @param comparator comparator for comparing partition values
     */
    public RangePartition(String name, Table table, Object minValue, Object maxValue, 
                         Comparable<Object> comparator) {
        this.name = name;
        this.table = table;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.comparator = comparator;
        
        if (comparator == null) {
            throw new IllegalArgumentException("Comparator cannot be null for range partition");
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
     * Returns the minimum value (inclusive) for this partition.
     * 
     * @return the minimum value
     */
    public Object getMinValue() {
        return minValue;
    }
    
    /**
     * Returns the maximum value (exclusive) for this partition.
     * 
     * @return the maximum value
     */
    public Object getMaxValue() {
        return maxValue;
    }
    
    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            return false;
        }
        
        try {
            int cmpMin = ((Comparable<Object>) minValue).compareTo(value);
            int cmpMax = ((Comparable<Object>) value).compareTo(maxValue);
            
            // Value is >= minValue and < maxValue
            return cmpMin <= 0 && cmpMax < 0;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Cannot compare value " + value + 
                    " with range bounds using comparator", e);
        }
    }
    
    @Override
    public Object getPartitionKey() {
        // For range partitions, return the range as a string representation
        return "[" + minValue + " - " + maxValue + ")";
    }
    
    @Override
    public String toString() {
        return "RangePartition{" +
                "name='" + name + '\'' +
                ", table=" + table.getName() +
                ", range=[" + minValue + " - " + maxValue + ")" +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangePartition that = (RangePartition) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(table, that.table) &&
               Objects.equals(minValue, that.minValue) &&
               Objects.equals(maxValue, that.maxValue);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, table, minValue, maxValue);
    }
}