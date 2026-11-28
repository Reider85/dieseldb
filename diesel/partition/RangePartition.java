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
            // A null bound means unbounded (first partition / MAXVALUE partition).
            if (minValue != null && ((Comparable<Object>) minValue).compareTo(value) > 0) {
                return false;
            }
            if (maxValue != null && ((Comparable<Object>) value).compareTo(maxValue) >= 0) {
                return false;
            }
            return true;
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
    
    /**
     * Checks if this partition can be skipped based on a range filter.
     * 
     * @param filterValue the filter value
     * @param operator the comparison operator ("lt", "lte", "gt", "gte")
     * @return true if this partition can be skipped
     */
    public boolean canSkipByFilter(Object filterValue, String operator) {
        if (filterValue == null || !(filterValue instanceof Comparable)) {
            return false;
        }
        
        @SuppressWarnings("unchecked")
        Comparable<Object> filterComparable = (Comparable<Object>) filterValue;
        
        try {
            switch (operator) {
                case "lt": // less than
                    return filterComparable.compareTo(minValue) >= 0;
                case "lte": // less than or equal
                    return filterComparable.compareTo(minValue) > 0;
                case "gt": // greater than
                    return filterComparable.compareTo(maxValue) <= 0;
                case "gte": // greater than or equal
                    return filterComparable.compareTo(maxValue) < 0;
                default:
                    return false;
            }
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Cannot compare filter value " + filterValue + 
                    " with range bounds", e);
        }
    }
    
    /**
     * Checks if this partition can be skipped based on a BETWEEN filter.
     * 
     * @param startValue the start of the BETWEEN range
     * @param endValue the end of the BETWEEN range
     * @return true if this partition can be skipped
     */
    public boolean canSkipByBetween(Object startValue, Object endValue) {
        if (startValue == null || endValue == null || !(startValue instanceof Comparable) || !(endValue instanceof Comparable)) {
            return false;
        }
        
        @SuppressWarnings("unchecked")
        Comparable<Object> startComparable = (Comparable<Object>) startValue;
        @SuppressWarnings("unchecked")
        Comparable<Object> endComparable = (Comparable<Object>) endValue;
        
        try {
            // If partition range is completely outside BETWEEN range, skip it
            int cmp1 = startComparable.compareTo(maxValue);
            int cmp2 = endComparable.compareTo(minValue);
            
            // partition.max < start OR partition.min > end
            return cmp1 < 0 || cmp2 > 0;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Cannot compare range with BETWEEN bounds", e);
        }
    }
    
    /**
     * Checks if this partition overlaps with the given range.
     * 
     * @param startValue the start of the range
     * @param endValue the end of the range
     * @return true if there is any overlap
     */
    public boolean overlapsWith(Object startValue, Object endValue) {
        if (startValue == null || endValue == null || !(startValue instanceof Comparable) || !(endValue instanceof Comparable)) {
            return true; // Cannot determine, assume overlap
        }
        
        @SuppressWarnings("unchecked")
        Comparable<Object> startComparable = (Comparable<Object>) startValue;
        @SuppressWarnings("unchecked")
        Comparable<Object> endComparable = (Comparable<Object>) endValue;
        
        try {
            @SuppressWarnings("unchecked")
            Comparable<Object> maxComparable = (Comparable<Object>) maxValue;
            @SuppressWarnings("unchecked")
            Comparable<Object> minComparable = (Comparable<Object>) minValue;
            
            // Check if ranges overlap: !(partition.max <= start || partition.min >= end)
            int cmp1 = maxComparable.compareTo(startComparable);
            int cmp2 = minComparable.compareTo(endComparable);
            
            return !(cmp1 <= 0 || cmp2 >= 0);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Cannot compare ranges", e);
        }
    }
}