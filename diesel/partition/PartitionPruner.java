package diesel.partition;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements partition pruning logic to determine which partitions can be skipped
 * based on WHERE clause conditions. Supports RANGE, LIST, and HASH partitioning
 * strategies with various predicate types.
 */
public class PartitionPruner {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPruner.class);
    
    /**
     * Analyzes filter conditions and returns partitions that should be scanned.
     * 
     * @param partitionManager the partition manager for the table
     * @param filterValues the filter values from WHERE clause
     * @return PruningResult containing partitions to scan and skip counts
     */
public static PruningResult prunePartitions(PartitionManager partitionManager, 
                                               Map<String, Object> filterValues) {
        if (partitionManager == null) {
            throw new IllegalArgumentException("PartitionManager cannot be null");
        }
        
        LOGGER.info("=== PartitionPruner.prunePartitions called for table {} with filters {} ===", 
                partitionManager.getParentTableName(), filterValues);
        
        if (filterValues == null || filterValues.isEmpty()) {
            // No filters - scan all partitions
            List<PartitionManager.Partition> allPartitions = getAllPartitions(partitionManager);
            LOGGER.info("=== No filters - scanning all {} partitions ===", allPartitions.size());
            return new PruningResult(allPartitions, 0, allPartitions.size(), "No filters - scanning all partitions");
        }
        
        String partitionColumn = partitionManager.getPartitionColumn();
        if (!filterValues.containsKey(partitionColumn) && !hasPartitionColumnFilter(filterValues, partitionColumn)) {
            // Partition column not in filter - scan all partitions
            List<PartitionManager.Partition> allPartitions = getAllPartitions(partitionManager);
            LOGGER.info("=== Partition column not in filter - scanning all {} partitions ===", allPartitions.size());
            return new PruningResult(allPartitions, 0, allPartitions.size(), "Partition column not in filter - scanning all partitions");
        }
        
        switch (partitionManager.getPartitionType()) {
            case RANGE:
                LOGGER.info("=== Using RANGE pruning ===");
                return pruneRangePartitions(partitionManager, filterValues);
            case LIST:
                LOGGER.info("=== Using LIST pruning ===");
                return pruneListPartitions(partitionManager, filterValues);
            case HASH:
                LOGGER.info("=== Using HASH pruning ===");
                return pruneHashPartitions(partitionManager, filterValues);
            default:
                throw new IllegalArgumentException("Unsupported partition type: " + partitionManager.getPartitionType());
        }
    }
    
    /**
     * Prunes range partitions based on filter conditions.
     */
    private static PruningResult pruneRangePartitions(PartitionManager partitionManager, 
                                                    Map<String, Object> filterValues) {
        String partitionColumn = partitionManager.getPartitionColumn();
        Object filterValue = null;
        
        // Get the filter value with the appropriate suffix
        if (filterValues.containsKey(partitionColumn + "_lt")) {
            filterValue = filterValues.get(partitionColumn + "_lt");
        } else if (filterValues.containsKey(partitionColumn + "_lte")) {
            filterValue = filterValues.get(partitionColumn + "_lte");
        } else if (filterValues.containsKey(partitionColumn + "_gt")) {
            filterValue = filterValues.get(partitionColumn + "_gt");
        } else if (filterValues.containsKey(partitionColumn + "_gte")) {
            filterValue = filterValues.get(partitionColumn + "_gte");
        } else if (filterValues.containsKey(partitionColumn + "_eq")) {
            filterValue = filterValues.get(partitionColumn + "_eq");
        }
        
        List<PartitionManager.Partition> allPartitions = getAllPartitions(partitionManager);
        List<PartitionManager.Partition> matchingPartitions = new ArrayList<>();
        int skippedCount = 0;
        
        LOGGER.info("Pruning range partitions for table {} with filter {} = {}", 
                partitionManager.getParentTableName(), partitionColumn, filterValue);
        
        // Check each partition to see if it can be skipped
        for (PartitionManager.Partition partition : allPartitions) {
            if (partition instanceof RangePartition) {
                RangePartition rangePartition = (RangePartition) partition;
                
                LOGGER.info("Checking range partition {} with min={}, max={} for filter {} = {}", 
                        rangePartition.getName(), rangePartition.getMinValue(), rangePartition.getMaxValue(), 
                        partitionColumn, filterValue);
                
                if (canSkipRangePartition(rangePartition, filterValues, partitionColumn)) {
                    skippedCount++;
                    LOGGER.info("SKIPPING range partition {} for filter {} = {}", 
                            rangePartition.getName(), partitionColumn, filterValue);
                } else {
                    LOGGER.info("INCLUDING range partition {} for filter {} = {}", 
                            rangePartition.getName(), partitionColumn, filterValue);
                    matchingPartitions.add(partition);
                }
            } else {
                // Not a range partition - include it
                LOGGER.info("Including non-range partition {}", partition.getName());
                matchingPartitions.add(partition);
            }
        }
        
        String reason = String.format("Range pruning: %d partitions skipped, %d to scan", 
                skippedCount, matchingPartitions.size());
        return new PruningResult(matchingPartitions, skippedCount, allPartitions.size(), reason);
    }
    
    /**
     * Determines if a range partition can be skipped based on filter conditions.
     */
    private static boolean canSkipRangePartition(RangePartition partition, 
                                               Map<String, Object> filterValues, 
                                               String partitionColumn) {
        // Check various filter patterns
        Object filterValue = filterValues.get(partitionColumn);
        
        // Handle equality filters (for single point queries)
        if (filterValues.containsKey(partitionColumn + "_eq")) {
            Object eqValue = filterValues.get(partitionColumn + "_eq");
            return !partition.containsValue(eqValue);
        }
        
        // Handle less than filters
        if (filterValues.containsKey(partitionColumn + "_lt")) {
            Object ltValue = filterValues.get(partitionColumn + "_lt");
            if (ltValue instanceof Comparable) {
                Comparable<?> ltComparable = (Comparable<?>) ltValue;
                // If partition's min >= filter value, skip this partition
                try {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> minComparable = (Comparable<Object>) partition.getMinValue();
                    boolean shouldSkip = minComparable != null && minComparable.compareTo(ltValue) >= 0;
                    LOGGER.info("LT check for partition {}: min={}, ltValue={}, shouldSkip={}", 
                            partition.getName(), minComparable, ltValue, shouldSkip);
                    if (shouldSkip) {
                        return true;
                    }
                } catch (ClassCastException e) {
                    LOGGER.warn("Cannot compare {} with range bounds", ltValue);
                }
                return false;
            }
        }
        
        // Handle less than or equal filters
        if (filterValues.containsKey(partitionColumn + "_lte")) {
            Object lteValue = filterValues.get(partitionColumn + "_lte");
            if (lteValue instanceof Comparable) {
                // Skip when the partition's lower bound is greater than the filter value
                try {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> minComparable = (Comparable<Object>) partition.getMinValue();
                    if (minComparable != null) {
                        return minComparable.compareTo(lteValue) > 0;
                    }
                } catch (ClassCastException e) {
                    LOGGER.warn("Cannot compare {} with range bounds", lteValue);
                }
                return false;
            }
        }
        
        // Handle greater than filters
        if (filterValues.containsKey(partitionColumn + "_gt")) {
            Object gtValue = filterValues.get(partitionColumn + "_gt");
            if (gtValue instanceof Comparable) {
                // Skip when the partition's upper bound is at or below the filter value
                try {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> maxComparable = (Comparable<Object>) partition.getMaxValue();
                    if (maxComparable != null) {
                        return maxComparable.compareTo(gtValue) <= 0;
                    }
                } catch (ClassCastException e) {
                    LOGGER.warn("Cannot compare {} with range bounds", gtValue);
                }
                return false;
            }
        }
        
        // Handle greater than or equal filters
        if (filterValues.containsKey(partitionColumn + "_gte")) {
            Object gteValue = filterValues.get(partitionColumn + "_gte");
            if (gteValue instanceof Comparable) {
                // Skip when the partition's upper bound is below the filter value
                try {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> maxComparable = (Comparable<Object>) partition.getMaxValue();
                    if (maxComparable != null) {
                        return maxComparable.compareTo(gteValue) < 0;
                    }
                } catch (ClassCastException e) {
                    LOGGER.warn("Cannot compare {} with range bounds", gteValue);
                }
                return false;
            }
        }
        
        // Handle BETWEEN filters
        if (filterValues.containsKey(partitionColumn + "_between_start") && 
            filterValues.containsKey(partitionColumn + "_between_end")) {
            Object startValue = filterValues.get(partitionColumn + "_between_start");
            Object endValue = filterValues.get(partitionColumn + "_between_end");
            
            if (startValue instanceof Comparable && endValue instanceof Comparable) {
                @SuppressWarnings("unchecked")
                Comparable<Object> startComparable = (Comparable<Object>) startValue;
                @SuppressWarnings("unchecked")
                Comparable<Object> endComparable = (Comparable<Object>) endValue;
                
                // Skip when the partition lies entirely before or after the BETWEEN range
                try {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> maxComparable = (Comparable<Object>) partition.getMaxValue();
                    @SuppressWarnings("unchecked")
                    Comparable<Object> minComparable = (Comparable<Object>) partition.getMinValue();
                    
                    boolean beforeRange = maxComparable != null && maxComparable.compareTo(startComparable) <= 0;
                    boolean afterRange = minComparable != null && minComparable.compareTo(endComparable) >= 0;
                    return beforeRange || afterRange;
                } catch (ClassCastException e) {
                    LOGGER.warn("Cannot compare range with BETWEEN bounds");
                    return false;
                }
            }
        }
        
        // Default: don't skip the partition
        return false;
    }
    
    /**
     * Prunes list partitions based on filter conditions.
     */
    private static PruningResult pruneListPartitions(PartitionManager partitionManager, 
                                                  Map<String, Object> filterValues) {
        String partitionColumn = partitionManager.getPartitionColumn();
        Object filterValue = filterValues.get(partitionColumn);
        PartitionManager.Partition defaultPartition = partitionManager.getDefaultPartition();
        List<PartitionManager.Partition> allPartitions = getAllPartitions(partitionManager);
        List<PartitionManager.Partition> matchingPartitions = new ArrayList<>();
        int skippedCount = 0;
        
        LOGGER.debug("Pruning list partitions for table {} with filter {} = {}", 
                partitionManager.getParentTableName(), partitionColumn, filterValue);
        
        // Check each non-default partition to see if it can be skipped
        for (PartitionManager.Partition partition : allPartitions) {
            if (partition == defaultPartition) {
                continue;
            }
            if (partition instanceof ListPartition) {
                ListPartition listPartition = (ListPartition) partition;
                
                if (canSkipListPartition(listPartition, filterValues, partitionColumn)) {
                    skippedCount++;
                    LOGGER.debug("Skipping list partition {} for filter {} = {}", 
                            listPartition.getName(), partitionColumn, filterValue);
                } else {
                    matchingPartitions.add(partition);
                }
            } else {
                // Not a list partition - include it
                matchingPartitions.add(partition);
            }
        }
        
        // The default partition is only needed when no defined partition can serve the filter
        if (defaultPartition != null) {
            if (matchingPartitions.isEmpty()) {
                matchingPartitions.add(defaultPartition);
            } else {
                skippedCount++;
            }
        }
        
        String reason = String.format("List pruning: %d partitions skipped, %d to scan", 
                skippedCount, matchingPartitions.size());
        return new PruningResult(matchingPartitions, skippedCount, allPartitions.size(), reason);
    }
    
    /**
     * Determines if a list partition can be skipped based on filter conditions.
     */
    private static boolean canSkipListPartition(ListPartition partition, 
                                             Map<String, Object> filterValues, 
                                             String partitionColumn) {
        // Handle IN filters (check if any value matches)
        if (filterValues.containsKey(partitionColumn + "_in")) {
            Object inValue = filterValues.get(partitionColumn + "_in");
            if (inValue instanceof Collection) {
                Collection<?> inValues = (Collection<?>) inValue;
                for (Object value : inValues) {
                    if (partition.containsValue(value)) {
                        return false; // Can't skip - partition contains a matching value
                    }
                }
                return true; // Can skip - no values match this partition
            }
        }
        
        // Handle equality filters, including IS NULL
        if (filterValues.containsKey(partitionColumn)) {
            return !partition.containsValue(filterValues.get(partitionColumn));
        }
        
        // Default: don't skip the partition
        return false;
    }
    
    /**
     * Hash partitions cannot be pruned since values can be in any partition.
     */
    private static PruningResult pruneHashPartitions(PartitionManager partitionManager, 
                                                   Map<String, Object> filterValues) {
        // For hash partitions, we must scan all partitions
        List<PartitionManager.Partition> allPartitions = getAllPartitions(partitionManager);
        
        String reason = "Hash partitioning - cannot prune, scanning all partitions";
        return new PruningResult(allPartitions, 0, allPartitions.size(), reason);
    }
    
    /**
     * Returns every partition exactly once. The default partition is usually already
     * present in the partition list, so it is only appended when missing.
     */
    private static List<PartitionManager.Partition> getAllPartitions(PartitionManager partitionManager) {
        List<PartitionManager.Partition> allPartitions = new ArrayList<>(partitionManager.getPartitions());
        PartitionManager.Partition defaultPartition = partitionManager.getDefaultPartition();
        if (defaultPartition != null && !allPartitions.contains(defaultPartition)) {
            allPartitions.add(defaultPartition);
        }
        return allPartitions;
    }
    
    /**
     * Result of partition pruning operation.
     */
    public static class PruningResult {
        private final List<PartitionManager.Partition> partitionsToScan;
        private final int partitionsSkipped;
        private final int totalPartitions;
        private final String reason;
        
        public PruningResult(List<PartitionManager.Partition> partitionsToScan, 
                           int partitionsSkipped, int totalPartitions, String reason) {
            this.partitionsToScan = Collections.unmodifiableList(new ArrayList<>(partitionsToScan));
            this.partitionsSkipped = partitionsSkipped;
            this.totalPartitions = totalPartitions;
            this.reason = reason;
        }
        
        public List<PartitionManager.Partition> getPartitionsToScan() {
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
            if (totalPartitions == 0) return 0.0;
            return (double) partitionsSkipped / totalPartitions;
        }
        
        @Override
        public String toString() {
            return String.format("PruningResult[scan=%d, skipped=%d, total=%d, ratio=%.2f, reason='%s']", 
                    partitionsToScan.size(), partitionsSkipped, totalPartitions, 
                    getPruningRatio(), reason);
        }
    }
    
    /**
     * Helper method to check if filter values contain any partition column filters with suffixes
     */
    private static boolean hasPartitionColumnFilter(Map<String, Object> filterValues, String partitionColumn) {
        String[] suffixes = {"_eq", "_lt", "_lte", "_gt", "_gte", "_in", "_between_start", "_between_end"};
        for (String suffix : suffixes) {
            if (filterValues.containsKey(partitionColumn + suffix)) {
                return true;
            }
        }
        return false;
    }
}