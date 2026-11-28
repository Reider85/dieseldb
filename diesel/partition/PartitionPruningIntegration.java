package diesel.partition;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diesel.Database;
import diesel.Table;

/**
 * Integrates partition pruning functionality with the query optimizer.
 * Provides methods to apply partition pruning to query execution plans.
 */
public class PartitionPruningIntegration {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPruningIntegration.class);
    
    /**
     * Applies partition pruning to a query execution plan.
     * 
     * @param database the database instance
     * @param tableName the table being queried
     * @param filterValues the filter conditions from WHERE clause
     * @return PartitionPruningResult containing optimized partition information
     */
    public static PartitionManager.PartitionPruningResult applyPartitionPruning(
            Database database, String tableName, Map<String, Object> filterValues) {
        
        LOGGER.debug("Applying partition pruning to table {} with filters {}", tableName, filterValues);
        
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table " + tableName + " not found");
        }
        
        PartitionManager partitionManager = table.getPartitionManager();
        if (partitionManager == null) {
            LOGGER.debug("Table {} is not partitioned, skipping pruning", tableName);
            // Return a result indicating no pruning was applied
            return createNoPruningResult(table);
        }
        
        // Apply partition pruning
        PartitionManager.PartitionPruningResult pruningResult = 
            partitionManager.routeSelectWithPruning(filterValues);
        
        LOGGER.info("Partition pruning result for table {}: {}", tableName, pruningResult.getPruningSummary());
        
        return pruningResult;
    }
    
    /**
     * Creates a result indicating no partitioning was applied.
     */
    private static PartitionManager.PartitionPruningResult createNoPruningResult(Table table) {
        List<PartitionManager.Partition> partitions = Collections.emptyList();
        
        // Create a mock partition manager result for non-partitioned tables
        return new PartitionManager.PartitionPruningResult(
                partitions, 0, 0, "Table is not partitioned", 0.0
        );
    }
    
    /**
     * Extracts filter values from a WHERE clause condition.
     * This is a simplified implementation - in production, you'd want more sophisticated parsing.
     * 
     * @param whereClause the WHERE clause string (without WHERE keyword)
     * @return map of filter column names to values
     */
    public static Map<String, Object> extractFilterValues(String whereClause) {
        Map<String, Object> filterValues = new HashMap<>();
        
        if (whereClause == null || whereClause.trim().isEmpty()) {
            return filterValues;
        }
        
        // Convert to lowercase for case-insensitive matching
        String lowerClause = whereClause.toLowerCase();
        
        // Extract simple equality conditions: column = value
        extractEqualityFilters(lowerClause, filterValues);
        
        // Extract less than conditions: column < value
        extractLessThanFilters(lowerClause, filterValues);
        
        // Extract less than or equal conditions: column <= value
        extractLessThanOrEqualFilters(lowerClause, filterValues);
        
        // Extract greater than conditions: column > value
        extractGreaterThanFilters(lowerClause, filterValues);
        
        // Extract greater than or equal conditions: column >= value
        extractGreaterThanOrEqualFilters(lowerClause, filterValues);
        
        // Extract BETWEEN conditions: column BETWEEN value1 AND value2
        extractBetweenFilters(lowerClause, filterValues);
        
        // Extract IN conditions: column IN (value1, value2, ...)
        extractInFilters(lowerClause, filterValues);
        
        return filterValues;
    }
    
    /**
     * Extracts filter values from a list of QueryParser conditions.
     * 
     * @param conditions the list of conditions from the WHERE clause
     * @return map of filter column names to values
     */
    public static Map<String, Object> extractFilterValuesFromConditions(List<?> conditions) {
        Map<String, Object> filterValues = new HashMap<>();
        
        if (conditions == null || conditions.isEmpty()) {
            return filterValues;
        }
        
        // Note: This is a simplified implementation since QueryParser.Condition is not public
        // In production, you'd want to use reflection or access internal APIs properly
        return filterValues;
    }
    
    private static void extractEqualityFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column = value
        // This is a simplified regex - in production, you'd want proper SQL parsing
        String pattern = "(\\w+)\\s*=\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valueStr = matcher.group(2);
            
            // Try to parse the value based on expected type
            Object value = parseValue(valueStr);
            filterValues.put(column, value);
            
            // Also add special key for partition pruner
            filterValues.put(column + "_eq", value);
        }
    }
    
    private static void extractLessThanFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column < value
        String pattern = "(\\w+)\\s*<\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valueStr = matcher.group(2);
            
            Object value = parseValue(valueStr);
            filterValues.put(column + "_lt", value);
        }
    }
    
    private static void extractLessThanOrEqualFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column <= value
        String pattern = "(\\w+)\\s*<=\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valueStr = matcher.group(2);
            
            Object value = parseValue(valueStr);
            filterValues.put(column + "_lte", value);
        }
    }
    
    private static void extractGreaterThanFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column > value
        String pattern = "(\\w+)\\s*>\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valueStr = matcher.group(2);
            
            Object value = parseValue(valueStr);
            filterValues.put(column + "_gt", value);
        }
    }
    
    private static void extractGreaterThanOrEqualFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column >= value
        String pattern = "(\\w+)\\s*>=\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valueStr = matcher.group(2);
            
            Object value = parseValue(valueStr);
            filterValues.put(column + "_gte", value);
        }
    }
    
    private static void extractBetweenFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column BETWEEN 'value1' AND 'value2'
        String pattern = "(\\w+)\\s+between\\s*'([^']+)'\\s+and\\s*'([^']+)'";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String value1Str = matcher.group(2);
            String value2Str = matcher.group(3);
            
            Object value1 = parseValue(value1Str);
            Object value2 = parseValue(value2Str);
            
            filterValues.put(column + "_between_start", value1);
            filterValues.put(column + "_between_end", value2);
        }
    }
    
    private static void extractInFilters(String clause, Map<String, Object> filterValues) {
        // Pattern: column IN ('value1', 'value2', ...)
        String pattern = "(\\w+)\\s+in\\s*\\(\\s*'([^']+)'(?:\\s*,\\s*'([^']+)'\\s*)*\\)";
        java.util.regex.Pattern regex = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = regex.matcher(clause);
        
        while (matcher.find()) {
            String column = matcher.group(1);
            String valuesPart = matcher.group(2); // This will only get the first value
            
            // For simplicity, just extract the first value for IN clause
            // In production, you'd want to parse all values in the IN list
            Object value = parseValue(valuesPart);
            filterValues.put(column + "_in", Collections.singletonList(value));
        }
    }
    
    private static Object parseValue(String valueStr) {
        if (valueStr == null) {
            return null;
        }
        
        // Try to parse as date
        try {
            return java.time.LocalDate.parse(valueStr);
        } catch (Exception e) {
            // Not a date, continue
        }
        
        // Try to parse as number
        try {
            return Double.parseDouble(valueStr);
        } catch (Exception e) {
            // Not a number, continue
        }
        
        // Return as string
        return valueStr;
    }
    
    /**
     * Logs partition pruning information for debugging and monitoring.
     * 
     * @ tableName the table name
     * @ pruningResult the pruning result to log
     */
    public static void logPartitionPruningInfo(String tableName, 
                                            PartitionManager.PartitionPruningResult pruningResult) {
        LOGGER.info("Partition pruning for table '{}':", tableName);
        LOGGER.info("  Partitions to scan: {}", pruningResult.getPartitionsToScan().size());
        LOGGER.info("  Partitions skipped: {}", pruningResult.getPartitionsSkipped());
        LOGGER.info("  Total partitions: {}", pruningResult.getTotalPartitions());
        LOGGER.info("  Pruning ratio: {:.2f}%", pruningResult.getPruningRatio() * 100);
        LOGGER.info("  Reason: {}", pruningResult.getReason());
        
        if (pruningResult.getPartitionsToScan().size() <= 5) {
            LOGGER.info("  Partitions to scan:");
            for (PartitionManager.Partition partition : pruningResult.getPartitionsToScan()) {
                LOGGER.info("    - {}", partition.getName());
            }
        }
    }
}