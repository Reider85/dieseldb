package diesel;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Executes database queries in parallel when they are independent (access different tables).
 * 
 * <p>This executor improves performance by running queries that access disjoint sets of tables
 * concurrently, reducing overall query latency for workloads with multiple independent operations.
 */
public class QueryExecutor {
    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());
    private final ExecutorService executorService;
    private final Database database;
    
    /**
     * Creates a query executor with the specified thread pool size.
     * 
     * @param poolSize the number of threads in the pool
     * @param database the database instance to execute queries against
     */
    public QueryExecutor(int poolSize, Database database) {
        this.database = database;
        this.executorService = new ThreadPoolExecutor(
                poolSize, poolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.AbortPolicy());
    }
    
    /**
     * Executes a list of queries in parallel, grouping independent queries for concurrent execution.
     * 
     * <p>Queries are analyzed to determine which tables they access. Queries that access
     * disjoint sets of tables are executed in parallel. Queries that share tables are
     * executed sequentially to maintain consistency.
     * 
     * @param queries list of SQL queries to execute
     * @param transactionId the transaction ID, or null for auto-commit mode
     * @return list of query results in the same order as the input queries
     * @throws Exception if any query fails to execute
     */
    public List<Object> executeQueries(List<String> queries, UUID transactionId) throws Exception {
        if (queries == null || queries.isEmpty()) {
            return new ArrayList<>();
        }
        
        // If only one query, execute it directly
        if (queries.size() == 1) {
            return List.of(database.executeQuery(queries.get(0), transactionId));
        }
        
        // Analyze which tables each query accesses
        List<Set<String>> queryTables = new ArrayList<>();
        for (String query : queries) {
            queryTables.add(extractTablesFromQuery(query));
        }
        
        // Group queries into independent sets that can be executed in parallel
        List<List<Integer>> independentGroups = groupIndependentQueries(queryTables);
        
        // Execute each group in parallel, queries within a group sequentially
        List<Object> results = new ArrayList<>(queries.size());
        List<Future<List<Object>>> futures = new ArrayList<>();
        
        // Submit groups for parallel execution
        for (List<Integer> group : independentGroups) {
            if (group.size() == 1) {
                // Single query in group - execute directly
                int queryIndex = group.get(0);
                results.add(queryIndex, database.executeQuery(queries.get(queryIndex), transactionId));
            } else {
                // Multiple queries in group - execute sequentially (they share tables)
                Callable<List<Object>> groupTask = () -> {
                    List<Object> groupResults = new ArrayList<>(group.size());
                    for (int i = 0; i < group.size(); i++) {
                        int queryIndex = group.get(i);
                        groupResults.add(i, database.executeQuery(queries.get(queryIndex), transactionId));
                    }
                    return groupResults;
                };
                
                Future<List<Object>> future = executorService.submit(groupTask);
                futures.add(future);
                
                // Store the future with its query indices for later result extraction
                // We'll handle this after submitting all tasks
            }
        }
        
        // Collect results from parallel groups
        int resultIndex = 0;
        for (List<Integer> group : independentGroups) {
            if (group.size() > 1) {
                // This group was submitted for parallel execution
                Future<List<Object>> future = futures.remove(0);
                List<Object> groupResults = future.get();
                for (int i = 0; i < group.size(); i++) {
                    int queryIndex = group.get(i);
                    results.add(queryIndex, groupResults.get(i));
                }
            }
            // Single query groups were handled above
        }
        
        return results;
    }
    
    /**
     * Shuts down the executor service, allowing currently executing tasks to complete.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Extracts the set of table names referenced in a SQL query.
     * 
     * <p>This is a simplified implementation that detects table names in
     * FROM, JOIN, INSERT INTO, UPDATE, and DELETE FROM clauses.
     * 
     * @param query the SQL query to analyze
     * @return set of table names referenced in the query
     */
    private Set<String> extractTablesFromQuery(String query) {
        Set<String> tables = new HashSet<>();
        if (query == null || query.isEmpty()) {
            return tables;
        }
        
        String upperQuery = query.toUpperCase().trim();
        
        // Extract tables from SELECT ... FROM ...
        int fromIndex = upperQuery.indexOf(" FROM ");
        if (fromIndex != -1) {
            int endIndex = findNextClauseStart(upperQuery, fromIndex + 6);
            String fromClause = upperQuery.substring(fromIndex + 6, 
                    endIndex != -1 ? endIndex : upperQuery.length());
            tables.addAll(extractTablesFromFromClause(fromClause));
        }
        
        // Extract tables from INSERT INTO ...
        int insertIndex = upperQuery.indexOf("INSERT INTO ");
        if (insertIndex != -1) {
            int endIndex = findNextClauseStart(upperQuery, insertIndex + 12);
            String intoClause = upperQuery.substring(insertIndex + 12, 
                    endIndex != -1 ? endIndex : upperQuery.length());
            tables.add(extractFirstTable(intoClause));
        }
        
        // Extract tables from UPDATE ...
        int updateIndex = upperQuery.indexOf("UPDATE ");
        if (updateIndex != -1) {
            int endIndex = findNextClauseStart(upperQuery, updateIndex + 7);
            String updateClause = upperQuery.substring(updateIndex + 7, 
                    endIndex != -1 ? endIndex : upperQuery.length());
            tables.add(extractFirstTable(updateClause));
        }
        
        // Extract tables from DELETE FROM ...
        int deleteFromIndex = upperQuery.indexOf("DELETE FROM ");
        if (deleteFromIndex != -1) {
            int endIndex = findNextClauseStart(upperQuery, deleteFromIndex + 12);
            String fromClause = upperQuery.substring(deleteFromIndex + 12, 
                    endIndex != -1 ? endIndex : upperQuery.length());
            tables.add(extractFirstTable(fromClause));
        }
        
        return tables;
    }
    
    /**
     * Finds the start of the next SQL clause after the given position.
     * 
     * @param query the uppercase query string
     * @param startIndex the position to start searching from
     * @return the index of the next clause keyword, or -1 if not found
     */
    private int findNextClauseStart(String query, int startIndex) {
        int whereIndex = query.indexOf(" WHERE ", startIndex);
        int groupIndex = query.indexOf(" GROUP BY ", startIndex);
        int havingIndex = query.indexOf(" HAVING ", startIndex);
        int orderIndex = query.indexOf(" ORDER BY ", startIndex);
        int limitIndex = query.indexOf(" LIMIT ", startIndex);
        int offsetIndex = query.indexOf(" OFFSET ", startIndex);
        
        int nextIndex = query.length();
        if (whereIndex != -1) nextIndex = Math.min(nextIndex, whereIndex);
        if (groupIndex != -1) nextIndex = Math.min(nextIndex, groupIndex);
        if (havingIndex != -1) nextIndex = Math.min(nextIndex, havingIndex);
        if (orderIndex != -1) nextIndex = Math.min(nextIndex, orderIndex);
        if (limitIndex != -1) nextIndex = Math.min(nextIndex, limitIndex);
        if (offsetIndex != -1) nextIndex = Math.min(nextIndex, offsetIndex);
        
        return (nextIndex == query.length()) ? -1 : nextIndex;
    }
    
    /**
     * Extracts table names from a FROM clause (which may contain JOINs).
     * 
     * @param fromClause the FROM clause (uppercase, no leading "FROM ")
     * @return set of table names in the FROM clause
     */
    private Set<String> extractTablesFromFromClause(String fromClause) {
        Set<String> tables = new HashSet<>();
        String[] parts = fromClause.split("\\s+(?i)JOIN\\s+");
        for (String part : parts) {
            String table = extractFirstTable(part.trim());
            if (!table.isEmpty()) {
                tables.add(table);
            }
        }
        return tables;
    }
    
    /**
     * Extracts the first table name from a clause (ignoring aliases).
     * 
     * @param clause a clause containing a table name (possibly with alias)
     * @return the table name, or empty string if not found
     */
    private String extractFirstTable(String clause) {
        if (clause == null || clause.isEmpty()) {
            return "";
        }
        
        String[] tokens = clause.trim().split("\\s+");
        if (tokens.length > 0) {
            // Remove any trailing commas or parentheses
            String table = tokens[0].replaceAll("[,()]", "");
            return table.isEmpty() ? "" : table;
        }
        return "";
    }
    
    /**
     * Groups queries into sets where queries within a set share tables
     * (and thus must be executed sequentially), while different sets
     * are independent and can be executed in parallel.
     * 
     * @param queryTables list of sets, where each set contains tables accessed by a query
     * @return list of groups, where each group contains indices of queries that can be executed together
     */
    private List<List<Integer>> groupIndependentQueries(List<Set<String>> queryTables) {
        List<List<Integer>> groups = new ArrayList<>();
        boolean[] assigned = new boolean[queryTables.size()];
        
        for (int i = 0; i < queryTables.size(); i++) {
            if (assigned[i]) {
                continue;
            }
            
            // Start a new group with query i
            List<Integer> group = new ArrayList<>();
            group.add(i);
            assigned[i] = true;
            
            // Find all queries that share tables with any query in the group
            boolean changed;
            do {
                changed = false;
                Set<String> groupTables = new HashSet<>();
                for (int queryIdx : group) {
                    groupTables.addAll(queryTables.get(queryIdx));
                }
                
                for (int j = 0; j < queryTables.size(); j++) {
                    if (!assigned[j]) {
                        // Check if query j shares any table with the group
                        Set<String> queryJTables = queryTables.get(j);
                        boolean sharesTable = false;
                        for (String table : queryJTables) {
                            if (groupTables.contains(table)) {
                                sharesTable = true;
                                break;
                            }
                        }
                        
                        if (sharesTable) {
                            group.add(j);
                            assigned[j] = true;
                            changed = true;
                        }
                    }
                }
            } while (changed);
            
            groups.add(group);
        }
        
        return groups;
    }
}