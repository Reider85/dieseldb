package diesel;

/**
 * Common contract of every parsed SQL statement: an executable object that
 * performs its operation against a {@link Table}.
 *
 * @param <T> the result type (a row list for SELECT, null for DML/DDL, or a
 *            String for transaction statements)
 */
interface Query<T> {
    /**
     * Executes the query against the given table.
     *
     * @param table the table the query operates on
     * @return the query result
     */
    T execute(Table table);
    
    /**
     * Executes the query with database context and transaction ID.
     * Default implementation delegates to the single-parameter method.
     *
     * @param database the database context
     * @param txid the transaction ID, or -1 for no transaction
     * @return the query result
     */
    default T execute(Database database, long txid) {
        // Extract the main table from the database for SHOW queries
        // For regular queries, this will be overridden by specific implementations
        Table table = extractTableForQuery(database, txid);
        return execute(table);
    }
    
    /**
     * Helper method to extract the appropriate table for query execution.
     * Can be overridden by queries that need specific table extraction logic.
     */
    default Table extractTableForQuery(Database database, long txid) {
        // For SHOW queries, we need to get the main table differently
        // This is a placeholder - SHOW queries should override this method
        throw new UnsupportedOperationException("This query type requires table extraction logic");
    }
}
