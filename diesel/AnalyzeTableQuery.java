package diesel;

/**
 * Executes an {@code ANALYZE TABLE} statement: forces a synchronous
 * recalculation of the table statistics (row count, average row size and the
 * last-analyzed timestamp, see {@link Table#analyze()}) and returns a status
 * message describing the fresh statistics.
 *
 * @see Query
 * @see Table#analyze()
 */
class AnalyzeTableQuery implements Query<String> {
    private final String tableName;

    /**
     * Creates an ANALYZE TABLE query for the given table.
     *
     * @param tableName the table whose statistics are recalculated
     */
    public AnalyzeTableQuery(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Returns the table whose statistics are recalculated.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Recalculates the table statistics and returns the status message.
     *
     * @param table the table the query operates on
     * @return the ANALYZE TABLE status message
     */
    @Override
    public String execute(Table table) {
        Table.TableStatistics stats = table.analyze();
        return "Table " + tableName + " analyzed: " + stats.getRowCount() + " rows, avg row size "
                + stats.getAvgRowSizeBytes() + " bytes, last analyzed " + Table.formatTimestamp(stats.getLastAnalyzedMillis());
    }
}
