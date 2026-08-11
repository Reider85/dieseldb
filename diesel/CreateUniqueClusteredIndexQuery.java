package diesel;

/**
 * Executes a CREATE UNIQUE CLUSTERED INDEX statement, building a unique
 * clustered B-tree index that physically reorders the rows.
 *
 * @see CreateIndexQueryBase
 */
class CreateUniqueClusteredIndexQuery extends CreateIndexQueryBase {
    /**
     * Creates a unique clustered index query.
     *
     * @param tableName  the table to index
     * @param columnName the column to index
     */
    public CreateUniqueClusteredIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    /**
     * Builds the unique clustered index on the table, sorting the rows and
     * failing on null or duplicate keys.
     *
     * @param table the table to index
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        table.createUniqueClusteredIndex(getColumnName());
        return null;
    }
}
