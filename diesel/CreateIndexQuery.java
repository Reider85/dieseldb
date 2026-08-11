package diesel;

/**
 * Executes a plain CREATE INDEX statement, building a B-tree secondary index.
 *
 * @see CreateIndexQueryBase
 */
class CreateIndexQuery extends CreateIndexQueryBase {
    /**
     * Creates a B-tree index query.
     *
     * @param tableName  the table to index
     * @param columnName the column to index
     */
    public CreateIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    /**
     * Builds the B-tree index on the table.
     *
     * @param table the table to index
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        table.createBTreeIndex(getColumnName());
        return null;
    }
}
