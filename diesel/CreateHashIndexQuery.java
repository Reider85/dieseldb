package diesel;

/**
 * Executes a CREATE HASH INDEX statement, building a hash secondary index.
 *
 * @see CreateIndexQueryBase
 */
class CreateHashIndexQuery extends CreateIndexQueryBase {
    /**
     * Creates a hash index query.
     *
     * @param tableName  the table to index
     * @param columnName the column to index
     */
    public CreateHashIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    /**
     * Builds the hash index on the table.
     *
     * @param table the table to index
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        table.createHashIndex(getColumnName());
        return null;
    }
}
