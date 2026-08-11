package diesel;

/**
 * Executes a CREATE UNIQUE INDEX statement, building a unique secondary index.
 *
 * @see CreateIndexQueryBase
 */
class CreateUniqueIndexQuery extends CreateIndexQueryBase {
    /**
     * Creates a unique index query.
     *
     * @param tableName  the table to index
     * @param columnName the column to index
     */
    public CreateUniqueIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    /**
     * Builds the unique index on the table, failing on duplicate keys.
     *
     * @param table the table to index
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        table.createUniqueIndex(getColumnName());
        return null;
    }
}
