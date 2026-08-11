package diesel;

/**
 * Common metadata shared by every CREATE INDEX query variant (B-tree, hash,
 * unique and unique clustered). The concrete subclasses only differ in the
 * index type they build on the table.
 */
abstract class CreateIndexQueryBase implements Query<Void> {
    private final String tableName;
    private final String columnName;

    /**
     * Creates the base with the shared metadata.
     *
     * @param tableName  the table the index is built on
     * @param columnName the column the index is built on
     */
    protected CreateIndexQueryBase(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /**
     * Returns the table the index is built on.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the column the index is built on.
     *
     * @return the column name
     */
    public String getColumnName() {
        return columnName;
    }
}
