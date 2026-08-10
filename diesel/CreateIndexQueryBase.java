package diesel;

/**
 * Common metadata shared by every CREATE INDEX query variant (B-tree, hash,
 * unique and unique clustered). The concrete subclasses only differ in the
 * index type they build on the table.
 */
abstract class CreateIndexQueryBase implements Query<Void> {
    private final String tableName;
    private final String columnName;

    protected CreateIndexQueryBase(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }
}
