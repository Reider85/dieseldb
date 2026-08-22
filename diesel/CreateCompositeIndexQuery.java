package diesel;

import java.util.List;

/**
 * Executes a CREATE INDEX statement with multiple columns, building a
 * composite B-tree index.
 *
 * @see CompositeBTreeIndex
 */
class CreateCompositeIndexQuery implements Query<Void> {
    private final String tableName;
    private final List<String> columnNames;

    /**
     * Creates a composite index query.
     *
     * @param tableName  the table to index
     * @param columnNames the columns to index (order matters)
     */
    public CreateCompositeIndexQuery(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public Void execute(Table table) {
        table.createCompositeBTreeIndex(columnNames);
        return null;
    }
}
