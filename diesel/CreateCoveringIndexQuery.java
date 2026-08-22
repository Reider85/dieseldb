package diesel;

import java.util.List;

/**
 * Executes a CREATE INDEX statement with a COVERING clause, building a
 * covering B-tree index that stores extra column values for index-only scans.
 *
 * @see CoveringBTreeIndex
 */
class CreateCoveringIndexQuery implements Query<Void> {
    private final String tableName;
    private final String indexColumn;
    private final List<String> coverColumns;

    /**
     * Creates a covering index query.
     *
     * @param tableName    the table to index
     * @param indexColumn  the column to index
     * @param coverColumns additional columns to store in the index
     */
    public CreateCoveringIndexQuery(String tableName, String indexColumn, List<String> coverColumns) {
        this.tableName = tableName;
        this.indexColumn = indexColumn;
        this.coverColumns = coverColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public List<String> getCoverColumns() {
        return coverColumns;
    }

    @Override
    public Void execute(Table table) {
        table.createCoveringBTreeIndex(indexColumn, coverColumns);
        return null;
    }
}
