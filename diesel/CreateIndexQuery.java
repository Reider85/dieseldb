package diesel;

class CreateIndexQuery implements Query<Void> {
    private final String tableName;
    private final String columnName;

    public CreateIndexQuery(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public Void execute(Table table) {
        table.createBTreeIndex(columnName);
        return null;
    }
}
