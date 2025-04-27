package diesel;


class CreateHashIndexQuery implements Query<Void> {
    private final String tableName;
    private final String columnName;

    public CreateHashIndexQuery(String tableName, String columnName) {
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
        table.createHashIndex(columnName);
        return null;
    }
}