package diesel;

class CreateIndexQuery extends CreateIndexQueryBase {
    public CreateIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    @Override
    public Void execute(Table table) {
        table.createBTreeIndex(getColumnName());
        return null;
    }
}
