package diesel;

class CreateHashIndexQuery extends CreateIndexQueryBase {
    public CreateHashIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    @Override
    public Void execute(Table table) {
        table.createHashIndex(getColumnName());
        return null;
    }
}
