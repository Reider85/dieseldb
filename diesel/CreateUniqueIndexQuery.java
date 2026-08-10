package diesel;

class CreateUniqueIndexQuery extends CreateIndexQueryBase {
    public CreateUniqueIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    @Override
    public Void execute(Table table) {
        table.createUniqueIndex(getColumnName());
        return null;
    }
}
