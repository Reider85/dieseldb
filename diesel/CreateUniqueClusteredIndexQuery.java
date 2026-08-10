package diesel;

class CreateUniqueClusteredIndexQuery extends CreateIndexQueryBase {
    public CreateUniqueClusteredIndexQuery(String tableName, String columnName) {
        super(tableName, columnName);
    }

    @Override
    public Void execute(Table table) {
        table.createUniqueClusteredIndex(getColumnName());
        return null;
    }
}
