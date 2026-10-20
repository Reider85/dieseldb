package diesel;

/**
 * Executes a DROP TABLE statement: drops the table through the database.
 *
 * @see Query
 */
class DropTableQuery implements Query<Void> {
    private final String tableName;

    /**
     * Creates a DROP TABLE query with the given table name.
     *
     * @param tableName the table name to drop
     */
    public DropTableQuery(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public Void execute(Table table) {
        table.getDatabase().dropTable(tableName);
        return null;
    }
}