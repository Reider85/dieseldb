package diesel;

import java.util.List;
import java.util.Map;

class CreateTableQuery implements Query<Void> {
    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String primaryKeyColumn;
    private final Map<String, Sequence> sequences;

    public CreateTableQuery(String tableName, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn, Map<String, Sequence> sequences) {
        this.tableName = tableName;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.primaryKeyColumn = primaryKeyColumn;
        this.sequences = sequences;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Map<String, Class<?>> getColumnTypes() {
        return columnTypes;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    @Override
    public Void execute(Table table) {
        Database database = table.getDatabase(); // Assuming Table has a method to get the Database
        database.createTable(tableName, columns, columnTypes, primaryKeyColumn);
        for (Map.Entry<String, Sequence> entry : sequences.entrySet()) {
            table.getSequences().put(entry.getKey(), entry.getValue());
        }
        return null;
    }
}