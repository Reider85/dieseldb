package diesel;
import java.util.*;

class CreateTableQuery implements Query<Void> {
    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;

    public CreateTableQuery(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.tableName = tableName;
        this.columns = columns;
        this.columnTypes = columnTypes;
    }

    @Override
    public Void execute(Table table) {
        throw new UnsupportedOperationException("CreateTableQuery should be handled by Database directly");
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
}