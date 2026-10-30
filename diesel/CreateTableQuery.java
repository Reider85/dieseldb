package diesel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Executes a CREATE TABLE statement: creates the table through the database
 * and registers its sequences.
 *
 * @see Query
 */
class CreateTableQuery implements Query<Void> {
    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String primaryKeyColumn;
    private final Map<String, Sequence> sequences;
    private final Map<String, String> columnCollations;

    /**
     * Creates a CREATE TABLE query with the given schema (no collations).
     *
     * @param tableName        the table name
     * @param columns          the ordered column names
     * @param columnTypes      the column name to type mapping
     * @param primaryKeyColumn the primary key column, or null
     * @param sequences        the sequences declared in the CREATE TABLE
     */
    public CreateTableQuery(String tableName, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn, Map<String, Sequence> sequences) {
        this(tableName, columns, columnTypes, primaryKeyColumn, sequences, Collections.emptyMap());
    }

    /**
     * Creates a CREATE TABLE query with per-column collations (prompt4 #49).
     *
     * @param tableName        the table name
     * @param columns          the ordered column names
     * @param columnTypes      the column name to type mapping
     * @param primaryKeyColumn the primary key column, or null
     * @param sequences        the sequences declared in the CREATE TABLE
     * @param columnCollations  the column name to collation mapping (only
     *                         CASE_INSENSITIVE is supported today)
     */
    public CreateTableQuery(String tableName, List<String> columns, Map<String, Class<?>> columnTypes,
                            String primaryKeyColumn, Map<String, Sequence> sequences,
                            Map<String, String> columnCollations) {
        this.tableName = tableName;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.primaryKeyColumn = primaryKeyColumn;
        this.sequences = sequences;
        this.columnCollations = columnCollations != null ? columnCollations : Collections.emptyMap();
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the column names.
     *
     * @return the column list
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * Returns the column name to type mapping.
     *
     * @return the column type map
     */
    public Map<String, Class<?>> getColumnTypes() {
        return columnTypes;
    }

    /**
     * Returns the primary key column, or null.
     *
     * @return the primary key column
     */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /**
     * Returns the sequences declared in the statement.
     *
     * @return the sequence map
     */
    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    /**
     * Returns the per-column collation mapping (prompt4 #49).
     *
     * @return column name to collation name, empty when none declared
     */
    public Map<String, String> getColumnCollations() {
        return columnCollations;
    }

    /**
     * Creates the table through the attached database and registers the
     * sequences on it.
     *
     * @param table the (temporary) table the query was dispatched with
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        Database database = table.getDatabase(); // Assuming Table has a method to get the Database
        database.createTable(tableName, columns, columnTypes, primaryKeyColumn, columnCollations);
        for (Map.Entry<String, Sequence> entry : sequences.entrySet()) {
            table.getSequences().put(entry.getKey(), entry.getValue());
        }
        return null;
    }
}