package diesel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsed representation of a constant SELECT without a FROM clause, such as
 * {@code SELECT 1 = 1} or {@code SELECT TRUE}. The expression is constant-folded
 * at parse time (the engine materialises derived structures during parsing, so
 * this follows the same pattern) and {@link #execute} simply returns the
 * precomputed value as a single-row result set, matching the shape produced by
 * {@code SELECT COUNT(*)} and keeping the wire protocol on the STATUS_ROWS path.
 *
 * <p>Prompt 4 #48 (R3-013b): {@code SELECT 1 = 1} must return Boolean TRUE as
 * the value type, proving boolean literals work as return values, not only as
 * WHERE-clause operands.
 *
 * @see Query
 * @see QueryParser#parseSelectQuery
 */
class ConstantSelectQuery implements Query<List<Map<String, Object>>> {
    private final String columnName;
    private final Object value;

    /**
     * Creates a constant SELECT query.
     *
     * @param columnName the result column name (the original expression text)
     * @param value      the precomputed constant value (may be null for NULL)
     */
    ConstantSelectQuery(String columnName, Object value) {
        this.columnName = columnName;
        this.value = value;
    }

    /**
     * Returns the precomputed constant as a single-row, single-column result
     * set, so callers that expect a {@code List} of row maps (like
     * {@code runSelectCount}) work unchanged.
     *
     * @param table unused; a constant SELECT has no table
     * @return a one-element list containing one row map
     */
    @Override
    @SuppressWarnings("unused")
    public List<Map<String, Object>> execute(Table table) {
        throw new UnsupportedOperationException("ConstantSelectQuery is executed without a table");
    }

    /**
     * Returns the precomputed constant as a single-row result set.
     *
     * @param database unused
     * @param txid     unused
     * @return a one-element list containing one row with the constant value
     */
    @Override
    @SuppressWarnings("unused")
    public List<Map<String, Object>> execute(Database database, long txid) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(columnName, value);
        return List.of(row);
    }

    /**
     * Returns the precomputed constant value.
     *
     * @return the folded value (may be null)
     */
    Object getValue() {
        return value;
    }

    /**
     * Returns the result column name.
     *
     * @return the original expression text used as column name
     */
    String getColumnName() {
        return columnName;
    }
}
