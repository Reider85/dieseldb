package diesel.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Compact {@code Object[]} row representation used internally by the delimited
 * storage back-ends (prompt 36). A row is an array whose {@code i}-th slot
 * holds the value of schema column {@code i}, so the per-row overhead collapses
 * to one object header plus one reference per column &mdash; several times
 * smaller than a per-row {@code HashMap<String,Object>}. Column-to-value maps
 * are built only at the public Map-based API boundary
 * ({@code scan}/{@code insert}/{@code update}).
 *
 * <p>Immutable after construction, so one instance can be shared between a
 * storage and its index manager: both then observe the very same row arrays.
 */
final class RowArrays {

    private final List<String> columnNames;
    private final Map<String, Integer> indexByName;

    RowArrays(List<String> columns) {
        this.columnNames = List.copyOf(columns);
        this.indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            indexByName.put(columns.get(i), i);
        }
    }

    /** Returns the ordered canonical column names. */
    List<String> columnNames() {
        return columnNames;
    }

    /** Returns the number of schema columns. */
    int size() {
        return columnNames.size();
    }

    /**
     * Returns the index of the given (case-insensitive) column name, or
     * {@code -1} when the name is not part of the schema.
     */
    int indexOf(String column) {
        if (column == null) {
            return -1;
        }
        Integer idx = indexByName.get(column);
        return idx != null ? idx : -1;
    }

    /**
     * Returns the value of the named column from an array row, or {@code null}
     * when the column is unknown or the value is absent.
     */
    Object get(Object[] row, String column) {
        int idx = indexOf(column);
        return (idx < 0 || row == null || idx >= row.length) ? null : row[idx];
    }

    /**
     * Converts a column-to-value map into a compact array row. Unknown keys are
     * dropped and missing columns stay {@code null}, which keeps the stored row
     * detached from the caller's map.
     */
    Object[] fromMap(Map<String, Object> row) {
        Object[] values = new Object[columnNames.size()];
        if (row == null) {
            return values;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            int idx = indexOf(entry.getKey());
            if (idx >= 0) {
                values[idx] = entry.getValue();
            }
        }
        return values;
    }

    /** Builds a column-to-value map from an array row (canonical column names). */
    Map<String, Object> toMap(Object[] row) {
        Map<String, Object> map = new HashMap<>(Math.max(columnNames.size() * 2, 4));
        for (int i = 0; i < columnNames.size() && row != null && i < row.length; i++) {
            map.put(columnNames.get(i), row[i]);
        }
        return map;
    }
}