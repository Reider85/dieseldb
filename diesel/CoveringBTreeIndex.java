package diesel;

import java.io.Serializable;
import java.util.*;

/**
 * A B-tree secondary index that also stores selected column values for each
 * indexed row.  When a query's SELECT columns are all contained in the
 * covering set, the engine can return results directly from the index without
 * touching the table rows.
 *
 * @see BTreeIndex
 * @see Index
 */
class CoveringBTreeIndex extends BTreeIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String indexColumn;
    private final List<String> coverColumns;
    private final Map<Integer, Map<String, Object>> coverData;

    /**
     * Creates a covering B-tree index.
     *
     * @param keyType     the Java type of the indexed column
     * @param indexColumn the column name used as the index key
     * @param coverColumns additional columns whose values are stored in the index
     */
    CoveringBTreeIndex(Class<?> keyType, String indexColumn, List<String> coverColumns) {
        super(keyType);
        this.indexColumn = indexColumn;
        this.coverColumns = List.copyOf(coverColumns);
        this.coverData = new HashMap<>();
    }

    /**
     * Inserts a key and stores the covered column values from the row.
     *
     * @param key      the index key
     * @param rowIndex the row index
     * @param row      the full row data (used to extract cover columns)
     */
    public void insertWithRow(Object key, int rowIndex, Map<String, Object> row) {
        insert(key, rowIndex);
        if (row != null) {
            Map<String, Object> covered = new HashMap<>();
            covered.put(indexColumn, row.get(indexColumn));
            for (String col : coverColumns) {
                covered.put(col, row.get(col));
            }
            coverData.put(rowIndex, covered);
        }
    }

    @Override
    public void remove(Object key, int rowIndex) {
        super.remove(key, rowIndex);
        coverData.remove(rowIndex);
    }

    /**
     * Returns the covered column values for the given row index.
     *
     * @param rowIndex the row index
     * @return the stored values, or null if not found
     */
    public Map<String, Object> getCoveredValues(int rowIndex) {
        return coverData.get(rowIndex);
    }

    /**
     * Returns the set of column names this index covers (including the
     * indexed column itself).
     */
    public Set<String> getAllCoveredColumns() {
        Set<String> all = new HashSet<>(coverColumns);
        all.add(indexColumn);
        return all;
    }

    /**
     * Returns true when this index stores all of the given columns.
     */
    public boolean coversColumns(Set<String> requiredColumns) {
        return getAllCoveredColumns().containsAll(requiredColumns);
    }

    /**
     * Returns the extra cover column names (excluding the indexed column).
     */
    public List<String> getCoverColumns() {
        return coverColumns;
    }

    /**
     * Returns the indexed column name.
     */
    public String getIndexColumn() {
        return indexColumn;
    }

    /**
     * Bulk-loads the index with cover data.
     *
     * @param sortedKeys sorted keys
     * @param sortedRowIdx corresponding row indexes
     * @param rows the table rows (used to populate cover data)
     */
    public void bulkLoadWithCover(List<Object> sortedKeys, List<Integer> sortedRowIdx,
                                   List<Map<String, Object>> rows) {
        bulkLoad(sortedKeys, sortedRowIdx);
        for (int i = 0; i < sortedRowIdx.size(); i++) {
            int rowIdx = sortedRowIdx.get(i);
            if (rowIdx >= 0 && rowIdx < rows.size()) {
                Map<String, Object> row = rows.get(rowIdx);
                Map<String, Object> covered = new HashMap<>();
                covered.put(indexColumn, row.get(indexColumn));
                for (String col : coverColumns) {
                    covered.put(col, row.get(col));
                }
                coverData.put(rowIdx, covered);
            }
        }
    }
}
