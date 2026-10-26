package diesel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Facade for accessing package-private diesel APIs for backup/restore operations.
 * This class provides controlled access to internal functionality without exposing
 * implementation details to external code.
 */
public class DumpAccess {

    /**
     * Returns the names of all user tables in the database.
     * Internal system tables (prefixed with "DIESEL_") are excluded.
     *
     * @param database the database instance
     * @return list of table names
     */
    public static List<String> tableNames(Database database) {
        List<String> tables = new ArrayList<>();
        for (Table table : database.getTables()) {
            if (!table.getName().startsWith("DIESEL_")) {
                tables.add(table.getName());
            }
        }
        return tables;
    }

    /**
     * Opens a cursor for reading rows from a table with MVCC support.
     * The cursor batches rows and filters them according to the transaction's visibility rules.
     *
     * @param database the database instance
     * @param tableName the table name to read from
     * @param txid the transaction id for MVCC consistency (null for autocommitted reads)
     * @return a row cursor for batched row iteration
     * @throws IllegalArgumentException if the table doesn't exist
     */
    public static RowCursor openRows(Database database, String tableName, Long txid) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return new RowCursor(database, table, txid);
    }

    /**
     * Inserts a row into a table without sequence-based primary key restrictions.
     * This allows explicit sequence PK values for backup/restore operations.
     *
     * @param database the database instance
     * @param tableName the table name to insert into
     * @param row the column-to-value map
     * @throws IllegalArgumentException if the table doesn't exist or values are invalid
     * @throws IllegalStateException if a unique/clustered key is duplicated
     */
    public static void restoreRow(Database database, String tableName, Map<String, Object> row) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        table.restoreRow(row);
    }

    /**
     * Seeds all sequences in a table to at least the maximum value found in the given rows.
     * This ensures that sequence values continue correctly after restore.
     *
     * @param database the database instance
     * @param tableName the table name
     * @param rows list of rows to scan for sequence values
     */
    public static void seedSequences(Database database, String tableName, List<Map<String, Object>> rows) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Find the maximum value for each sequence column
        Map<String, Long> maxValues = new ConcurrentHashMap<>();
        for (Map<String, Object> row : rows) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String column = entry.getKey();
                Sequence sequence = table.getSequences().get(column);
                if (sequence != null) {
                    Object value = entry.getValue();
                    if (value instanceof Number) {
                        long numValue = ((Number) value).longValue();
                        maxValues.merge(column, numValue, Math::max);
                    }
                }
            }
        }

        // Seed each sequence to at least its max found value
        for (Map.Entry<String, Long> entry : maxValues.entrySet()) {
            String column = entry.getKey();
            Sequence sequence = table.getSequences().get(column);
            if (sequence != null) {
                sequence.seedAtLeast(entry.getValue());
            }
        }
    }

    /**
     * Checks if a row with the given primary key value already exists in the table.
     * Used for conflict detection during restore operations.
     *
     * @param database the database instance
     * @param tableName the table name
     * @param pkValue the primary key value to check
     * @return true if a row with this PK exists and is visible, false otherwise
     */
    public static boolean rowExistsByPk(Database database, String tableName, Object pkValue) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        if (table.getPrimaryKeyColumn() == null || pkValue == null) {
            return false;
        }

        // Use the clustered index to search for the PK
        Index index = table.getIndexes().get(table.getPrimaryKeyColumn());
        if (index != null) {
            List<Integer> hits = index.search(pkValue);
            for (int hit : hits) {
                if (!table.isDeleted(hit)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Batched row cursor for reading table rows with MVCC support.
     * This class provides batched iteration over table rows while respecting
     * transaction visibility rules and filtering out dropped/hidden columns.
     */
    public static class RowCursor {
        private final Database database;
        private final Table table;
        private final long txid;
        private final int batchSize;
        private int currentRow;
        private List<Map<String, Object>> currentBatch;
        private boolean closed;

        /**
         * Creates a new row cursor.
         *
         * @param database the database instance
         * @param table the table to read from
         * @param txid the transaction id for MVCC consistency (null for autocommitted reads)
         */
        public RowCursor(Database database, Table table, Long txid) {
            this.database = database;
            this.table = table;
            this.txid = txid != null ? txid : -1; // -1 indicates autocommitted read
            this.batchSize = 1000; // Configurable batch size
            this.currentRow = 0;
            this.closed = false;
        }

        /**
         * Returns the next batch of rows from the table.
         * Each row is filtered to exclude dropped/hidden columns.
         *
         * @return list of rows (each row is a map of column name to value)
         */
        public List<Map<String, Object>> nextBatch() {
            if (closed) {
                throw new IllegalStateException("Cursor is closed");
            }

            List<Map<String, Object>> batch = new ArrayList<>(batchSize);
            int endRow = Math.min(currentRow + batchSize, table.getLiveRows().size());

            for (int i = currentRow; i < endRow; i++) {
                Map<String, Object> row = table.getLiveRows().get(i);
                if (table.isRowVisibleToReader(i)) {
                    // Filter out dropped/hidden columns
                    Map<String, Object> filteredRow = new HashMap<>();
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        if (!table.isDroppedColumn(entry.getKey())) {
                            filteredRow.put(entry.getKey(), entry.getValue());
                        }
                    }
                    batch.add(filteredRow);
                }
            }

            currentRow = endRow;
            return batch;
        }

        /**
         * Returns true if there are more rows to read.
         *
         * @return true if more rows are available
         */
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            return currentRow < table.getLiveRows().size();
        }

        /**
         * Closes the cursor and releases any resources.
         */
        public void close() {
            closed = true;
        }
    }
}