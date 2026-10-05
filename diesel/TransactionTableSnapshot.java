package diesel;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

/**
 * MVCC snapshot view of a table for transaction isolation.
 * 
 * <p>Provides a filtered view of table rows based on transaction visibility rules:
 * - Own writes are visible
 * - Uncommitted writes from other transactions are not visible
 * - Committed writes are visible if they occurred before the transaction snapshot
 * 
 * <p>This is a lazy view - rows are materialized on demand based on the current
 * table state and transaction metadata.
 */
public class TransactionTableSnapshot {
    
    private final String tableName;
    private final Table table;
    private final long currentTxid;
    private final long snapshotTxid;
    private final long snapshotCsn;
    private final IsolationLevel isolationLevel;
    private final TxStatusTracker txStatusTracker;
    
    /**
     * Creates a new transaction table snapshot.
     * 
     * @param tableName the name of the table
     * @param table the underlying table
     * @param currentTxid the current transaction id
     * @param snapshotTxid the snapshot txid counter value at BEGIN time
     * @param snapshotCsn the snapshot commit CSN counter value at BEGIN time
     * @param isolationLevel the isolation level
     * @param txStatusTracker transaction status tracker for visibility checks
     */
    public TransactionTableSnapshot(String tableName, Table table, long currentTxid,
                                   long snapshotTxid, long snapshotCsn,
                                   IsolationLevel isolationLevel,
                                   TxStatusTracker txStatusTracker) {
        this.tableName = tableName;
        this.table = table;
        this.currentTxid = currentTxid;
        this.snapshotTxid = snapshotTxid;
        this.snapshotCsn = snapshotCsn;
        this.isolationLevel = isolationLevel;
        this.txStatusTracker = txStatusTracker;
    }
    
    /**
     * Returns all visible rows in this snapshot.
     * 
     * @return list of visible row maps
     */
    public List<Map<String, Object>> getRows() {
        table.getTableLock().readLock().lock();
        try {
            List<Map<String, Object>> result = new ArrayList<>();
            int rowCount = table.getRawRowCount();
            
            for (int i = 0; i < rowCount; i++) {
                if (!table.isDeleted(i)) {
                    RowVersionMeta meta = table.getRowVersionMeta(i);
                    if (isRowVisible(i, meta)) {
                        result.add(getVisibleRowValues(i, meta));
                    }
                }
            }
            return result;
        } finally {
            table.getTableLock().readLock().unlock();
        }
    }
    
    /**
     * Returns the number of visible rows in this snapshot.
     * 
     * @return count of visible rows
     */
    public int getRowCount() {
        // For performance, we could cache this if needed
        return getRows().size();
    }
    
    /**
     * Returns the visible row at the given index (0-based).
     * 
     * @param index the row index
     * @return the visible row map
     * @throws IndexOutOfBoundsException if index is out of bounds
     */
    public Map<String, Object> getRow(int index) {
        List<Map<String, Object>> rows = getRows();
        if (index < 0 || index >= rows.size()) {
            throw new IndexOutOfBoundsException("Row index " + index + " out of bounds for snapshot with " + rows.size() + " rows");
        }
        return rows.get(index);
    }
    
    /**
     * Returns the table name for this snapshot.
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Returns the underlying table (use with caution - for testing only).
     */
    Table getTable() {
        return table;
    }
    
    /**
     * Checks if the row with the given metadata is visible in this snapshot.
     */
    private boolean isRowVisible(int rowIndex, RowVersionMeta meta) {
        // Bootstrap row (xmin=0) - check visibility based on xmax
        if (meta == null || meta.getXmin() == 0) {
            if (meta == null || meta.getXmax() == 0) {
                return true;
            }
            if (meta.getXmax() == currentTxid) {
                // Own uncommitted delete hides the row from its deleter
                // (mirrors TupleVisibility rule 2).
                return false;
            }
            return !isTxCommittedBefore(meta.getXmax());
        }
        
        // Use TupleVisibility to determine visibility
        // Create a Row object for visibility checking
        Map<String, Object> rowValues = table.getRows().get(rowIndex);
        Row row = new Row(rowValues, meta.getXmin(), meta.getXmax(), meta.getCommandId());
        
        // Create txCommitted predicate for this transaction
        LongPredicate txCommitted = txid -> {
            if (txid == currentTxid) {
                return false; // Own writes are handled separately by TupleVisibility
            }
            return txStatusTracker.isCommittedBefore(txid, snapshotCsn);
        };
        
        return TupleVisibility.visible(row, currentTxid, snapshotTxid, isolationLevel, txCommitted);
    }
    
    /**
     * Checks if the given transaction id is committed before our snapshot.
     */
    private boolean isTxCommittedBefore(long txid) {
        return txStatusTracker.isCommittedBefore(txid, snapshotCsn);
    }
    
    /**
     * Returns the visible values for a row with MVCC metadata.
     * Uses committedValues if available, otherwise current row values.
     */
    private Map<String, Object> getVisibleRowValues(int rowIndex, RowVersionMeta meta) {
        Map<String, Object> sourceValues = table.getRows().get(rowIndex);
        
        if (meta != null && meta.getCommittedValues() != null) {
            // Return a copy of committed values
            return new HashMap<>(meta.getCommittedValues());
        } else {
            // Return a copy of current values
            return new HashMap<>(sourceValues);
        }
    }
    
    /**
     * Returns a string representation of this snapshot for debugging.
     */
    @Override
    public String toString() {
        return "TransactionTableSnapshot{" +
               "tableName='" + tableName + '\'' +
               ", rowCount=" + getRowCount() +
               ", currentTxid=" + currentTxid +
               ", snapshotTxid=" + snapshotTxid +
               ", snapshotCsn=" + snapshotCsn +
               ", isolationLevel=" + isolationLevel +
               '}';
    }
}