package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 MVCC-3 regression: {@link TransactionTableSnapshot#getRowCount()} must be
 * O(N) cheap count walk (memoized → O(1) after first), not O(N) full materialization
 * with N HashMap copies (the old path).
 * 
 * <p>Pre-fix: getRowCount() called getRows().size() which allocated 25k HashMaps
 * for a 50k-row table with 50% visibility. Post-fix: countVisibleRows() increments
 * a counter with no HashMap allocations.
 */
@Tag("concurrency")
class SnapshotGetRowCountIsLinearTest {

    @Test
    void rowCountIsCheapAndCorrect() {
        Database database = new Database();
        Table table = new Table(database, "count_test", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 50k rows
        for (int i = 0; i < 50_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        // Mark every other row as uncommitted delete (50% visibility)
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        for (int i = 0; i < 50_000; i += 2) {
            table.markDelete(i, txid);
        }
        
        // Create snapshot
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "count_test", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // First call: O(N) count walk (should be < 50ms)
        long start = System.nanoTime();
        int count = snapshot.getRowCount();
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        
        assertEquals(25_000, count, "Expected 25k visible rows (50% of 50k)");
        assertTrue(durationMs < 50, 
                "getRowCount() of 50k rows with 50% visibility took " + durationMs + "ms, expected < 50ms");
        
        // Second call: O(1) from memoized count
        long start2 = System.nanoTime();
        int count2 = snapshot.getRowCount();
        long duration2Ms = (System.nanoTime() - start2) / 1_000_000;
        
        assertEquals(25_000, count2);
        assertTrue(duration2Ms < 1, 
                "Second getRowCount() call took " + duration2Ms + "ms, expected < 1ms (O(1))");
        
        // Cross-check: count should equal getRows().size()
        List<Map<String, Object>> rows = snapshot.getRows();
        assertEquals(count, rows.size(), "Count should match materialized list size");
    }
    
    @Test
    void rowCountOnSmallTable() {
        Database database = new Database();
        Table table = new Table(database, "small", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 10 rows
        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "small", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        assertEquals(10, snapshot.getRowCount());
        
        // Mark one row as deleted
        table.markDelete(0, txid);
        
        // Create a new snapshot after the deletion to see the updated state
        TransactionTableSnapshot newSnapshot = new TransactionTableSnapshot(
                "small", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        assertEquals(9, newSnapshot.getRowCount());
    }
}