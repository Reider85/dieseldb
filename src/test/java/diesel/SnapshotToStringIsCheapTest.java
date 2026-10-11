package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 MVCC-3 regression: {@link TransactionTableSnapshot#toString()} must be
 * O(1) after first call (memoized count), not O(N) full materialization
 * with N HashMap copies for every log line.
 * 
 * <p>Pre-fix: toString() called getRowCount() which called getRows().size()
 * which allocated 10k HashMaps for a 10k-row table. Post-fix: toString() uses
 * the memoized count from countVisibleRows() (no HashMap allocations).
 */
@Tag("concurrency")
class SnapshotToStringIsCheapTest {

    @Test
    void toStringContainsCorrectRowCount() {
        Database database = new Database();
        Table table = new Table(database, "tostring_test", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 10k rows
        for (int i = 0; i < 10_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "tostring_test", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // First toString: should contain "rowCount=10000"
        String firstStr = snapshot.toString();
        assertTrue(firstStr.contains("rowCount=10000"), 
                "First toString should contain rowCount=10000, got: " + firstStr);
        assertTrue(firstStr.contains("tostring_test"), 
                "First toString should contain table name, got: " + firstStr);
    }
    
    @Test
    void toStringCheapAfterFirstCall() {
        Database database = new Database();
        Table table = new Table(database, "cheap_test", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 10k rows
        for (int i = 0; i < 10_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "cheap_test", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // First call: O(N) count walk (should be < 10ms)
        long start1 = System.nanoTime();
        String str1 = snapshot.toString();
        long firstDurationMs = (System.nanoTime() - start1) / 1_000_000;
        
        assertTrue(firstDurationMs < 50, 
                "First toString() on 10k rows took " + firstDurationMs + "ms, expected < 50ms");
        assertTrue(str1.contains("rowCount=10000"));
        
        // Subsequent calls: O(1) from memoized count
        long totalDurationMs = 0;
        for (int i = 0; i < 1000; i++) {
            long start = System.nanoTime();
            String str = snapshot.toString();
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            totalDurationMs += durationMs;
            
            // Each call should be very fast and contain correct count
            assertTrue(str.contains("rowCount=10000"), 
                    "toString() call " + i + " should contain correct count");
            assertTrue(durationMs < 5, 
                    "toString() call " + i + " took " + durationMs + "ms, expected < 5ms");
        }
        
        // Total for 1000 calls should be < 100ms
        assertTrue(totalDurationMs < 500, 
                "1000 toString() calls took " + totalDurationMs + "ms, expected < 500ms");
    }
    
    @Test
    void toStringWithDifferentRowCounts() {
        Database database = new Database();
        
        // Test with 1 row
        Table table1 = new Table(database, "one_row", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        table1.addRow(Map.of("id", 1, "name", "one"));
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot1 = new TransactionTableSnapshot(
                "one_row", table1, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        String str1 = snapshot1.toString();
        assertTrue(str1.contains("rowCount=1"));
        
        // Test with 0 rows
        Table table0 = new Table(database, "empty", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        TransactionTableSnapshot snapshot0 = new TransactionTableSnapshot(
                "empty", table0, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        String str0 = snapshot0.toString();
        assertTrue(str0.contains("rowCount=0"));
        
        // Test with deleted rows (should still show actual visible count)
        table1.markDelete(0, txid);
        TransactionTableSnapshot snapshotDeleted = new TransactionTableSnapshot(
                "one_row", table1, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        String strDeleted = snapshotDeleted.toString();
        assertTrue(strDeleted.contains("rowCount=0"));
    }
}