package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 MVCC-3 regression: {@link TransactionTableSnapshot#getRow(int)} must not
 * call the defensive-copy {@link Table#getRows()} (which would materialize the
 * entire row list). It should use an early-exit O(N) walk when the rows list
 * is not memoized.
 * 
 * <p>Pre-fix: getRow(index) called getRows().get(index) which allocated N HashMaps.
 * Post-fix: getVisibleRow(index) walks until the index-th visible row is found,
 * returning only that row with no full list materialization.
 */
@Tag("concurrency")
class SnapshotGetRowIsLinearTest {

    /** Table that counts calls to the defensive-copy getRows() API. */
    static final class CountingTable extends Table {
        final AtomicInteger getRowsCalls = new AtomicInteger();

        CountingTable(Database database, String name) {
            super(database, name, List.of("id", "name"),
                    Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        }

        @Override
        public List<Map<String, Object>> getRows() {
            getRowsCalls.incrementAndGet();
            return super.getRows();
        }
    }

    @Test
    void getRowCorrectness() {
        Database database = new Database();
        CountingTable table = new CountingTable(database, "getrow_test");
        
        // Add 100 rows
        for (int i = 0; i < 100; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "getrow_test", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // Test first row
        Map<String, Object> firstRow = snapshot.getRow(0);
        assertEquals(0, firstRow.get("id"));
        assertEquals("user0", firstRow.get("name"));
        
        // Test middle row
        Map<String, Object> middleRow = snapshot.getRow(49);
        assertEquals(49, middleRow.get("id"));
        assertEquals("user49", middleRow.get("name"));
        
        // Test last row
        Map<String, Object> lastRow = snapshot.getRow(99);
        assertEquals(99, lastRow.get("id"));
        assertEquals("user99", lastRow.get("name"));
        
        // Table.getRows() should never be called during getRow
        assertEquals(0, table.getRowsCalls.get(), 
                "Table.getRows() was called during getRow — should use early-exit walk");
    }
    
    @Test
    void getRowOutOfBounds() {
        Database database = new Database();
        Table table = new Table(database, "bounds", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 5 rows
        for (int i = 0; i < 5; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "bounds", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // Valid indices
        assertDoesNotThrow(() -> snapshot.getRow(0));
        assertDoesNotThrow(() -> snapshot.getRow(4));
        
        // Invalid indices
        assertThrows(IndexOutOfBoundsException.class, () -> snapshot.getRow(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> snapshot.getRow(5));
        assertThrows(IndexOutOfBoundsException.class, () -> snapshot.getRow(100));
        
        // Error messages should contain the actual count
        try {
            snapshot.getRow(5);
            fail("Should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            assertTrue(e.getMessage().contains("out of bounds for snapshot with 5 rows"),
                    "Error message should contain actual row count: " + e.getMessage());
        }
    }
    
    @Test
    void getRowPerformance() {
        Database database = new Database();
        Table table = new Table(database, "perf", List.of("id", "name"),
                Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add 50k rows
        for (int i = 0; i < 50_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "user" + i);
            table.addRow(row);
        }
        
        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "perf", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // Access last row (worst case: must walk almost all rows)
        long start = System.nanoTime();
        Map<String, Object> lastRow = snapshot.getRow(49_999);
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        
        assertEquals(49_999, lastRow.get("id"));
        assertEquals("user49999", lastRow.get("name"));
        assertTrue(durationMs < 50, 
                "getRow(49999) on 50k table took " + durationMs + "ms, expected < 50ms");
    }
    
    @Test
    void getRowWithMemoizedRows() {
        Database database = new Database();
        CountingTable table = new CountingTable(database, "memoized");
        
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
                "memoized", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        
        // First call to getRows() memoizes the list
        List<Map<String, Object>> rows = snapshot.getRows();
        assertEquals(10, rows.size());
        assertEquals(0, table.getRowsCalls.get(), 
                "getRows() should not be called by getRow when using memoized list");
        
        // Subsequent getRow calls should use the memoized list
        Map<String, Object> row5 = snapshot.getRow(5);
        assertEquals(5, row5.get("id"));
        assertEquals(0, table.getRowsCalls.get(), 
                "getRows() should not be called by getRow when using memoized list");
    }
}