package diesel.storage.avro;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for Avro-11: Delete with tombstones and compaction
 */
@Tag("storage")
@Tag("avro")
public class AvroDeleteIsOLogNTest {

    @Test
    public void testDeleteWithTombstones() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_delete", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Insert initial rows
        for (int i = 1; i <= 100; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Verify initial state
        assertEquals(100, storage.scan().size());
        
        // Delete some rows (mark as tombstones)
        storage.delete(10);  // Delete row 10
        storage.delete(20);  // Delete row 20
        storage.delete(30);  // Delete row 30
        
        // Verify scan skips deleted rows
        List<Map<String, Object>> remainingRows = storage.scan();
        assertEquals(97, remainingRows.size());
        
        // Verify the deleted rows are not in scan results
        assertFalse(remainingRows.stream().anyMatch(row -> row.get("id").equals(11)));
        assertFalse(remainingRows.stream().anyMatch(row -> row.get("id").equals(21)));
        assertFalse(remainingRows.stream().anyMatch(row -> row.get("id").equals(31)));
        
        // Verify internal rows still exist (tombstoned)
        assertEquals(100, storage.getInternalRows().size());
    }

    @Test
    public void testCompactionTrigger() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_compaction", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Insert 100 rows
        for (int i = 1; i <= 100; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Delete 26 rows (26% of 100, should trigger compaction)
        // Delete by row id instead of index to avoid issues after compaction
        for (int i = 1; i <= 26; i++) {
            int rowIndex = findRowIndexById(storage, i);
            storage.delete(rowIndex);
        }
        
        // Check if compaction was triggered by checking row counts
        List<Map<String, Object>> remainingRows = storage.scan();
        int scannedCount = remainingRows.size();
        int internalCount = storage.getInternalRows().size();
        
        // After deleting 26 rows from 100 with compaction:
        // - 25 deletions triggered compaction (100 -> 75 rows)
        // - 1 more deletion (75 -> 74 visible rows, 1 tombstoned)
        assertEquals(74, scannedCount, "Scan should return 74 visible rows");
        assertEquals(75, internalCount, "Internal storage should have 75 total rows with 1 tombstoned");
        
        // Verify we have exactly 1 tombstoned row
        int tombstonedCount = internalCount - scannedCount;
        assertEquals(1, tombstonedCount, "Should have exactly 1 tombstoned row");
        
        
        
        // Verify remaining rows are correct
        for (int i = 27; i <= 100; i++) {
            final int finalI = i;
            boolean found = remainingRows.stream().anyMatch(row -> row.get("id").equals(finalI));
            System.out.println("Checking for id " + i + ": " + (found ? "FOUND" : "NOT FOUND"));
            assertTrue(found, "Row with id " + i + " should be present");
        }
    }

    @Test
    public void testDeletePerformance() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_perf", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Insert 1000 rows
        for (int i = 1; i <= 1000; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Measure delete performance
        long startTime = System.nanoTime();
        
        // Delete every 10th row
        for (int i = 9; i < 1000; i += 10) {
            storage.delete(i);
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Verify deletes worked
        List<Map<String, Object>> remainingRows = storage.scan();
        assertEquals(900, remainingRows.size());
        
        // Performance assertion: delete should be O(log n) per operation
        assertTrue(duration < 1_000_000, "Delete operations took too long: " + duration + " ns");
        
        System.out.println("100 delete operations took: " + duration + " ns");
    }
    
    private int findRowIndexById(AvroRowStorage storage, int id) {
        List<Object[]> internalRows = storage.getInternalRows();
        String[] columns = {"id", "name", "value"};
        for (int i = 0; i < internalRows.size(); i++) {
            Object[] rowArray = internalRows.get(i);
            for (int j = 0; j < columns.length && j < rowArray.length; j++) {
                if (columns[j].equals("id") && rowArray[j] instanceof Integer && rowArray[j].equals(id)) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("Row with id " + id + " not found");
    }
}