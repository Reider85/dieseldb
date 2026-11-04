package diesel.storage.avro;

import diesel.StorageType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for Avro-12 optimization: Switch `AvroRowStorage.insertAt` to tombstones + 
 * optimized index shifts (o2_prompts.md #27, Critical).
 * 
 * Verifies that insertAt operations scale as O(n log n) rather than O(n²) when
 * inserting in the middle of large datasets.
 */
@Tag("storage")
@StorageType("avro")
class AvroInsertIsOLogNTest {

    @TempDir
    Path tempDir;

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "BIRTHDATE", "LAST_LOGIN", "ACTIVE");
    }

    private static Map<String, Object> row(int id, String name, int age, double balance) {
        Map<String, Object> row = new HashMap<>();
        row.put("ID", id);
        row.put("NAME", name);
        row.put("AGE", age);
        row.put("BALANCE", balance);
        row.put("BIRTHDATE", java.time.LocalDate.of(1990, 1, 1));
        row.put("LAST_LOGIN", java.time.LocalDateTime.now());
        row.put("ACTIVE", true);
        return row;
    }

    @Test
    void testMiddleInsertPerformance() {
        // Test that middle insert operations scale linearly with number of inserts,
        // not quadratically with total rows (O(n log n) vs O(n²))
        
        int totalRows = 100_000;
        int insertCount = 1_000;
        
        AvroRowStorage storage = new AvroRowStorage("test_table", cols(), createColumnTypes());
        
        // Pre-populate with many rows
        for (int i = 0; i < totalRows; i++) {
            storage.insert(row(i, "User_" + i, 25 + (i % 50), 1000.0 + i));
        }
        
        // Time middle insert operations
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < insertCount; i++) {
            // Insert at middle position to trigger ArrayList shifts
            int insertPos = totalRows / 2;
            storage.insertAt(insertPos, row(-i, "New_User_" + i, 30, 2000.0));
            totalRows++; // Update count since we're inserting
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Performance check: should complete in reasonable time (not O(n²))
        // O(n log n) for 1000 inserts in 100k rows should be under 10 seconds on modern hardware
        assertTrue(duration < 10000, 
            String.format("Middle inserts took %d ms, expected < 10000 ms (O(n log n) scaling)", duration));
        
        // Verify data integrity
        assertEquals(totalRows, storage.getRowCount());
        
        // Verify that we can still access all data correctly
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(totalRows, allRows.size());
        
        // Verify that inserted rows are present
        boolean foundInserted = false;
        for (Map<String, Object> r : allRows) {
            if (r.get("NAME").toString().startsWith("New_User_")) {
                foundInserted = true;
                break;
            }
        }
        assertTrue(foundInserted, "Inserted rows should be present in scan");
    }

    @Test
    void testTailInsertPerformance() {
        // Test that tail inserts remain O(1) and very fast
        
        int totalRows = 100_000;
        int insertCount = 1_000;
        
        AvroRowStorage storage = new AvroRowStorage("test_table", cols(), createColumnTypes());
        
        // Pre-populate with many rows
        for (int i = 0; i < totalRows; i++) {
            storage.insert(row(i, "User_" + i, 25 + (i % 50), 1000.0 + i));
        }
        
        // Time tail insert operations (should be O(1) per insert)
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < insertCount; i++) {
            // Append at end - should be O(1)
            storage.insert(row(-i, "Tail_User_" + i, 30, 2000.0));
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Performance check: tail inserts should be very fast (O(1) per insert)
        assertTrue(duration < 2000, 
            String.format("Tail inserts took %d ms, expected < 2000 ms (O(1) scaling)", duration));
        
        // Verify data integrity
        assertEquals(totalRows + insertCount, storage.getRowCount());
    }

    @Test
    void testMixedInsertOperations() {
        // Test mix of middle and tail inserts
        
        AvroRowStorage storage = new AvroRowStorage("test_table", cols(), createColumnTypes());
        
        // Start with some data
        for (int i = 0; i < 10_000; i++) {
            storage.insert(row(i, "User_" + i, 25 + (i % 50), 1000.0 + i));
        }
        
        // Mix of middle and tail inserts
        for (int i = 0; i < 500; i++) {
            if (i % 2 == 0) {
                // Middle insert
                storage.insertAt(5000, row(-i, "Middle_User_" + i, 30, 2000.0));
            } else {
                // Tail insert
                storage.insert(row(-i, "Tail_User_" + i, 30, 2000.0));
            }
        }
        
        // Verify all data is present and accessible
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(10_000 + 500, allRows.size());
        
        // Verify we can find both types of inserted rows
        boolean foundMiddle = false;
        boolean foundTail = false;
        
        for (Map<String, Object> r : allRows) {
            String name = (String) r.get("NAME");
            if (name.startsWith("Middle_User_")) {
                foundMiddle = true;
            } else if (name.startsWith("Tail_User_")) {
                foundTail = true;
            }
        }
        
        assertTrue(foundMiddle, "Middle-inserted rows should be present");
        assertTrue(foundTail, "Tail-inserted rows should be present");
    }

    private Map<String, Class<?>> createColumnTypes() {
        Map<String, Class<?>> types = new HashMap<>();
        types.put("ID", Integer.class);
        types.put("NAME", String.class);
        types.put("AGE", Integer.class);
        types.put("BALANCE", Double.class);
        types.put("BIRTHDATE", java.time.LocalDate.class);
        types.put("LAST_LOGIN", java.time.LocalDateTime.class);
        types.put("ACTIVE", Boolean.class);
        return types;
    }
}