package diesel.storage.avro;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for Avro-9: Optimized AvroSecondaryIndex.shiftPositions with binarySearch suffix-shift
 */
@Tag("storage")
@Tag("avro")
public class AvroShiftPositionsIsOAffectedTest {

    @Test
    public void testSecondaryIndexShiftPerformance() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Insert initial rows
        for (int i = 1; i <= 1000; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Create a secondary index
        storage.createSecondaryIndex("idx_value", "value");
        
        // Measure shift performance for middle insertion
        long startTime = System.nanoTime();
        
        // Insert in the middle which should trigger shift operations
        storage.insertAt(500, Map.of("id", 1001, "name", "inserted", "value", 500.5));
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Verify the insertion worked
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(1001, allRows.size());
        assertEquals(1001, allRows.get(500).get("id"));
        
        // Performance assertion: shift should be reasonable (not extremely slow)
        // Rough smoke bound (per the original test comment). The instrumented
        // phase breakdown on this machine measured the actual index work at
        // ~2ms for the 500-entry suffix (secShift=1.7-2.3ms), while a cold
        // first insertAt() in a fresh JVM pays 15-23ms of GC/JIT first-call
        // cost - the old 10ms bound sat inside that noise band and went red
        // in isolation and in batch runs alike. 50ms still fails a
        // catastrophic regression (seconds) without being load-sensitive.
        assertTrue(duration < 50_000_000, "Shift operation took too long: " + duration + " ns");
        
        System.out.println("Secondary index shift took: " + duration + " ns");
    }

    @Test
    public void testPrimaryKeyIndexShiftPerformance() {
        // Create test data with primary key
        AvroRowStorage storage = new AvroRowStorage("test_table_pk", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Set primary key
        storage.setPrimaryKeyColumn("id");
        
        // Insert initial rows
        for (int i = 1; i <= 1000; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Measure shift performance for middle insertion
        long startTime = System.nanoTime();
        
        // Insert in the middle which should trigger shift operations
        storage.insertAt(500, Map.of("id", 1001, "name", "inserted", "value", 500.5));
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Verify the insertion worked
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(1001, allRows.size());
        assertEquals(1001, allRows.get(500).get("id"));
        
        // Performance assertion: shift should be reasonable (not extremely slow)
        // Same rationale as the secondary-index bound above: cold first-call
        // GC/JIT noise measured 15-23ms against ~2ms of actual index work;
        // 50ms is a smoke bound, not a precision benchmark.
        assertTrue(duration < 50_000_000, "Primary key shift took too long: " + duration + " ns");
        
        System.out.println("Primary key index shift took: " + duration + " ns");
    }
}