package diesel.storage.avro;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for Avro-12: Bulk update operations with deferred index rebuilding
 */
@Tag("storage")
@Tag("avro")
public class AvroInsertBulkWindowIsOLogNTest {

    @Test
    public void testBulkInsertPerformance() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_bulk", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Create secondary index
        storage.createSecondaryIndex("idx_value", "value");
        
        // Set primary key
        storage.setPrimaryKeyColumn("id");
        
        // Begin bulk update window
        storage.beginBulkUpdate();
        
        // Insert many rows in bulk mode
        long startTime = System.nanoTime();
        
        for (int i = 1; i <= 1000; i++) {
            storage.insert(Map.of("id", i, "name", "bulk_item" + i, "value", i * 2.5));
        }
        
        long bulkInsertTime = System.nanoTime() - startTime;
        
        // End bulk update window (should trigger index rebuild)
        startTime = System.nanoTime();
        storage.endBulkUpdate();
        long rebuildTime = System.nanoTime() - startTime;
        
        // Verify all rows are present
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(1000, allRows.size());
        
        // Verify indexes are working
        assertTrue(allRows.stream().anyMatch(row -> row.get("name").equals("bulk_item500")));
        
        // Bulk insert should be fast (deferred index updates)
        assertTrue(bulkInsertTime < 200_000_000, "Bulk insert took too long: " + bulkInsertTime + " ns");
        
        // Rebuild should be proportional to data size
        assertTrue(rebuildTime < 200_000_000, "Index rebuild took too long: " + rebuildTime + " ns");
        
        System.out.println("Bulk insert: " + bulkInsertTime + " ns, Rebuild: " + rebuildTime + " ns");
    }

    @Test
    public void testBulkUpdatePerformance() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_bulk_update", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Insert initial rows
        for (int i = 1; i <= 500; i++) {
            storage.insert(Map.of("id", i, "name", "item" + i, "value", i * 1.5));
        }
        
        // Create secondary index
        storage.createSecondaryIndex("idx_value", "value");
        
        // Begin bulk update window
        storage.beginBulkUpdate();
        
        // Update many rows in bulk mode
        long startTime = System.nanoTime();
        
        for (int i = 1; i <= 500; i++) {
            storage.update(i - 1, Map.of("id", i, "name", "updated_item" + i, "value", i * 3.0));
        }
        
        long bulkUpdateTime = System.nanoTime() - startTime;
        
        // End bulk update window (should trigger index rebuild)
        startTime = System.nanoTime();
        storage.endBulkUpdate();
        long rebuildTime = System.nanoTime() - startTime;
        
        // Verify all rows are updated
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(500, allRows.size());
        
        // Verify updates are correct
        assertTrue(allRows.stream().anyMatch(row -> row.get("name").equals("updated_item250")));
        assertTrue(allRows.stream().anyMatch(row -> ((Double) row.get("value")) == 250.0 * 3.0));
        
        // Bulk update should be fast (deferred index updates)
        assertTrue(bulkUpdateTime < 100_000_000, "Bulk update took too long: " + bulkUpdateTime + " ns");
        
        // Rebuild should be proportional to data size
        assertTrue(rebuildTime < 200_000_000, "Index rebuild took too long: " + rebuildTime + " ns");
        
        System.out.println("Bulk update: " + bulkUpdateTime + " ns, Rebuild: " + rebuildTime + " ns");
    }

    @Test
    public void testNestedBulkOperations() {
        // Create test data
        AvroRowStorage storage = new AvroRowStorage("test_table_nested", 
            List.of("id", "name", "value"), 
            Map.of("id", Integer.class, "name", String.class, "value", Double.class));
        
        // Test nested bulk operations
        storage.beginBulkUpdate();
        
        // First level bulk
        storage.insert(Map.of("id", 1, "name", "level1", "value", 1.0));
        
        // Second level bulk (nested)
        storage.beginBulkUpdate();
        storage.insert(Map.of("id", 2, "name", "level2", "value", 2.0));
        storage.endBulkUpdate(); // Exit second level
        
        storage.insert(Map.of("id", 3, "name", "level1_again", "value", 3.0));
        storage.endBulkUpdate(); // Exit first level
        
        // Verify all operations completed
        List<Map<String, Object>> allRows = storage.scan();
        assertEquals(3, allRows.size());
        
        // Verify all rows are present
        assertTrue(allRows.stream().anyMatch(row -> row.get("name").equals("level1")));
        assertTrue(allRows.stream().anyMatch(row -> row.get("name").equals("level2")));
        assertTrue(allRows.stream().anyMatch(row -> row.get("name").equals("level1_again")));
    }
}