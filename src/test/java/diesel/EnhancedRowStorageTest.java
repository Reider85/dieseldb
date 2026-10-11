package diesel.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * Tests for the enhanced RowStorage interface methods introduced in Prompt 21.
 * Verifies that the new abstractions reduce code duplication and provide
 * better functionality for storage implementations.
 */
@Tag("query")
class EnhancedRowStorageTest {

    private RowStorage storage;
    private final String tableName = "test_table";
    private final List<String> columns = List.of("id", "name", "age");
    private final Map<String, Class<?>> columnTypes = Map.of(
        "id", Integer.class,
        "name", String.class,
        "age", Integer.class
    );

    @BeforeEach
    void setUp() {
        storage = new InMemoryRowStorage(tableName, columns, columnTypes);
    }

    @Test
    void testStorageMetadata() {
        StorageMetadata metadata = storage.getMetadata();
        assertNotNull(metadata);
        assertEquals("InMemoryRowStorage", metadata.getStorageType());
        assertFalse(metadata.supportsPersistence());
        assertFalse(metadata.supportsMVCC());
        assertEquals(0, metadata.getProperties().get("rowCount"));
    }

    @Test
    void testCallWithConsumer() {
        // Test that static callWith Consumer executes the operation
        RowStorage.callWith(storage, s -> {
            s.insert(createTestRow(1, "Alice", 25));
            s.insert(createTestRow(2, "Bob", 30));
        });

        List<Map<String, Object>> rows = storage.scan();
        assertEquals(2, rows.size());
        assertEquals("Alice", rows.get(0).get("name"));
        assertEquals("Bob", rows.get(1).get("name"));
    }

    @Test
    void testCallWithFunction() {
        // Test that static callWith Function returns the operation result
        storage.insert(createTestRow(1, "Alice", 25));
        
        String result = RowStorage.callWith(storage, s -> {
            List<Map<String, Object>> rows = s.scan();
            return "Found " + rows.size() + " rows";
        }, "No storage");

        assertEquals("Found 1 rows", result);
    }

    @Test
    void testBulkInsert() {
        List<Map<String, Object>> rows = List.of(
            createTestRow(1, "Alice", 25),
            createTestRow(2, "Bob", 30),
            createTestRow(3, "Charlie", 35)
        );

        storage.bulkInsert(rows);

        List<Map<String, Object>> scannedRows = storage.scan();
        assertEquals(3, scannedRows.size());
        assertEquals("Alice", scannedRows.get(0).get("name"));
        assertEquals("Bob", scannedRows.get(1).get("name"));
        assertEquals("Charlie", scannedRows.get(2).get("name"));
    }

    @Test
    void testBulkUpdate() {
        // Insert initial rows
        storage.insert(createTestRow(1, "Alice", 25));
        storage.insert(createTestRow(2, "Bob", 30));

        List<Integer> indices = List.of(0, 1);
        List<Map<String, Object>> updates = List.of(
            createTestRow(1, "Alice Smith", 26),
            createTestRow(2, "Bob Johnson", 31)
        );

        storage.bulkUpdate(indices, updates);

        List<Map<String, Object>> scannedRows = storage.scan();
        assertEquals("Alice Smith", scannedRows.get(0).get("name"));
        assertEquals(26, scannedRows.get(0).get("age"));
        assertEquals("Bob Johnson", scannedRows.get(1).get("name"));
        assertEquals(31, scannedRows.get(1).get("age"));
    }

    @Test
    void testBulkDelete() {
        // Insert initial rows
        storage.insert(createTestRow(1, "Alice", 25));
        storage.insert(createTestRow(2, "Bob", 30));
        storage.insert(createTestRow(3, "Charlie", 35));

        // Delete Alice (index 0) and Bob (index 1)
        // Note: bulkDelete should handle index shifting automatically
        storage.bulkDelete(List.of(0, 1));

        List<Map<String, Object>> scannedRows = storage.scan();
        assertEquals(1, scannedRows.size());
        assertEquals("Charlie", scannedRows.get(0).get("name"));
    }

    @Test
    void testBulkUpdateSizeMismatchThrowsException() {
        storage.insert(createTestRow(1, "Alice", 25));

        List<Integer> indices = List.of(0);
        List<Map<String, Object>> updates = List.of(
            createTestRow(1, "Alice Smith", 26),
            createTestRow(2, "Bob", 30) // Extra update
        );

        assertThrows(IllegalArgumentException.class, () -> {
            storage.bulkUpdate(indices, updates);
        });
    }

    @Test
    void testMVCCMethodsDefaultToRegularOperations() {
        // Test that MVCC methods delegate to regular operations by default
        storage.insertWithMVCC(createTestRow(1, "Alice", 25), 123L);
        
        List<Map<String, Object>> rows = storage.scan();
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("name"));

        storage.updateWithMVCC(0, createTestRow(1, "Alice Smith", 26), 123L);
        
        rows = storage.scan();
        assertEquals("Alice Smith", rows.get(0).get("name"));
        assertEquals(26, rows.get(0).get("age"));

        storage.deleteWithMVCC(0, 123L);
        
        rows = storage.scan();
        assertEquals(0, rows.size());
    }

    @Test
    void testNullSafety() {
        // Null receiver: the operation must NOT execute and no exception must
        // be thrown (an instance method call on null would throw NPE before
        // entering the method body — the whole point of the static helper).
        assertDoesNotThrow(() -> RowStorage.callWith((RowStorage) null,
                (Consumer<RowStorage>) s -> { throw new IllegalStateException("must not run"); }));

        // Null receiver + Function: default value is returned, op not executed.
        String nullReceiver = RowStorage.callWith((RowStorage) null,
                s -> "must not run", "Default value");
        assertEquals("Default value", nullReceiver);

        // Null operation: no exception, default value returned.
        RowStorage.callWith(storage, (Consumer<RowStorage>) null);
        String nullOperation = RowStorage.callWith(storage, null, "Default value");
        assertEquals("Default value", nullOperation);
    }

    @Test
    void testCsvRowStorageEnhancements() {
        RowStorage csvStorage = new CsvRowStorage(tableName, columns, columnTypes);
        
        StorageMetadata metadata = csvStorage.getMetadata();
        assertNotNull(metadata);
        assertEquals("CsvRowStorage", metadata.getStorageType());
        assertTrue(metadata.supportsPersistence());
        assertFalse(metadata.supportsMVCC());
        
        // Test that CSV storage can handle bulk operations
        csvStorage.bulkInsert(List.of(
            createTestRow(1, "Alice", 25),
            createTestRow(2, "Bob", 30)
        ));

        List<Map<String, Object>> rows = csvStorage.scan();
        assertEquals(2, rows.size());
    }

    private Map<String, Object> createTestRow(int id, String name, int age) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("age", age);
        return row;
    }
}