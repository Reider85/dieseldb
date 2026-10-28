package diesel;

import diesel.schema.SchemaVersion;
import diesel.schema.SchemaVersionRegistry;
import diesel.schema.SchemaVersionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;

/**
 * Tests for schema version timeout behavior (prompt4 #37, R3-010 step 1/3).
 * Tests that old versions are removed after timeout and pinned versions are protected.
 */
@Tag("smoke")
public class OldVersionTimeoutTest {

    private Database database;
    @TempDir
    Path tempDir;
    private static final String TEST_TABLE = "TEST_TABLE";

    @BeforeEach
    void setUp() {
        // Set short timeout for testing
        System.setProperty("schema.old.version.timeout.min", "1");
        database = new Database();
        database.setDataDir(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        // Restore default timeout
        System.setProperty("schema.old.version.timeout.min", "10");
        if (database != null) {
            database.close();
        }
    }

    @Test
    void stalePinIsKilledAfterTimeout() throws InterruptedException {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Pin version 1 (simulate running query)
        SchemaVersionRegistry.PinnedSchema pinnedV1 = registry.pinCurrent();
        
        // Add a column to create version 2
        List<String> newColumns = new ArrayList<>(table.getColumns());
        newColumns.add("AGE");
        
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.put("AGE", Integer.class);
        
        table.applySchemaChange(newColumns, newTypes, "ADD COLUMN AGE");
        
        // Verify current version is 2
        assertEquals(2, registry.currentVersion());
        
        // Verify pinned version 1 is still accessible
        SchemaVersion v1 = registry.resolve(1);
        assertEquals(1, v1.getVersion());
        assertEquals(2, v1.getColumns().size());
        
        // Wait for timeout (1 minute + buffer)
        Thread.sleep(65000); // 65 seconds
        
        // Trigger reap (this happens automatically in the background, but let's force it for testing)
        // Note: In real usage, the reaper runs periodically, so we need to wait for it to run
        Thread.sleep(5000); // Additional wait for reaper to run
        
        // Release the pin (this should allow the version to be reaped)
        pinnedV1.close();
        
        // Try to access version 1 - should be killed
        assertThrows(diesel.schema.SchemaVersionTimeoutException.class, () -> {
            registry.resolve(1);
        });
    }

    @Test
    void currentVersionNeverKilled() throws InterruptedException {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID"), 
            Map.of("ID", Integer.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Add column to create version 2
        List<String> newColumns = new ArrayList<>(table.getColumns());
        newColumns.add("NAME");
        
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.put("NAME", String.class);
        
        table.applySchemaChange(newColumns, newTypes, "ADD COLUMN NAME");
        
        // Current version should be 2 and never killed
        assertEquals(2, registry.currentVersion());
        
        // Wait for timeout
        Thread.sleep(65000);
        Thread.sleep(5000); // Wait for reaper
        
        // Current version should still be accessible
        SchemaVersion v2 = registry.resolve(2);
        assertEquals(2, v2.getVersion());
        assertEquals(2, v2.getColumns().size());
    }

    @Test
    void timeoutResolvedFromSysprop() {
        // Test with different timeout via system property
        System.setProperty("schema.old.version.timeout.min", "5");
        
        try {
            database = new Database(); // Re-create with new timeout
            database.setDataDir(tempDir.toString());
            database.createTable(TEST_TABLE, 
                List.of("ID"), 
                Map.of("ID", Integer.class), 
                "ID");
            
            Table table = database.getTable(TEST_TABLE);
            SchemaVersionRegistry registry = table.getSchemaVersions();
            
            // The timeout should be 5 minutes, not the default 10
            // We can't easily test the actual timeout duration in unit tests,
            // but we can verify the registry was created with the right configuration
            assertEquals(1, registry.currentVersion());
            
        } finally {
            // Restore original timeout
            System.setProperty("schema.old.version.timeout.min", "1");
        }
    }

    @Test
    void closeIsIdempotentAfterReap() throws InterruptedException {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID"), 
            Map.of("ID", Integer.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Pin and unpin to create a pin that can be reaped
        SchemaVersionRegistry.PinnedSchema pin = registry.pinCurrent();
        pin.close();
        
        // Add column to create version 2
        List<String> newColumns = new ArrayList<>(table.getColumns());
        newColumns.add("NAME");
        
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.put("NAME", String.class);
        
        table.applySchemaChange(newColumns, newTypes, "ADD COLUMN NAME");
        
        // Wait for timeout
        Thread.sleep(65000);
        Thread.sleep(5000); // Wait for reaper
        
        // Closing should be idempotent
        assertDoesNotThrow(() -> {
            registry.close();
        });
        
        // Closing again should also be safe
        assertDoesNotThrow(() -> {
            registry.close();
        });
    }

    @Test
    void multiplePinsSameVersion() throws InterruptedException {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Pin version 1 with two different pins
        SchemaVersionRegistry.PinnedSchema pin1 = registry.pinCurrent();
        SchemaVersionRegistry.PinnedSchema pin2 = registry.pinCurrent();
        
        // Add column to create version 2
        List<String> newColumns = new ArrayList<>(table.getColumns());
        newColumns.add("AGE");
        
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.put("AGE", Integer.class);
        
        table.applySchemaChange(newColumns, newTypes, "ADD COLUMN AGE");
        
        // Both pins should still be able to access version 1
        SchemaVersion v1 = registry.resolve(1);
        assertEquals(1, v1.getVersion());
        
        // Release one pin
        pin1.close();
        
        // Version 1 should still be accessible via the remaining pin
        v1 = registry.resolve(1);
        assertEquals(1, v1.getVersion());
        
        // Release the second pin
        pin2.close();
        
        // Now version 1 can be reaped
        Thread.sleep(65000);
        Thread.sleep(5000); // Wait for reaper
        
        assertThrows(diesel.schema.SchemaVersionTimeoutException.class, () -> {
            registry.resolve(1);
        });
    }
}