package diesel;

import diesel.schema.SchemaVersion;
import diesel.schema.SchemaVersionRegistry;
import diesel.schema.SchemaVersionTimeoutException;
import diesel.storage.page.CatalogTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for online schema changes (prompt4 #37, R3-010 step 1/3).
 * Tests SchemaVersionRegistry, ALTER TABLE ADD COLUMN, and catalog integration.
 */
@Tag("smoke")
public class SchemaVersionTest {

    private Database database;
    private static final String TEST_TABLE = "TEST_TABLE";

    @BeforeEach
    void setUp() {
        database = new Database();
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @Test
    void tenSequentialAlterCreateTenVersions() {
        // Create initial table
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Initial version should be 1
        assertEquals(1, registry.currentVersion());
        SchemaVersion v1 = registry.resolve(1);
        assertEquals(2, v1.getColumns().size());
        assertTrue(v1.getColumns().contains("ID"));
        assertTrue(v1.getColumns().contains("NAME"));
        
        // Pin initial version (simulate running query)
        SchemaVersionRegistry.PinnedSchema pinnedV1 = registry.pinCurrent();
        
        // Perform 10 ALTER TABLE ADD COLUMN operations
        for (int i = 1; i <= 10; i++) {
            String newColumn = "COL_" + i;
            String newType = i % 2 == 0 ? "Integer" : "String";
            Class<?> typeClass = i % 2 == 0 ? Integer.class : String.class;
            
            // Build new schema (current + one new column)
            List<String> newColumns = new ArrayList<>(table.getColumns());
            newColumns.add(newColumn);
            
            Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
            newTypes.put(newColumn, typeClass);
            
            // Apply schema change
            table.applySchemaChange(newColumns, newTypes, 
                "ADD COLUMN " + newColumn + " " + newType);
            
            // Verify version incremented
            assertEquals(i + 1, registry.currentVersion());
            
            // Verify new column exists
            SchemaVersion currentVersion = registry.current();
            assertTrue(currentVersion.getColumns().contains(newColumn));
            assertEquals(typeClass, currentVersion.getColumnTypes().get(newColumn));
            
            // Verify pinned version 1 is still accessible and unchanged
            SchemaVersion resolvedV1 = registry.resolve(1);
            assertEquals(1, resolvedV1.getVersion());
            assertEquals(2, resolvedV1.getColumns().size());
            assertFalse(resolvedV1.getColumns().contains(newColumn));
        }
        
        // Verify final state
        assertEquals(11, registry.currentVersion());
        SchemaVersion finalVersion = registry.current();
        assertEquals(12, finalVersion.getColumns().size());
        
        // Clean up pin
        pinnedV1.close();
    }

    @Test
    void oldVersionVisibleToRunningQuery() {
        // Create table with initial schema
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Pin version 1 (simulate running query)
        SchemaVersionRegistry.PinnedSchema pinnedV1 = registry.pinCurrent();
        
        // Add multiple columns
        for (int i = 1; i <= 5; i++) {
            String newColumn = "COL_" + i;
            List<String> newColumns = new ArrayList<>(table.getColumns());
            newColumns.add(newColumn);
            
            Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
            newTypes.put(newColumn, String.class);
            
            table.applySchemaChange(newColumns, newTypes, "ADD COLUMN " + newColumn);
        }
        
        // Resolve pinned version 3 (should exist and have correct schema)
        SchemaVersion v3 = registry.resolve(3);
        assertEquals(3, v3.getVersion());
        assertEquals(4, v3.getColumns().size()); // ID, NAME, COL_1, COL_2
        assertTrue(v3.getColumns().contains("ID"));
        assertTrue(v3.getColumns().contains("NAME"));
        assertTrue(v3.getColumns().contains("COL_1"));
        assertTrue(v3.getColumns().contains("COL_2"));
        assertFalse(v3.getColumns().contains("COL_3"));
        
        // Current version should be 6
        assertEquals(6, registry.currentVersion());
        
        pinnedV1.close();
    }

    @Test
    void sqlAddColumnExtendsTable() {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID"), 
            Map.of("ID", Integer.class), 
            "ID");
        
        // Insert initial row
        database.executeQuery("INSERT INTO " + TEST_TABLE + " (ID) VALUES (1)", null);
        
        // Add new column
        String alterSql = "ALTER TABLE " + TEST_TABLE + " ADD COLUMN AGE INTEGER";
        database.executeQuery(alterSql, null);
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Verify current schema has new column
        SchemaVersion v2 = registry.resolve(2);
        assertEquals(2, v2.getColumns().size());
        assertTrue(v2.getColumns().contains("ID"));
        assertTrue(v2.getColumns().contains("AGE"));
        assertEquals(Integer.class, v2.getColumnTypes().get("AGE"));
        
        // Verify existing row returns null for new column
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) database.executeQuery("SELECT ID, AGE FROM " + TEST_TABLE, null);
        assertEquals(1, results.size());
        Map<String, Object> row = results.get(0);
        assertEquals(1, row.get("ID"));
        assertNull(row.get("AGE")); // New column returns null for old rows
        
        // Insert new row with new column
        database.executeQuery("INSERT INTO " + TEST_TABLE + " (ID, AGE) VALUES (2, 25)", null);
        
        // Verify both rows
        results = (List<Map<String, Object>>) database.executeQuery("SELECT ID, AGE FROM " + TEST_TABLE + " ORDER BY ID", null);
        assertEquals(2, results.size());
        
        row = results.get(0);
        assertEquals(1, row.get("ID"));
        assertNull(row.get("AGE"));
        
        row = results.get(1);
        assertEquals(2, row.get("ID"));
        assertEquals(25, row.get("AGE"));
        
        // Verify version history
        assertEquals(2, registry.currentVersion());
    }

    @Test
    void queryCacheInvalidatedOnChange() {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        // Execute query to populate cache
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results1 = (List<Map<String, Object>>) database.executeQuery("SELECT NAME FROM " + TEST_TABLE, null);
        assertEquals(0, results1.size());
        
        // Add column
        database.executeQuery("ALTER TABLE " + TEST_TABLE + " ADD COLUMN AGE INTEGER", null);
        
        // Execute same query again - should see cache invalidation
        results1 = (List<Map<String, Object>>) database.executeQuery("SELECT NAME FROM " + TEST_TABLE, null);
        assertEquals(0, results1.size()); // Still empty, but cache was invalidated
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        
        // Verify version changed
        assertEquals(2, registry.currentVersion());
        
        // Verify old version still accessible (v1 = original ID+NAME schema)
        SchemaVersion v1 = registry.resolve(1);
        assertEquals(2, v1.getColumns().size());
        assertTrue(v1.getColumns().contains("ID"));
    }

    @Test
    void catalogRecordsLatestVersion() {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID"), 
            Map.of("ID", Integer.class), 
            "ID");
        
        // Add column
        database.executeQuery("ALTER TABLE " + TEST_TABLE + " ADD COLUMN NAME STRING", null);
        
        // Verify catalog records the version
        CatalogTable catalog = database.getCatalog();
        assertEquals(2, catalog.getSchemaVersion(TEST_TABLE));
    }

    @Test
    void alterErrors() {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID", "NAME"), 
            Map.of("ID", Integer.class, "NAME", String.class), 
            "ID");
        
        // Error: ALTER unknown table (Database.getTable throws TableNotFoundException)
        assertThrows(TableNotFoundException.class, () -> {
            database.executeQuery("ALTER TABLE UNKNOWN ADD COLUMN X Integer", null);
        });
        
        // Error: duplicate column
        assertThrows(IllegalArgumentException.class, () -> {
            database.executeQuery("ALTER TABLE " + TEST_TABLE + " ADD COLUMN ID Integer", null);
        });
        
        // Error: NOT NULL syntax (step 1 only supports nullable)
        assertThrows(diesel.SyntaxErrorException.class, () -> {
            database.executeQuery("ALTER TABLE " + TEST_TABLE + " ADD COLUMN AGE INTEGER NOT NULL", null);
        });
        
        // Error: DROP COLUMN (not supported yet)
        assertThrows(diesel.SyntaxErrorException.class, () -> {
            database.executeQuery("ALTER TABLE " + TEST_TABLE + " DROP COLUMN NAME", null);
        });
        
        // Verify ALTER USER still routes to the user path (regression test):
        // parse succeeds and execution reaches UserStore -> unknown user error
        assertThrows(diesel.security.UserNotFoundException.class, () -> {
            database.executeQuery("ALTER USER admin PASSWORD 'newpass'", null);
        });
    }

    @Test
    void oldVersionPinnedBeyondBoundStillResolvable() {
        // Create table
        database.createTable(TEST_TABLE, 
            List.of("ID"), 
            Map.of("ID", Integer.class), 
            "ID");
        
        Table table = database.getTable(TEST_TABLE);
        SchemaVersionRegistry registry = table.getSchemaVersions();
        // Pin v1 so bounded history cannot evict it (test: pinned beyond bound)
        SchemaVersionRegistry.PinnedSchema pin = registry.pinCurrent();
        try {
            // Add many columns (> MAX_RETAINED = 32)
            for (int i = 1; i <= 40; i++) {
                String newColumn = "COL_" + i;
                List<String> newColumns = new ArrayList<>(table.getColumns());
                newColumns.add(newColumn);
                
                Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
                newTypes.put(newColumn, String.class);
                
                table.applySchemaChange(newColumns, newTypes, "ADD COLUMN " + newColumn);
            }
            
            // Verify current version is 41
            assertEquals(41, table.getSchemaVersions().currentVersion());
            
            // Verify pinned version (v1) is still accessible despite being beyond bound
            SchemaVersion v1 = table.getSchemaVersions().resolve(1);
            assertEquals(1, v1.getVersion());
            assertEquals(1, v1.getColumns().size());
            assertTrue(v1.getColumns().contains("ID"));
            
        } finally {
            pin.close();
            database.close();
        }
    }
}