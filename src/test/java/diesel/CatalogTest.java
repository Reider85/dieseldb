package diesel;

import diesel.storage.StorageType;
import diesel.storage.page.CatalogTable;
import diesel.storage.page.CatalogSchema;
import diesel.storage.page.Page;
import diesel.storage.page.PageHeader;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.storage.page.PageType;
import diesel.storage.page.PinnedPage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CatalogTable and CatalogSchema functionality.
 */
@Tag("storage")
class CatalogTest {
    
    @TempDir
    Path tempDir;
    
    private PageManager pageManager;
    private CatalogTable catalog;
    
    @BeforeEach
    void setUp() throws IOException {
        pageManager = new PageManager(tempDir.resolve("test.db"), 100, 8192);
        catalog = new CatalogTable(pageManager);
    }
    
    @Test
    void testCreateAndGetTableSchema() throws IOException {
        // Create a table schema
        List<CatalogSchema.ColumnSchema> columns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false),
            new CatalogSchema.ColumnSchema("AGE", "Integer", true, false)
        );
        
        List<String> sequences = List.of("users_seq");
        List<CatalogSchema.IndexSchema> indices = List.of(
            new CatalogSchema.IndexSchema("idx_name", List.of("NAME"), false)
        );
        
        catalog.createTableSchema(
            "USERS",
            columns,
            "ID",
            StorageType.CSV,
            sequences,
            indices
        );
        
        // Verify the schema was created
        CatalogSchema schema = catalog.getTableSchema("USERS");
        assertNotNull(schema);
        assertEquals("USERS", schema.getTableName());
        assertEquals("ID", schema.getPrimaryKey());
        assertEquals(StorageType.CSV, schema.getStorageType());
        assertEquals(3, schema.getColumns().size());
        assertEquals(1, schema.getSequences().size());
        assertEquals(1, schema.getIndices().size());
        
        // Verify column details
        CatalogSchema.ColumnSchema idColumn = schema.getColumns().get(0);
        assertEquals("ID", idColumn.getName());
        assertEquals("Long", idColumn.getType());
        assertFalse(idColumn.isNullable());
        assertTrue(idColumn.isUnique());
        
        // Verify sequence
        assertEquals("users_seq", schema.getSequences().get(0));
        
        // Verify index
        CatalogSchema.IndexSchema index = schema.getIndices().get(0);
        assertEquals("idx_name", index.getName());
        assertEquals(List.of("NAME"), index.getColumns());
        assertFalse(index.isUnique());
    }
    
    @Test
    void testTableExists() throws IOException {
        // Initially no tables exist
        assertFalse(catalog.tableExists("USERS"));
        
        // Create a table
        List<CatalogSchema.ColumnSchema> columns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false)
        );
        
        catalog.createTableSchema(
            "USERS",
            columns,
            "ID",
            StorageType.CSV,
            List.of(),
            List.of()
        );
        
        // Now table exists
        assertTrue(catalog.tableExists("USERS"));
        assertFalse(catalog.tableExists("NONEXISTENT"));
    }
    
    @Test
    void testDropTableSchema() throws IOException {
        // Create a table
        List<CatalogSchema.ColumnSchema> columns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false)
        );
        
        catalog.createTableSchema(
            "USERS",
            columns,
            "ID",
            StorageType.CSV,
            List.of(),
            List.of()
        );
        
        // Verify it exists
        assertNotNull(catalog.getTableSchema("USERS"));
        assertEquals(1, catalog.getTableCount());
        
        // Drop the table
        catalog.dropTableSchema("USERS");
        
        // Verify it's gone
        assertNull(catalog.getTableSchema("USERS"));
        assertEquals(0, catalog.getTableCount());
        assertFalse(catalog.tableExists("USERS"));
    }
    
    @Test
    void testUpdateTableSchema() throws IOException {
        // Create initial schema
        List<CatalogSchema.ColumnSchema> initialColumns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false)
        );
        
        catalog.createTableSchema(
            "USERS",
            initialColumns,
            "ID",
            StorageType.CSV,
            List.of(),
            List.of()
        );
        
        // Update with new schema
        List<CatalogSchema.ColumnSchema> updatedColumns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false),
            new CatalogSchema.ColumnSchema("AGE", "Integer", true, false)
        );
        
        List<CatalogSchema.IndexSchema> updatedIndices = List.of(
            new CatalogSchema.IndexSchema("idx_name", List.of("NAME"), false)
        );
        
        catalog.updateTableSchema(
            "USERS",
            updatedColumns,
            "ID",
            StorageType.CSV,
            List.of(),
            updatedIndices
        );
        
        // Verify the update
        CatalogSchema schema = catalog.getTableSchema("USERS");
        assertNotNull(schema);
        assertEquals(3, schema.getColumns().size());
        assertEquals(1, schema.getIndices().size());
        
        // Verify new column
        CatalogSchema.ColumnSchema ageColumn = schema.getColumns().stream()
            .filter(col -> "AGE".equals(col.getName()))
            .findFirst()
            .orElse(null);
        assertNotNull(ageColumn);
        assertEquals("AGE", ageColumn.getName());
        assertEquals("Integer", ageColumn.getType());
        
        // Verify index
        CatalogSchema.IndexSchema index = schema.getIndices().get(0);
        assertEquals("idx_name", index.getName());
        assertEquals(List.of("NAME"), index.getColumns());
    }
    
    @Test
    void testCatalogPersistence() throws IOException {
        // Create a table
        List<CatalogSchema.ColumnSchema> columns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true),
            new CatalogSchema.ColumnSchema("NAME", "String", true, false)
        );
        
        catalog.createTableSchema(
            "USERS",
            columns,
            "ID",
            StorageType.CSV,
            List.of(),
            List.of()
        );
        
        // Close and reopen the catalog (with a fresh PageManager on the same file)
        pageManager.close();
        pageManager = PageManager.open(tempDir.resolve("test.db"), 100, 8192);
        
        // Create new catalog with same page manager
        CatalogTable newCatalog = new CatalogTable(pageManager);
        newCatalog.loadCatalog();
        
        // Verify the data persisted
        CatalogSchema schema = newCatalog.getTableSchema("USERS");
        assertNotNull(schema);
        assertEquals("USERS", schema.getTableName());
        assertEquals("ID", schema.getPrimaryKey());
        assertEquals(2, schema.getColumns().size());
    }
    
    @Test
    void testCatalogBuilder() {
        // Test the builder pattern
        CatalogSchema schema = new CatalogSchema.Builder()
            .tableName("PRODUCTS")
            .addColumn("ID", "Long", false, true)
            .addColumn("NAME", "String", true, false)
            .addColumn("PRICE", "Double", true, false)
            .primaryKey("ID")
            .storageType(StorageType.TSV)
            .addSequence("products_seq")
            .addIndex("idx_name", List.of("NAME"), false)
            .build();
        
        assertEquals("PRODUCTS", schema.getTableName());
        assertEquals("ID", schema.getPrimaryKey());
        assertEquals(StorageType.TSV, schema.getStorageType());
        assertEquals(3, schema.getColumns().size());
        assertEquals(1, schema.getSequences().size());
        assertEquals(1, schema.getIndices().size());
    }
    
    @Test
    void testPageType() throws IOException {
        // Create a table to ensure page 0 is created
        List<CatalogSchema.ColumnSchema> columns = List.of(
            new CatalogSchema.ColumnSchema("ID", "Long", false, true)
        );
        
        catalog.createTableSchema(
            "TEST",
            columns,
            "ID",
            StorageType.CSV,
            List.of(),
            List.of()
        );
        
        // Verify page 0 has CATALOG type
        try (PinnedPage pinnedPage = pageManager.readPage(new PageId(0, 1, 0))) {
            Page page = pinnedPage.getPage();
            assertNotNull(page);
            assertEquals(PageType.CATALOG.getPageTypeValue(), page.getPageTypeValue());
        }
    }
}