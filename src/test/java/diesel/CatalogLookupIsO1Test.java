package diesel;

import diesel.storage.StorageType;
import diesel.storage.page.CatalogSchema;
import diesel.storage.page.CatalogTable;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 Buffer-1/2/3: the catalog name index must be O(1) per operation, not the
 * old O(N) list scan ({@code schemas.stream().filter(...)} / {@code removeIf}).
 *
 * <p>Each method builds N=100k schemas in an in-memory catalog
 * ({@code new CatalogTable(null)} makes {@code flush()} a no-op) and asserts the
 * measured wall time of the hot call. With the old linear-scan implementation
 * 100k operations over a 100k-entry list cost ~5*10^9 string comparisons
 * (tens of seconds); the bound below is set 30x above the expected O(1) time
 * (~10-100 ms) so the test fails a regression without being load-sensitive.
 * {@code @Timeout} catches a regression even faster than the wall bound.
 *
 * <p>Consolidates the three prompt-named classes (CatalogLookupIsO1Test,
 * CatalogUpdateIsO1Test, CatalogDropIsO1Test) into one file.
 */
@Tag("smoke")
@Tag("storage")
class CatalogLookupIsO1Test {

    private static final int TABLE_COUNT = 100_000;
    private static final long BOUND_MILLIS = 2_000;

    private static CatalogSchema schemaFor(String tableName) {
        return new CatalogSchema(
                tableName,
                List.of(new CatalogSchema.ColumnSchema("ID", "Long", false, true)),
                "ID",
                StorageType.CSV,
                List.of(),
                List.of());
    }

    private static void loadTables(CatalogTable catalog, int count) {
        for (int i = 0; i < count; i++) {
            catalog.updateTableSchema(schemaFor("T" + i));
        }
    }

    @Test
    @Timeout(30)
    void lookupIsConstantTime() {
        CatalogTable catalog = new CatalogTable(null);
        loadTables(catalog, TABLE_COUNT);
        assertEquals(TABLE_COUNT, catalog.getTableCount());

        long start = System.nanoTime();
        for (int i = 0; i < TABLE_COUNT; i++) {
            CatalogSchema schema = catalog.getTableSchema("T" + i);
            assertNotNull(schema, "schema T" + i + " must be found");
            assertEquals("T" + i, schema.getTableName());
            if (i % 10 == 0) {
                assertNull(catalog.getTableSchema("MISSING" + i), "unknown table must miss");
                assertFalse(catalog.tableExists("MISSING" + i));
            }
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < BOUND_MILLIS,
                "100k lookups over a 100k catalog took " + elapsedMillis
                        + " ms, expected < " + BOUND_MILLIS + " ms (O(1) name index)");
    }

    @Test
    @Timeout(30)
    void updateIsConstantTime() {
        CatalogTable catalog = new CatalogTable(null);
        int half = TABLE_COUNT / 2;
        loadTables(catalog, half);

        long start = System.nanoTime();
        // 50k fresh inserts + 50k replace-in-place of existing names
        for (int i = 0; i < half; i++) {
            catalog.updateTableSchema(schemaFor("T" + i));
            catalog.updateTableSchema(schemaFor("N" + i));
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(TABLE_COUNT, catalog.getTableCount());
        assertNotNull(catalog.getTableSchema("T0"));
        assertEquals("T0", catalog.getTableSchema("T0").getTableName());
        assertNotNull(catalog.getTableSchema("N49999"));
        assertEquals("N0", catalog.getTableSchema("N0").getTableName());
        assertTrue(elapsedMillis < BOUND_MILLIS,
                "100k updateTableSchema calls over a 100k catalog took " + elapsedMillis
                        + " ms, expected < " + BOUND_MILLIS + " ms (O(1) name index)");
    }

    @Test
    @Timeout(30)
    void dropIsConstantTime() {
        CatalogTable catalog = new CatalogTable(null);
        loadTables(catalog, TABLE_COUNT);

        int dropCount = 10_000;
        long start = System.nanoTime();
        for (int i = 0; i < dropCount; i++) {
            catalog.dropTableSchema("T" + i);
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(TABLE_COUNT - dropCount, catalog.getTableCount());
        assertNull(catalog.getTableSchema("T0"));
        assertFalse(catalog.tableExists("T9999"));
        assertNotNull(catalog.getTableSchema("T10000"));
        assertTrue(elapsedMillis < BOUND_MILLIS,
                dropCount + " dropTableSchema calls over a 100k catalog took " + elapsedMillis
                        + " ms, expected < " + BOUND_MILLIS + " ms (O(1) name index)");
    }
}
