package diesel.format;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link FormatRegistry}: registration, lookup, resolution and
 * per-table config override.
 */
class FormatRegistryTest {

    @BeforeEach
    void setUp() {
        FormatRegistry.unregister("CUSTOM");
    }

    @AfterEach
    void tearDown() {
        FormatRegistry.unregister("CUSTOM");
    }

    @Test
    void builtInFormatsAreRegistered() {
        Set<String> names = FormatRegistry.getRegisteredFormats();
        assertTrue(names.contains("CSV"), "CSV should be registered");
        assertTrue(names.contains("PARQUET"), "PARQUET should be registered");
    }

    @Test
    void getReturnsCorrectFormat() {
        TableFormat csv = FormatRegistry.get("CSV");
        assertNotNull(csv);
        assertEquals("CSV", csv.getName());
        assertEquals(".csv", csv.getFileExtension());

        TableFormat parquet = FormatRegistry.get("PARQUET");
        assertNotNull(parquet);
        assertEquals("PARQUET", parquet.getName());
        assertEquals(".parquet", parquet.getFileExtension());
    }

    @Test
    void getIsCaseInsensitive() {
        assertNotNull(FormatRegistry.get("csv"));
        assertNotNull(FormatRegistry.get("Csv"));
        assertNotNull(FormatRegistry.get("PARQUET"));
        assertNotNull(FormatRegistry.get("parquet"));
    }

    @Test
    void getReturnsNullForUnknown() {
        assertNull(FormatRegistry.get("UNKNOWN"));
        assertNull(FormatRegistry.get(null));
        assertNull(FormatRegistry.get(""));
    }

    @Test
    void registerAndUnregister() {
        TableFormat custom = new TableFormat() {
            @Override public String getName() { return "CUSTOM"; }
            @Override public String getDescription() { return "Custom"; }
            @Override public String getFileExtension() { return ".custom"; }
            @Override public FormatCapabilities getCapabilities() { return FormatCapabilities.rowBased(); }
            @Override public void write(TableData d, java.nio.file.Path p, WriteOptions o) {}
            @Override public TableData read(java.nio.file.Path p, ReadOptions o) { return null; }
            @Override public TableData inferSchema(java.nio.file.Path p) { return null; }
        };
        TableFormat prev = FormatRegistry.register(custom);
        assertNull(prev);
        assertTrue(FormatRegistry.getRegisteredFormats().contains("CUSTOM"));

        TableFormat removed = FormatRegistry.unregister("CUSTOM");
        assertNotNull(removed);
        assertEquals("CUSTOM", removed.getName());
        assertNull(FormatRegistry.get("CUSTOM"));
    }

    @Test
    void registerReplacesExisting() {
        CsvFormat first = new CsvFormat();
        FormatRegistry.register(first);

        CsvFormat second = new CsvFormat();
        TableFormat prev = FormatRegistry.register(second);
        assertNotNull(prev);
        assertSame(first, prev);
    }

    @Test
    void registerRejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () -> {
            FormatRegistry.register(new TableFormat() {
                @Override public String getName() { return "  "; }
                @Override public String getDescription() { return ""; }
                @Override public String getFileExtension() { return ".blank"; }
                @Override public FormatCapabilities getCapabilities() { return FormatCapabilities.rowBased(); }
                @Override public void write(TableData d, java.nio.file.Path p, WriteOptions o) {}
                @Override public TableData read(java.nio.file.Path p, ReadOptions o) { return null; }
                @Override public TableData inferSchema(java.nio.file.Path p) { return null; }
            });
        });
    }

    @Test
    void resolveWithPerTableConfig() {
        Map<String, String> config = Map.of(
                "storage.format.MY_TABLE", "PARQUET",
                "storage.format.default", "CSV"
        );
        TableFormat resolved = FormatRegistry.resolve("MY_TABLE", config);
        assertNotNull(resolved);
        assertEquals("PARQUET", resolved.getName());
    }

    @Test
    void resolveFallsBackToDefault() {
        Map<String, String> config = Map.of(
                "storage.format.default", "PARQUET"
        );
        TableFormat resolved = FormatRegistry.resolve("ANY_TABLE", config);
        assertNotNull(resolved);
        assertEquals("PARQUET", resolved.getName());
    }

    @Test
    void resolveFallsBackToGlobal() {
        Map<String, String> config = Map.of(
                "storage.format", "PARQUET"
        );
        TableFormat resolved = FormatRegistry.resolve("ANY_TABLE", config);
        assertNotNull(resolved);
        assertEquals("PARQUET", resolved.getName());
    }

    @Test
    void resolveFallsBackToDefaultFormatWhenEmpty() {
        Map<String, String> config = Map.of();
        TableFormat resolved = FormatRegistry.resolve("ANY_TABLE", config);
        assertNotNull(resolved);
        assertEquals("CSV", resolved.getName());
    }

    @Test
    void resolveWithNullConfig() {
        TableFormat resolved = FormatRegistry.resolve("ANY_TABLE", null);
        assertNotNull(resolved);
        assertEquals("CSV", resolved.getName());
    }

    @Test
    void resolveByNameValue() {
        TableFormat resolved = FormatRegistry.resolve("PARQUET");
        assertNotNull(resolved);
        assertEquals("PARQUET", resolved.getName());

        TableFormat csvResolved = FormatRegistry.resolve("CSV");
        assertNotNull(csvResolved);
        assertEquals("CSV", csvResolved.getName());
    }

    @Test
    void resolveByNameValueFallsBack() {
        TableFormat resolved = FormatRegistry.resolve("NONEXISTENT");
        assertNotNull(resolved);
        assertEquals("CSV", resolved.getName());
    }

    @Test
    void perTableOverridesDefault() {
        Map<String, String> config = Map.of(
                "storage.format.MY_TABLE", "CSV",
                "storage.format.default", "PARQUET"
        );
        TableFormat resolved = FormatRegistry.resolve("MY_TABLE", config);
        assertEquals("CSV", resolved.getName());
    }

    @Test
    void formatCapabilitiesAreCorrect() {
        CsvFormat csv = (CsvFormat) FormatRegistry.get("CSV");
        FormatCapabilities caps = csv.getCapabilities();
        assertFalse(caps.isColumnar());
        assertFalse(caps.supportsProjection());
        assertFalse(caps.supportsPredicatePushdown());
        assertFalse(caps.supportsCompression());

        ParquetFormat parquet = (ParquetFormat) FormatRegistry.get("PARQUET");
        FormatCapabilities pcaps = parquet.getCapabilities();
        assertTrue(pcaps.isColumnar());
        assertTrue(pcaps.supportsProjection());
        assertTrue(pcaps.supportsPredicatePushdown());
        assertTrue(pcaps.supportsCompression());
    }
}
