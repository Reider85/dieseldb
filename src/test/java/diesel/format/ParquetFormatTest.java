package diesel.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ParquetFormat}: write, read, roundtrip, schema inference,
 * capabilities.
 */
class ParquetFormatTest {

    private final ParquetFormat format = new ParquetFormat();

    @TempDir
    Path tempDir;

    @Test
    void writeAndReadSimple() throws IOException {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("ID", Integer.class);
        types.put("NAME", String.class);

        TableData data = TableData.builder()
                .columns(List.of("ID", "NAME"))
                .columnTypes(types)
                .rows(List.of(
                        Map.of("ID", 1, "NAME", "Alice"),
                        Map.of("ID", 2, "NAME", "Bob")
                ))
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .metadataValue(TableData.META_FORMAT_VERSION, 3)
                .build();

        Path file = tempDir.resolve("test.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        assertTrue(file.toFile().exists());
        assertTrue(file.toFile().length() > 0);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(2, read.getRows().size());
        assertEquals("Alice", read.getRows().get(0).get("NAME"));
        assertEquals("Bob", read.getRows().get(1).get("NAME"));
    }

    @Test
    void roundtripPreservesTypes() throws IOException {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("ID", Integer.class);
        types.put("BIG", Long.class);
        types.put("Dbl", Double.class);
        types.put("Flag", Boolean.class);
        types.put("Name", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 42);
        row.put("BIG", 123456789L);
        row.put("Dbl", 3.14);
        row.put("Flag", true);
        row.put("Name", "test");

        TableData data = TableData.builder()
                .columns(List.of("ID", "BIG", "Dbl", "Flag", "Name"))
                .columnTypes(types)
                .rows(List.of(row))
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .metadataValue(TableData.META_FORMAT_VERSION, 3)
                .build();

        Path file = tempDir.resolve("types.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(1, read.getRows().size());
        Map<String, Object> r = read.getRows().get(0);
        assertEquals(42, r.get("ID"));
        assertEquals(123456789L, r.get("BIG"));
        assertEquals(3.14, (Double) r.get("Dbl"), 0.001);
        assertEquals(Boolean.TRUE, r.get("Flag"));
        assertEquals("test", r.get("Name"));
    }

    @Test
    void metadataRoundtrip() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "NAME", String.class);

        TableData data = TableData.builder()
                .columns(List.of("ID", "NAME"))
                .columnTypes(types)
                .rows(List.of(Map.of("ID", 1, "NAME", "Alice")))
                .metadataValue(TableData.META_PRIMARY_KEY, "ID")
                .metadataValue(TableData.META_FORMAT_VERSION, 3)
                .metadataValue(TableData.META_HAS_CLUSTERED_INDEX, true)
                .metadataValue(TableData.META_CLUSTERED_INDEX_COLUMN, "ID")
                .build();

        Path file = tempDir.resolve("meta.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals("ID", read.getMetadataValue(TableData.META_PRIMARY_KEY));
        assertEquals(3, read.getMetadataValue(TableData.META_FORMAT_VERSION));
        assertEquals(true, read.getMetadataValue(TableData.META_HAS_CLUSTERED_INDEX));
        assertEquals("ID", read.getMetadataValue(TableData.META_CLUSTERED_INDEX_COLUMN));
    }

    @Test
    void inferSchema() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "NAME", String.class);
        TableData data = TableData.builder()
                .columns(List.of("ID", "NAME"))
                .columnTypes(types)
                .rows(List.of(Map.of("ID", 1, "NAME", "Alice")))
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .build();

        Path file = tempDir.resolve("infer.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData schema = format.inferSchema(file);
        assertEquals(2, schema.getColumns().size());
        assertTrue(schema.getColumns().contains("ID"));
        assertTrue(schema.getColumns().contains("NAME"));
        assertTrue(schema.getRows().isEmpty());
    }

    @Test
    void canReadByExtension() {
        assertTrue(format.canRead(Path.of("test.parquet")));
        assertTrue(format.canRead(Path.of("data/MY_TABLE.PARQUET")));
        assertFalse(format.canRead(Path.of("test.csv")));
        assertFalse(format.canRead(Path.of("test.txt")));
    }

    @Test
    void formatMetadata() {
        assertEquals("PARQUET", format.getName());
        assertEquals(".parquet", format.getFileExtension());
        assertNotNull(format.getDescription());
        assertTrue(format.getCapabilities().isColumnar());
        assertTrue(format.getCapabilities().supportsProjection());
        assertTrue(format.getCapabilities().supportsPredicatePushdown());
    }

    @Test
    void emptyTable() throws IOException {
        Map<String, Class<?>> types = Map.of("A", String.class, "B", Integer.class);
        TableData data = TableData.builder()
                .columns(List.of("A", "B"))
                .columnTypes(types)
                .rows(List.of())
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .build();

        Path file = tempDir.resolve("empty.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(0, read.getRows().size());
        assertEquals(2, read.getColumns().size());
    }

    @Test
    void readWithLimit() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class);
        TableData data = TableData.builder()
                .columns(List.of("ID"))
                .columnTypes(types)
                .rows(List.of(
                        Map.of("ID", 1),
                        Map.of("ID", 2),
                        Map.of("ID", 3)
                ))
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .build();

        Path file = tempDir.resolve("limit.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, new ReadOptions(null, List.of(), 2, Map.of()));
        assertEquals(2, read.getRows().size());
    }

    @Test
    void multipleRowGroups() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "NAME", String.class);
        java.util.List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(Map.of("ID", i, "NAME", "row-" + i));
        }

        TableData data = TableData.builder()
                .columns(List.of("ID", "NAME"))
                .columnTypes(types)
                .rows(rows)
                .metadataValue(TableData.META_PRIMARY_KEY, "")
                .build();

        Path file = tempDir.resolve("multi.parquet");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(1000, read.getRows().size());
    }
}
