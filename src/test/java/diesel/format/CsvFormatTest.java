package diesel.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CsvFormat}: write, read, roundtrip, schema inference,
 * quoting, type handling.
 */
class CsvFormatTest {

    private final CsvFormat format = new CsvFormat();

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
                .build();

        Path file = tempDir.resolve("test.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        assertTrue(file.toFile().exists());

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
        row.put("BIG", 9999999999L);
        row.put("Dbl", 3.14);
        row.put("Flag", true);
        row.put("Name", "test");

        TableData data = TableData.builder()
                .columns(List.of("ID", "BIG", "Dbl", "Flag", "Name"))
                .columnTypes(types)
                .rows(List.of(row))
                .build();

        Path file = tempDir.resolve("types.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(1, read.getRows().size());
        Map<String, Object> r = read.getRows().get(0);
        assertEquals(42, r.get("ID"));
        assertEquals(9999999999L, r.get("BIG"));
        assertEquals(3.14, (Double) r.get("Dbl"), 0.001);
        assertEquals(Boolean.TRUE, r.get("Flag"));
        assertEquals("test", r.get("Name"));
    }

    @Test
    void emptyTable() throws IOException {
        TableData data = TableData.builder()
                .columns(List.of("A", "B"))
                .columnTypes(Map.of("A", String.class, "B", Integer.class))
                .rows(List.of())
                .build();

        Path file = tempDir.resolve("empty.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(0, read.getRows().size());
        assertEquals(2, read.getColumns().size());
    }

    @Test
    void nullValues() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "VAL", String.class);

        TableData data = TableData.builder()
                .columns(List.of("ID", "VAL"))
                .columnTypes(types)
                .rows(List.of(Map.of("ID", 1)))
                .build();

        Path file = tempDir.resolve("nulls.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals(1, read.getRows().size());
        assertNull(read.getRows().get(0).get("VAL"));
    }

    @Test
    void quotedStrings() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "TEXT", String.class);

        TableData data = TableData.builder()
                .columns(List.of("ID", "TEXT"))
                .columnTypes(types)
                .rows(List.of(Map.of("ID", 1, "TEXT", "hello \"world\"")))
                .build();

        Path file = tempDir.resolve("quoted.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, ReadOptions.DEFAULT);
        assertEquals("hello \"world\"", read.getRows().get(0).get("TEXT"));
    }

    @Test
    void inferSchema() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class, "NAME", String.class);
        TableData data = TableData.builder()
                .columns(List.of("ID", "NAME"))
                .columnTypes(types)
                .rows(List.of(
                        Map.of("ID", 1, "NAME", "Alice"),
                        Map.of("ID", 2, "NAME", "Bob")
                ))
                .build();

        Path file = tempDir.resolve("infer.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData schema = format.inferSchema(file);
        assertEquals(2, schema.getColumns().size());
        assertEquals("ID", schema.getColumns().get(0));
        assertEquals("NAME", schema.getColumns().get(1));
        assertTrue(schema.getRows().isEmpty());
    }

    @Test
    void canReadByExtension() {
        assertTrue(format.canRead(Path.of("test.csv")));
        assertTrue(format.canRead(Path.of("data/MY_TABLE.CSV")));
        assertFalse(format.canRead(Path.of("test.parquet")));
        assertFalse(format.canRead(Path.of("test.txt")));
    }

    @Test
    void formatMetadata() {
        assertEquals("CSV", format.getName());
        assertEquals(".csv", format.getFileExtension());
        assertNotNull(format.getDescription());
        assertFalse(format.getCapabilities().isColumnar());
    }

    @Test
    void readLimit() throws IOException {
        Map<String, Class<?>> types = Map.of("ID", Integer.class);
        TableData data = TableData.builder()
                .columns(List.of("ID"))
                .columnTypes(types)
                .rows(List.of(
                        Map.of("ID", 1),
                        Map.of("ID", 2),
                        Map.of("ID", 3)
                ))
                .build();

        Path file = tempDir.resolve("limit.csv");
        format.write(data, file, WriteOptions.DEFAULT);

        TableData read = format.read(file, new ReadOptions(null, List.of(), 2, Map.of()));
        assertEquals(2, read.getRows().size());
    }

    @Test
    void formatValueHandlesAllTypes() {
        assertEquals("", CsvFormat.formatValue(null));
        assertEquals("\"hello\"", CsvFormat.formatValue("hello"));
        assertEquals("42", CsvFormat.formatValue(42));
        assertEquals("3.14", CsvFormat.formatValue(3.14));
        assertEquals("true", CsvFormat.formatValue(true));

        LocalDate date = LocalDate.of(2026, 1, 15);
        assertEquals(date.toString(), CsvFormat.formatValue(date));

        LocalDateTime dt = LocalDateTime.of(2026, 1, 15, 10, 30);
        assertEquals(dt.toString(), CsvFormat.formatValue(dt));

        UUID uuid = UUID.randomUUID();
        assertEquals(uuid.toString(), CsvFormat.formatValue(uuid));

        BigDecimal bd = new BigDecimal("123.456");
        assertEquals("123.456", CsvFormat.formatValue(bd));
    }
}
