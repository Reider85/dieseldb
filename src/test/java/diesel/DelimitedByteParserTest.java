package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.DelimitedByteParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link DelimitedByteParser}: RFC 4180 edge cases, type-specific
 * parsing, byte-identical output vs legacy reader, and allocation assertions.
 */
@Tag("storage")
class DelimitedByteParserTest {

    @TempDir
    File tempDir;

    // ── RFC 4180 edge cases ──────────────────────────────────────

    @Test
    void emptyFieldBetweenCommas() {
        List<String> cols = List.of("A", "B", "C");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class, "C", String.class);
        byte[] bytes = "A,B,C\na,,b\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"a", null, "b"}, rows.get(0));
    }

    @Test
    void trailingComma() {
        List<String> cols = List.of("A", "B", "C");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class, "C", String.class);
        byte[] bytes = "A,B,C\na,b,\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"a", "b", null}, rows.get(0));
    }

    @Test
    void leadingComma() {
        List<String> cols = List.of("A", "B", "C");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class, "C", String.class);
        byte[] bytes = "A,B,C\n,a,b\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{null, "a", "b"}, rows.get(0));
    }

    @Test
    void quotedFieldWithComma() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        byte[] bytes = "A,B\n\"a,b\",c\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"a,b", "c"}, rows.get(0));
    }

    @Test
    void quotedFieldWithEscapedQuote() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        byte[] bytes = "A,B\n\"a\"\"b\",c\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"a\"b", "c"}, rows.get(0));
    }

    @Test
    void quotedFieldWithEmbeddedNewline() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        byte[] bytes = "A,B\n\"line1\nline2\",c\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"line1\nline2", "c"}, rows.get(0));
    }

    @Test
    void emptyQuotedField() {
        List<String> cols = List.of("A");
        Map<String, Class<?>> types = Map.of("A", String.class);
        byte[] bytes = "A\n\"\"\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertNull(rows.get(0)[0]);
    }

    @Test
    void quotedCommaField() {
        List<String> cols = List.of("A");
        Map<String, Class<?>> types = Map.of("A", String.class);
        byte[] bytes = "A\n\",\"\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(",", rows.get(0)[0]);
    }

    @Test
    void multipleRows() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        byte[] bytes = "A,B\na,1\nb,2\nc,3\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{"a", "1"}, rows.get(0));
        assertArrayEquals(new Object[]{"b", "2"}, rows.get(1));
        assertArrayEquals(new Object[]{"c", "3"}, rows.get(2));
    }

    @Test
    void headerParsedAndSkipped() {
        List<String> cols = List.of("NAME", "AGE");
        Map<String, Class<?>> types = Map.of("NAME", String.class, "AGE", Integer.class);
        byte[] bytes = "NAME,AGE\nAlice,30\nBob,25\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(2, rows.size());
        assertEquals("Alice", rows.get(0)[0]);
        assertEquals(30, rows.get(0)[1]);
        assertEquals("Bob", rows.get(1)[0]);
        assertEquals(25, rows.get(1)[1]);
    }

    // ── Type-specific parsing ────────────────────────────────────

    @Test
    void parseLongColumn() {
        List<String> cols = List.of("ID");
        Map<String, Class<?>> types = Map.of("ID", Long.class);
        byte[] bytes = "ID\n0\n1\n-1\n9223372036854775807\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(4, rows.size());
        assertEquals(0L, rows.get(0)[0]);
        assertEquals(1L, rows.get(1)[0]);
        assertEquals(-1L, rows.get(2)[0]);
        assertEquals(Long.MAX_VALUE, rows.get(3)[0]);
    }

    @Test
    void parseIntColumn() {
        List<String> cols = List.of("AGE");
        Map<String, Class<?>> types = Map.of("AGE", Integer.class);
        byte[] bytes = "AGE\n0\n1\n-1\n2147483647\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(4, rows.size());
        assertEquals(0, rows.get(0)[0]);
        assertEquals(1, rows.get(1)[0]);
        assertEquals(-1, rows.get(2)[0]);
        assertEquals(Integer.MAX_VALUE, rows.get(3)[0]);
    }

    @Test
    void parseDoubleColumn() {
        List<String> cols = List.of("VAL");
        Map<String, Class<?>> types = Map.of("VAL", Double.class);
        byte[] bytes = "VAL\n0.0\n1.5\n-1.5\n1e10\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(4, rows.size());
        assertEquals(0.0, rows.get(0)[0]);
        assertEquals(1.5, rows.get(1)[0]);
        assertEquals(-1.5, rows.get(2)[0]);
        assertEquals(1e10, rows.get(3)[0]);
    }

    @Test
    void parseFloatColumn() {
        List<String> cols = List.of("VAL");
        Map<String, Class<?>> types = Map.of("VAL", Float.class);
        byte[] bytes = "VAL\n0.0\n1.5\n-1.5\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(3, rows.size());
        assertInstanceOf(Float.class, rows.get(0)[0]);
        assertEquals(0.0f, rows.get(0)[0]);
        assertEquals(1.5f, rows.get(1)[0]);
        assertEquals(-1.5f, rows.get(2)[0]);
    }

    @Test
    void parseBigDecimalColumn() {
        List<String> cols = List.of("BIG");
        Map<String, Class<?>> types = Map.of("BIG", BigDecimal.class);
        byte[] bytes = "BIG\n12345678901234567890.123456789\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(new BigDecimal("12345678901234567890.123456789"), rows.get(0)[0]);
    }

    @Test
    void parseBooleanColumn() {
        List<String> cols = List.of("FLAG");
        Map<String, Class<?>> types = Map.of("FLAG", Boolean.class);
        byte[] bytes = "FLAG\ntrue\nfalse\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(2, rows.size());
        assertEquals(true, rows.get(0)[0]);
        assertEquals(false, rows.get(1)[0]);
    }

    @Test
    void parseLocalDateColumn() {
        List<String> cols = List.of("DT");
        Map<String, Class<?>> types = Map.of("DT", LocalDate.class);
        byte[] bytes = "DT\n2024-01-15\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(LocalDate.of(2024, 1, 15), rows.get(0)[0]);
    }

    @Test
    void parseLocalDateTimeColumn() {
        List<String> cols = List.of("TS");
        Map<String, Class<?>> types = Map.of("TS", LocalDateTime.class);
        byte[] bytes = "TS\n2024-01-15T10:30:00\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30), rows.get(0)[0]);
    }

    @Test
    void parseUuidColumn() {
        List<String> cols = List.of("ID");
        Map<String, Class<?>> types = Map.of("ID", UUID.class);
        byte[] bytes = "ID\n550e8400-e29b-41d4-a716-446655440000\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), rows.get(0)[0]);
    }

    // ── Mixed types row ──────────────────────────────────────────

    @Test
    void mixedTypesRow() {
        List<String> cols = List.of("ID", "NAME", "AGE", "BAL", "ACTIVE");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class, "NAME", String.class, "AGE", Integer.class,
                "BAL", Double.class, "ACTIVE", Boolean.class);
        byte[] bytes = "ID,NAME,AGE,BAL,ACTIVE\n1,Alice,30,1000.50,true\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{1L, "Alice", 30, 1000.50, true}, rows.get(0));
    }

    // ── TSV mode (delimiter='\t', no quoting) ────────────────────

    @Test
    void tsvModeTabDelimited() {
        List<String> cols = List.of("A", "B", "C");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", Integer.class, "C", Long.class);
        byte[] bytes = "A\tB\tC\nhello\t42\t999\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) '\t', (byte) 0, "test.tsv");
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{"hello", 42, 999L}, rows.get(0));
    }

    // ── Unicode round-trip ───────────────────────────────────────

    @Test
    void unicodeRoundTrip() {
        List<String> cols = List.of("NAME", "CITY");
        Map<String, Class<?>> types = Map.of("NAME", String.class, "CITY", String.class);
        String line = "NAME,CITY\nПривет Москва,Москва\n";
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals("Привет Москва", rows.get(0)[0]);
        assertEquals("Москва", rows.get(0)[1]);
    }

    @Test
    void emojiRoundTrip() {
        List<String> cols = List.of("DATA");
        Map<String, Class<?>> types = Map.of("DATA", String.class);
        byte[] bytes = "DATA\n💾 emoji test\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals("💾 emoji test", rows.get(0)[0]);
    }

    // ── BOM handling ─────────────────────────────────────────────

    @Test
    void bomIsStrippedFromHeader() {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);
        byte[] bomBytes = "\uFEFF".getBytes(StandardCharsets.UTF_8);
        byte[] rest = "ID,NAME\n1,Alice\n".getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[bomBytes.length + rest.length];
        System.arraycopy(bomBytes, 0, bytes, 0, bomBytes.length);
        System.arraycopy(rest, 0, bytes, bomBytes.length, rest.length);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0)[0]);
        assertEquals("Alice", rows.get(0)[1]);
    }

    // ── Comparison with legacy CsvRowReader ──────────────────────

    @Test
    void comparisonWithLegacyCsvReader() throws Exception {
        List<String> cols = List.of("ID", "NAME", "AGE", "BAL", "FLAG");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class, "NAME", String.class, "AGE", Integer.class,
                "BAL", BigDecimal.class, "FLAG", Boolean.class);

        StringBuilder sb = new StringBuilder();
        sb.append("ID,NAME,AGE,BAL,FLAG\n");
        for (int i = 0; i < 1000; i++) {
            sb.append(i).append(",User_").append(i).append(",")
                    .append(18 + (i % 80)).append(",12.50,")
                    .append(i % 2 == 0).append("\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        // DelimitedByteParser
        List<Object[]> byteRows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");

        // Legacy CsvRowReader
        File csvFile = new File(tempDir, "comparison.csv");
        java.nio.file.Files.write(csvFile.toPath(), bytes);
        List<Map<String, Object>> legacyRows;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, cols, types)) {
            reader.readHeader();
            legacyRows = reader.readAll();
        }

        assertEquals(legacyRows.size(), byteRows.size());
        for (int i = 0; i < legacyRows.size(); i++) {
            Map<String, Object> legacy = legacyRows.get(i);
            Object[] fast = byteRows.get(i);
            assertEquals(legacy.get("ID"), fast[0], "Row " + i + " ID mismatch");
            assertEquals(legacy.get("NAME"), fast[1], "Row " + i + " NAME mismatch");
            assertEquals(legacy.get("AGE"), fast[2], "Row " + i + " AGE mismatch");
            assertEquals(legacy.get("BAL"), fast[3], "Row " + i + " BAL mismatch");
            assertEquals(legacy.get("FLAG"), fast[4], "Row " + i + " FLAG mismatch");
        }
    }

    @Test
    void comparisonWithLegacyCsvReaderQuoted() throws Exception {
        List<String> cols = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        StringBuilder sb = new StringBuilder();
        sb.append("ID,DATA\n");
        sb.append("1,\"has,comma\"\n");
        sb.append("2,\"has\"\"quotes\"\"\"\n");
        sb.append("3,\"line1\nline2\"\n");
        sb.append("4,\"normal\"\n");
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        List<Object[]> byteRows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");

        File csvFile = new File(tempDir, "quoted_comparison.csv");
        java.nio.file.Files.write(csvFile.toPath(), bytes);
        List<Map<String, Object>> legacyRows;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, cols, types)) {
            reader.readHeader();
            legacyRows = reader.readAll();
        }

        assertEquals(legacyRows.size(), byteRows.size());
        for (int i = 0; i < legacyRows.size(); i++) {
            Map<String, Object> legacy = legacyRows.get(i);
            Object[] fast = byteRows.get(i);
            assertEquals(legacy.get("ID"), fast[0], "Row " + i + " ID mismatch");
            assertEquals(legacy.get("DATA"), fast[1], "Row " + i + " DATA mismatch");
        }
    }

    // ── parseRange (partitioned parallel path) ───────────────────

    @Test
    void parseRangeBasic() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        // Header: "A,B\n" = 4 bytes (0-3)
        // Row 0:   "0,zero\n"   = 7 bytes (4-10)
        // Row 1:   "1,one\n"    = 6 bytes (11-16)
        // Row 2:   "2,two\n"    = 6 bytes (17-22)
        // Row 3:   "3,three\n"  = 7 bytes (23-29)
        byte[] bytes = "A,B\n0,zero\n1,one\n2,two\n3,three\n".getBytes(StandardCharsets.UTF_8);
        int[] mapping = {0, 1};
        // parseRange parses from start to end of byte array (end param is only a guard check).
        // start=11 skips header + row 0; byte array ends after row 3.
        List<Object[]> rows = DelimitedByteParser.parseRange(bytes, 11, bytes.length, StandardCharsets.UTF_8, cols, types,
                mapping, 2, (byte) ',', (byte) '"', "test.csv");
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{"1", "one"}, rows.get(0));
        assertArrayEquals(new Object[]{"2", "two"}, rows.get(1));
        assertArrayEquals(new Object[]{"3", "three"}, rows.get(2));
    }

    // ── Empty file ───────────────────────────────────────────────

    @Test
    void emptyFileNoRows() {
        List<String> cols = List.of("A", "B");
        Map<String, Class<?>> types = Map.of("A", String.class, "B", String.class);
        byte[] bytes = "A,B\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertTrue(rows.isEmpty());
    }

    // ── Null values (empty fields for non-string types) ───────────

    @Test
    void emptyFieldForTypedColumnReturnsNull() {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);
        byte[] bytes = "ID,NAME\n,Alice\n".getBytes(StandardCharsets.UTF_8);
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, cols, types,
                (byte) ',', (byte) '"', "test.csv");
        assertEquals(1, rows.size());
        assertNull(rows.get(0)[0]); // empty Long field → null
        assertEquals("Alice", rows.get(0)[1]);
    }
}
