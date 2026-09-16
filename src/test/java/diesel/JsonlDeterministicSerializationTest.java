package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowWriter;
import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 51 - deterministic JSONL serialization: byte-reproducible output
 * regardless of JVM run, platform, or Map implementation.
 *
 * <p>Guards: (1) same data written twice produces byte-identical files;
 * (2) BigDecimal scale is preserved (100.50 != 100.5); (3) nested-object
 * keys are alphabetically sorted; (4) non-ASCII strings are UTF-8 (no
 * unicode escapes); (5) control characters are escaped; (6) no BOM or
 * platform line endings leak into the output.
 */
@Tag("storage")
class JsonlDeterministicSerializationTest {

    @TempDir
    Path tempDir;

    // ── helpers ─────────────────────────────────────────────────────────

    private static List<String> cols(String... names) {
        return List.of(names);
    }

    private String writeOnce(List<String> columns, Map<String, Object> rowData,
                             JsonParserConfig config) throws Exception {
        Path file = tempDir.resolve("test.jsonl");
        try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
             JsonlRowWriter w = new JsonlRowWriter(bw, new JsonlSchemaManager(columns, null, config))) {
            w.writeRow(rowData);
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    // ── 1. Byte-identical across two writes ─────────────────────────────

    @Test
    void doubleWriteProducesIdenticalBytes() throws Exception {
        List<String> columns = cols("X", "Y", "Z");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("X", 42L);
        data.put("Y", "hello");
        data.put("Z", true);

        String first = writeOnce(columns, data, JsonParserConfig.defaults());
        String second = writeOnce(columns, data, JsonParserConfig.defaults());
        assertEquals(first, second, "two writes of the same data must be byte-identical");
    }

    // ── 2. BigDecimal preserves scale ───────────────────────────────────

    @Test
    void bigDecimalPreservesTrailingZeros() throws Exception {
        List<String> columns = cols("VAL");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("VAL", new BigDecimal("100.50"));

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains("100.50"), "BigDecimal 100.50 must write as 100.50, got: " + out);
        assertFalse(out.contains("100.5\n") || out.endsWith("100.5"),
                "BigDecimal 100.50 must NOT be truncated to 100.5, got: " + out);
    }

    @Test
    void bigIntegerNoExponent() throws Exception {
        List<String> columns = cols("VAL");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("VAL", new BigDecimal("9007199254740993"));

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains("9007199254740993"),
                "large integer must not use exponent notation, got: " + out);
        assertFalse(out.contains("E"), "must not contain exponent, got: " + out);
    }

    // ── 3. Unicode strings — UTF-8, no unicode escapes ───────────────────

    @Test
    void unicodeStringWrittenAsUtf8() throws Exception {
        List<String> columns = cols("TEXT");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("TEXT", "\u041f\u0440\u0438\u0432\u0435\u0442 \u043c\u0438\u0440 \u4f60\u597d \uD83C\uDF89");

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains("\u041f\u0440\u0438\u0432\u0435\u0442"), "Cyrillic must be UTF-8, not escaped, got: " + out);
        assertTrue(out.contains("\u4f60\u597d"), "CJK must be UTF-8, not escaped, got: " + out);
        assertFalse(out.contains("\\u"), "must not contain unicode escapes, got: " + out);
    }

    // ── 4. Control characters are escaped ────────────────────────────────

    @Test
    void controlCharactersAreEscaped() throws Exception {
        List<String> columns = cols("TEXT");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("TEXT", "line1\nline2\ttab\rcr");

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains("\\n"), "newline must be escaped, got: " + out);
        assertTrue(out.contains("\\t"), "tab must be escaped, got: " + out);
        assertTrue(out.contains("\\r"), "carriage return must be escaped, got: " + out);
    }

    // ── 5. Boolean and null literals ────────────────────────────────────

    @Test
    void booleanAndNullLiteralsExact() throws Exception {
        List<String> columns = cols("A", "B", "C");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("A", true);
        data.put("B", false);
        data.put("C", null);

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains(":true"), "boolean true must be lowercase :true");
        assertTrue(out.contains(":false"), "boolean false must be lowercase :false");
        assertTrue(out.contains(":null"), "null must be lowercase :null");
        assertFalse(out.contains(":True") || out.contains(":False") || out.contains(":NULL"),
                "literals must be lowercase");
    }

    // ── 6. Nested object keys sorted alphabetically ─────────────────────

    @Test
    void nestedObjectKeysSortedAlphabetically() throws Exception {
        List<String> columns = cols("DATA");
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("zebra", 1);
        nested.put("apple", 2);
        nested.put("mango", 3);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("DATA", nested);

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        int appleIdx = out.indexOf("\"apple\"");
        int mangoIdx = out.indexOf("\"mango\"");
        int zebraIdx = out.indexOf("\"zebra\"");
        assertTrue(appleIdx < mangoIdx && mangoIdx < zebraIdx,
                "nested keys must be sorted alphabetically, got: " + out);
    }

    // ── 7. Nested object keys deterministic for HashMap ─────────────────

    @Test
    void nestedHashMapKeysStillSorted() throws Exception {
        List<String> columns = cols("DATA");
        Map<String, Object> nested = new HashMap<>();
        nested.put("c", 3);
        nested.put("a", 1);
        nested.put("b", 2);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("DATA", nested);

        String first = writeOnce(columns, data, JsonParserConfig.defaults());
        String second = writeOnce(columns, data, JsonParserConfig.defaults());
        assertEquals(first, second, "HashMap nested keys must produce identical output across writes");
        int aIdx = first.indexOf("\"a\"");
        int bIdx = first.indexOf("\"b\"");
        int cIdx = first.indexOf("\"c\"");
        assertTrue(aIdx < bIdx && bIdx < cIdx,
                "HashMap keys must be sorted alphabetically, got: " + first);
    }

    // ── 8. Array order preserved ────────────────────────────────────────

    @Test
    void arrayOrderPreserved() throws Exception {
        List<String> columns = cols("ITEMS");
        List<Object> items = new ArrayList<>();
        items.add(3);
        items.add(1);
        items.add(2);
        items.add("abc");
        items.add(null);
        items.add(true);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("ITEMS", items);

        String out = writeOnce(columns, data, JsonParserConfig.defaults());
        assertTrue(out.contains("[3,1,2,\"abc\",null,true]"),
                "array order must be preserved exactly, got: " + out);
    }

    // ── 9. Full row round-trip — all types deterministic ────────────────

    @Test
    void fullRowAllTypesDeterministic() throws Exception {
        List<String> columns = cols("ID", "NAME", "SCORE", "RATIO", "ACTIVE", "TAGS");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("ID", 42L);
        data.put("NAME", "\u0422\u0435\u0441\u0442");
        data.put("SCORE", 99);
        data.put("RATIO", new BigDecimal("3.14"));
        data.put("ACTIVE", true);
        data.put("TAGS", List.of("a", "b"));

        String first = writeOnce(columns, data, JsonParserConfig.defaults());
        String second = writeOnce(columns, data, JsonParserConfig.defaults());
        assertEquals(first, second, "full row must be byte-identical across writes");
        assertTrue(first.contains("\"ID\":42"));
        assertTrue(first.contains("\"SCORE\":99"));
        assertTrue(first.contains("\"RATIO\":3.14"));
        assertTrue(first.contains("\"ACTIVE\":true"));
        assertTrue(first.contains("\"TAGS\":[\"a\",\"b\"]"));
    }

    // ── 10. No BOM in output ───────────────────────────────────────────

    @Test
    void noByteOrderMark() throws Exception {
        List<String> columns = cols("X");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("X", "hello");

        Path file = tempDir.resolve("bom_test.jsonl");
        try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
             JsonlRowWriter w = new JsonlRowWriter(bw, columns)) {
            w.writeRow(data);
        }
        byte[] bytes = Files.readAllBytes(file);
        assertFalse(bytes.length >= 3 && (bytes[0] & 0xFF) == 0xEF
                        && (bytes[1] & 0xFF) == 0xBB && (bytes[2] & 0xFF) == 0xBF,
                "output must not start with UTF-8 BOM");
    }

    // ── 11. Line endings are \n, not \r\n ───────────────────────────────

    @Test
    void lineEndingIsLfNotCrlf() throws Exception {
        List<String> columns = cols("X");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("X", "hello");

        Path file = tempDir.resolve("lineend_test.jsonl");
        try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
             JsonlRowWriter w = new JsonlRowWriter(bw, columns)) {
            w.writeRow(data);
        }
        byte[] bytes = Files.readAllBytes(file);
        for (byte b : bytes) {
            if (b == 0x0D) {
                fail("output must use LF not CRLF, found 0x0D");
            }
        }
        assertEquals(0x0A, bytes[bytes.length - 1], "file must end with LF");
    }

    // ── 12. Both backends produce deterministic output independently ─────

    @Test
    void jacksonBackendDeterministic() throws Exception {
        List<String> columns = cols("A", "B");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("A", new BigDecimal("100.50"));
        data.put("B", "text");

        JsonParserConfig jackson = JsonParserConfig.defaultsFor(JsonParserConfig.Backend.JACKSON);
        String first = writeOnce(columns, data, jackson);
        String second = writeOnce(columns, data, jackson);
        assertEquals(first, second, "Jackson backend must be deterministic");
        assertTrue(first.contains("100.50"), "Jackson must preserve BigDecimal scale");
    }

    @Test
    void gsonBackendDeterministic() throws Exception {
        List<String> columns = cols("A", "B");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("A", new BigDecimal("100.50"));
        data.put("B", "text");

        JsonParserConfig gson = JsonParserConfig.defaultsFor(JsonParserConfig.Backend.GSON);
        String first = writeOnce(columns, data, gson);
        String second = writeOnce(columns, data, gson);
        assertEquals(first, second, "Gson backend must be deterministic");
    }

    // ── 13. Nested objects with mixed value types ─────────────────────────

    @Test
    void nestedObjectWithMixedTypesDeterministic() throws Exception {
        List<String> columns = cols("DATA");
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("str", "hello");
        nested.put("num", 42);
        nested.put("flt", 3.14);
        nested.put("bool", true);
        nested.put("nil", null);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("DATA", nested);

        String first = writeOnce(columns, data, JsonParserConfig.defaults());
        String second = writeOnce(columns, data, JsonParserConfig.defaults());
        assertEquals(first, second, "nested object with mixed types must be deterministic");
        int boolIdx = first.indexOf("\"bool\"");
        int fltIdx = first.indexOf("\"flt\"");
        int nilIdx = first.indexOf("\"nil\"");
        int numIdx = first.indexOf("\"num\"");
        int strIdx = first.indexOf("\"str\"");
        assertTrue(boolIdx < fltIdx && fltIdx < nilIdx && nilIdx < numIdx && numIdx < strIdx,
                "nested keys must be sorted alphabetically, got: " + first);
    }
}
