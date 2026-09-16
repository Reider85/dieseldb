package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowWriter;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonTypeMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 43: JSONL type mapping (JsonTypeMapper). Number-precision rules
 * (2^53 boundary in DOUBLE columns, exact LONG reads, scientific notation),
 * the {@code jsonl.type.coercion} strict/lenient mode, ISO-8601/UUID/boolean
 * string conversions and the write-side 2^53 guard.
 */
@Tag("storage")
class JsonlTypeMapperTest {

    @TempDir
    Path tempDir;

    private static final String FILE_HINT = "type_mapper_test.jsonl";
    private static final String CTX = "type_mapper_test.jsonl:line 1: field 'V': ";

    private final List<String> sysPropsForRestore = new java.util.ArrayList<>();

    @AfterEach
    void restoreSystemProperties() {
        for (String key : sysPropsForRestore) {
            System.clearProperty(key);
        }
        sysPropsForRestore.clear();
    }

    private void setProperty(String key, String value) {
        sysPropsForRestore.add(key);
        System.setProperty(key, value);
    }

    private static JsonTypeMapper strict() {
        return new JsonTypeMapper(JsonParserConfig.builder()
                .typeCoercion(JsonParserConfig.CoercionMode.STRICT).build());
    }

    private static JsonTypeMapper lenient() {
        return new JsonTypeMapper(JsonParserConfig.builder()
                .typeCoercion(JsonParserConfig.CoercionMode.LENIENT).build());
    }

    private static Object map(Class<?> type, JsonEvent token, String raw) {
        return strict().toColumnValue(type, token, raw, CTX);
    }

    // ── 2^53 precision rules ─────────────────────────────────────────────

    @Test
    void two53PlusOneInLongColumnReadsExact() {
        assertEquals(9007199254740993L, map(Long.class, JsonEvent.VALUE_NUMBER_INT, "9007199254740993"));
    }

    @Test
    void two53PlusOneInDoubleColumnFails() {
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> map(Double.class, JsonEvent.VALUE_NUMBER_INT, "9007199254740993"));
        assertContains(e.getMessage(), "9007199254740993");
        assertContains(e.getMessage(), "2^53");
        assertContains(e.getMessage(), "LONG");
    }

    @Test
    void two53BoundaryInDoubleColumnAllowed() {
        assertEquals(9.007199254740992E15, map(Double.class, JsonEvent.VALUE_NUMBER_INT, "9007199254740992"));
    }

    @Test
    void decimalExceedingDoubleRangeFails() {
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> map(Double.class, JsonEvent.VALUE_NUMBER_FLOAT, "1e400"));
        assertContains(e.getMessage(), "DOUBLE");
    }

    @Test
    void scientificNotationParsedExactlyIntoLong() {
        assertEquals(100000L, map(Long.class, JsonEvent.VALUE_NUMBER_FLOAT, "1e5"));
        assertEquals(15250L, map(Long.class, JsonEvent.VALUE_NUMBER_INT, "1.525E4"));
    }

    @Test
    void scientificNotationParsedIntoDouble() {
        assertEquals(1.0E5, map(Double.class, JsonEvent.VALUE_NUMBER_FLOAT, "1e5"));
        assertEquals(0.0025, map(Double.class, JsonEvent.VALUE_NUMBER_FLOAT, "2.5E-3"));
        assertEquals(1.0E308, map(Double.class, JsonEvent.VALUE_NUMBER_FLOAT, "1e308"));
    }

    @Test
    void highPrecisionBigDecimalRoundTripsExact() {
        BigDecimal exact = new BigDecimal("123456789.123456789123456789");
        assertEquals(exact, map(BigDecimal.class, JsonEvent.VALUE_NUMBER_FLOAT, "123456789.123456789123456789"));
    }

    @Test
    void fractionalNumberIntoIntegerColumnFails() {
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> map(Integer.class, JsonEvent.VALUE_NUMBER_FLOAT, "1.5"));
        assertContains(e.getMessage(), "1.5");
        assertContains(e.getMessage(), "cannot parse");
    }

    @Test
    void integerOverflowFails() {
        assertContains(assertThrows(DieselIOException.class,
                        () -> map(Integer.class, JsonEvent.VALUE_NUMBER_INT, "2147483648")).getMessage(),
                "out of range");
        assertContains(assertThrows(DieselIOException.class,
                        () -> map(Long.class, JsonEvent.VALUE_NUMBER_INT, "9223372036854775808")).getMessage(),
                "out of range");
    }

    // ── Coercion mode ────────────────────────────────────────────────────

    @Test
    void strictRejectsStringIntoNumericColumn() {
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> strict().toColumnValue(Long.class, JsonEvent.VALUE_STRING, "1", CTX));
        assertContains(e.getMessage(), "strict mode");
        assertContains(e.getMessage(), "jsonl.type.coercion");
        assertContains(e.getMessage(), "Long");
    }

    @Test
    void lenientCoercesStringIntoNumericColumnWithWarning() throws Exception {
        try (Slf4jLogCapture capture = new Slf4jLogCapture(JsonTypeMapper.class)) {
            assertEquals(1L, lenient().toColumnValue(Long.class, JsonEvent.VALUE_STRING, "1", CTX));
            assertEquals(1, capture.eventsMatching(ch.qos.logback.classic.Level.WARN, "lenient").size());
        }
    }

    @Test
    void lenientMalformedNumericStringStillFails() {
        assertThrows(DieselIOException.class,
                () -> lenient().toColumnValue(Long.class, JsonEvent.VALUE_STRING, "not-a-number", CTX));
    }

    // ── String-to-typed conversions (allowed in both modes) ─────────────

    @Test
    void isoStringsConvertToDateDateTimeAndUuid() {
        assertEquals(LocalDate.of(2020, 1, 2),
                strict().toColumnValue(LocalDate.class, JsonEvent.VALUE_STRING, "2020-01-02", CTX));
        assertEquals(LocalDateTime.of(2020, 1, 2, 10, 0),
                strict().toColumnValue(LocalDateTime.class, JsonEvent.VALUE_STRING, "2020-01-02T10:00:00", CTX));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                strict().toColumnValue(UUID.class, JsonEvent.VALUE_STRING,
                        "123e4567-e89b-12d3-a456-426614174000", CTX));
    }

    @Test
    void nonIsoDateStringFails() {
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> strict().toColumnValue(LocalDate.class, JsonEvent.VALUE_STRING, "2020/01/02", CTX));
        assertContains(e.getMessage(), "cannot parse");
    }

    @Test
    void strictBooleanStringsConvert() {
        assertEquals(true, strict().toColumnValue(Boolean.class, JsonEvent.VALUE_STRING, "true", CTX));
        assertEquals(false, strict().toColumnValue(Boolean.class, JsonEvent.VALUE_STRING, "FALSE", CTX));
        assertEquals(true, strict().toColumnValue(Boolean.class, JsonEvent.VALUE_STRING, "1", CTX));
    }

    @Test
    void invalidBooleanStringFails() {
        assertThrows(DieselIOException.class,
                () -> strict().toColumnValue(Boolean.class, JsonEvent.VALUE_STRING, "maybe", CTX));
    }

    @Test
    void tokenShapeMismatchFails() {
        assertContains(assertThrows(DieselIOException.class,
                        () -> map(Long.class, JsonEvent.VALUE_TRUE, "true")).getMessage(),
                "not compatible");
        assertContains(assertThrows(DieselIOException.class,
                        () -> map(Boolean.class, JsonEvent.VALUE_NUMBER_INT, "1")).getMessage(),
                "not compatible");
    }

    // ── Untyped / null / string columns ──────────────────────────────────

    @Test
    void nullTokenMapsToNullForAnyColumn() {
        assertNull(strict().toColumnValue(Long.class, JsonEvent.VALUE_NULL, "null", CTX));
        assertNull(strict().toColumnValue(null, JsonEvent.VALUE_NULL, "null", CTX));
    }

    @Test
    void stringAndUntypedColumnsKeepRawText() {
        assertEquals("1", strict().toColumnValue(null, JsonEvent.VALUE_NUMBER_INT, "1", CTX));
        assertEquals("9007199254740993",
                strict().toColumnValue(String.class, JsonEvent.VALUE_NUMBER_INT, "9007199254740993", CTX));
        assertEquals("1e5", strict().toColumnValue(String.class, JsonEvent.VALUE_NUMBER_FLOAT, "1e5", CTX));
    }

    // ── Write-side 2^53 guard (JsonlSchemaManager delegation) ────────────

    @Test
    void writeSideGuardRejectsBigIntegerIntoDoubleColumn() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> JsonTypeMapper
                .validateDoublePrecision(Double.class, new BigDecimal("9007199254740993"), "BALANCE", "record 1: "));
        assertContains(e.getMessage(), "record 1");
        assertContains(e.getMessage(), "'BALANCE'");
        assertContains(e.getMessage(), "2^53");

        assertDoesNotThrow(() -> JsonTypeMapper
                .validateDoublePrecision(Double.class, new BigDecimal("9007199254740991"), "BALANCE", "record 1: "));
        assertDoesNotThrow(() -> JsonTypeMapper
                .validateDoublePrecision(Double.class, new BigDecimal("100.5"), "BALANCE", "record 1: "));
        assertDoesNotThrow(() -> JsonTypeMapper
                .validateDoublePrecision(Integer.class, 123, "AGE", "record 1: "));
    }

    @Test
    void writerRejectsTwo53PlusOneIntoDoubleColumn() throws Exception {
        List<String> columns = List.of("V");
        Map<String, Class<?>> types = Map.of("V", Double.class);
        DieselIOException e = assertThrows(DieselIOException.class, () -> {
            try (StringWriter sw = new StringWriter();
                 BufferedWriter bw = new BufferedWriter(sw);
                 JsonlRowWriter writer = new JsonlRowWriter(bw, columns, types)) {
                writer.writeRow(Map.of("V", new BigDecimal("9007199254740993")));
            }
        });
        assertContains(e.getMessage(), "2^53");
    }

    @Test
    void writerAllowsTwo53PlusOneInLongColumnAndReaderKeepsItExact() throws Exception {
        Path file = tempDir.resolve("long.jsonl");
        List<String> columns = List.of("V");
        Map<String, Class<?>> types = Map.of("V", Long.class);

        try (BufferedWriter bw = Files.newBufferedWriter(file);
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns, types)) {
            writer.writeRow(Map.of("V", 9007199254740993L));
        }

        try (BufferedReader br = Files.newBufferedReader(file);
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, file.toString())) {
            assertEquals(9007199254740993L, reader.readAll().get(0).get("V"));
        }
    }

    // ── Config resolution (jsonl.type.coercion / jsonl.duplicate.keys) ──

    @Test
    void defaultsAreStrictAndFail() {
        JsonParserConfig defaults = JsonParserConfig.defaults();
        assertEquals(JsonParserConfig.CoercionMode.STRICT, defaults.typeCoercion());
        assertEquals(JsonParserConfig.DuplicateKeyMode.FAIL, defaults.duplicateKeys());
    }

    @Test
    void systemPropertyOverridesCoercionAndDuplicateKeys() {
        setProperty("jsonl.type.coercion", "LENIENT");
        setProperty("jsonl.duplicate.keys", "LAST_WINS");
        JsonParserConfig overridden = JsonParserConfig.builder().build();
        assertEquals(JsonParserConfig.CoercionMode.LENIENT, overridden.typeCoercion());
        assertEquals(JsonParserConfig.DuplicateKeyMode.LAST_WINS, overridden.duplicateKeys());
    }

    @Test
    void invalidSystemPropertyFallsBackToDefaults() {
        setProperty("jsonl.type.coercion", "BOGUS");
        setProperty("jsonl.duplicate.keys", "SOMETIMES");
        JsonParserConfig defaults = JsonParserConfig.builder().build();
        assertEquals(JsonParserConfig.CoercionMode.STRICT, defaults.typeCoercion());
        assertEquals(JsonParserConfig.DuplicateKeyMode.FAIL, defaults.duplicateKeys());
    }

    // ── Reader integration ───────────────────────────────────────────────

    @Test
    void unicodeEscapeInStringColumnDecodes() throws Exception {
        List<String> columns = List.of("V");
        Map<String, Class<?>> types = Map.of("V", String.class);
        try (BufferedReader br = new BufferedReader(new StringReader("{\"V\":\"\\u0041BC\\u0410\"}\n"));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, FILE_HINT)) {
            assertEquals("ABC\u0410", reader.readAll().get(0).get("V"));
        }
    }

    @Test
    void nestedNullInStringColumnRoundTrips() throws Exception {
        List<String> columns = List.of("V");
        Map<String, Class<?>> types = Map.of("V", String.class);
        try (BufferedReader br = new BufferedReader(new StringReader("{\"V\":{\"a\":null,\"b\":[1,null,2]}}\n"));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, FILE_HINT)) {
            String captured = (String) reader.readAll().get(0).get("V");
            assertNotNull(captured);
            assertTrue(captured.contains("\"a\":null"), "nested null must be preserved, got: " + captured);
            assertTrue(captured.contains("null"), "null inside array must be preserved, got: " + captured);
        }
    }

    @Test
    void strictReaderRejectsQuotedNumberIntoNumericColumn() throws Exception {
        List<String> columns = List.of("V");
        Map<String, Class<?>> types = Map.of("V", Long.class);
        try (BufferedReader br = new BufferedReader(new StringReader("{\"V\":\"1\"}\n"));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, FILE_HINT)) {
            DieselIOException e = assertThrows(DieselIOException.class, reader::readAll);
            assertContains(e.getMessage(), FILE_HINT);
            assertContains(e.getMessage(), "line 1");
            assertContains(e.getMessage(), "strict mode");
        }
    }

    @Test
    void lenientReaderCoercesQuotedScalars() throws Exception {
        List<String> columns = List.of("V", "W");
        Map<String, Class<?>> types = Map.of("V", Long.class, "W", Double.class);
        JsonParserConfig config = JsonParserConfig.builder()
                .typeCoercion(JsonParserConfig.CoercionMode.LENIENT).build();
        try (BufferedReader br = new BufferedReader(
                new StringReader("{\"V\":\"9007199254740993\",\"W\":\"1e5\"}\n"));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, FILE_HINT, config)) {
            Map<String, Object> loaded = reader.readAll().get(0);
            assertEquals(9007199254740993L, loaded.get("V"));
            assertEquals(1.0E5, loaded.get("W"));
        }
    }

    private void assertContains(String haystack, String needle) {
        assertNotNull(haystack);
        assertTrue(haystack.contains(needle), "expected message <" + haystack + "> to contain <" + needle + ">");
    }
}