package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlRowWriter;
import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 41: JSONL storage - extended features. Type validation (read and
 * write), field projection with parsed/skipped counters, JSON Path
 * (dot-notation) extraction and the schema sidecar file
 * ({@code <name>.schema.json}).
 */
@Tag("storage")
class JsonlSchemaProjectionTest {

    @TempDir
    Path tempDir;

    private static final String FILE_HINT = "schema_test.jsonl";

    /** These tests exercise the classic single-JSON-column capture semantics. */
    private static final JsonParserConfig JSON_COLUMN =
            JsonParserConfig.builder().nestedMode(JsonParserConfig.NestedMode.JSON_COLUMN).build();

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "BIRTHDATE", "LAST_LOGIN", "SESSION_ID", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("BALANCE", BigDecimal.class);
        t.put("BIRTHDATE", LocalDate.class);
        t.put("LAST_LOGIN", LocalDateTime.class);
        t.put("SESSION_ID", UUID.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        List<String> c = cols();
        for (int i = 0; i < vals.length; i++) {
            r.put(c.get(i), vals[i]);
        }
        return r;
    }

    // ── Read-side type validation ────────────────────────────────────────

    @Test
    void readNestedObjectInTypedColumnFailsWithCoordinates() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"AGE\":{\"a\":1}}"));
        assertContains(e.getMessage(), FILE_HINT);
        assertContains(e.getMessage(), "line 1");
        assertContains(e.getMessage(), "'AGE'");
        assertContains(e.getMessage(), "object");
    }

    @Test
    void readNestedArrayInTypedColumnFailsWithCoordinates() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"SESSION_ID\":[1,2]}"));
        assertContains(e.getMessage(), FILE_HINT);
        assertContains(e.getMessage(), "line 1");
        assertContains(e.getMessage(), "'SESSION_ID'");
        assertContains(e.getMessage(), "array");
    }

    @Test
    void readNumbersInDateAndUuidAndBooleanColumnsFail() {
        DieselIOException date = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"BIRTHDATE\":2020}"));
        assertContains(date.getMessage(), "'BIRTHDATE'");
        assertContains(date.getMessage(), "number");

        DieselIOException uuid = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"SESSION_ID\":5}"));
        assertContains(uuid.getMessage(), "'SESSION_ID'");

        DieselIOException bool = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"ACTIVE\":1}"));
        assertContains(bool.getMessage(), "'ACTIVE'");
    }

    @Test
    void readBooleanInNumericColumnFails() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"AGE\":true}"));
        assertContains(e.getMessage(), "'AGE'");
        assertContains(e.getMessage(), "boolean");
    }

    @Test
    void readJsonNullInTypedColumnIsAllowed() throws Exception {
        Map<String, Object> loaded = readOne("{\"ID\":1,\"NAME\":\"A\",\"AGE\":null}");
        assertNull(loaded.get("AGE"), "JSON null is a valid value for any column");
    }

    @Test
    void readScalarStringCoercionStaysLenient() throws Exception {
        JsonParserConfig lenient = JsonParserConfig.builder()
                .typeCoercion(JsonParserConfig.CoercionMode.LENIENT)
                .build();
        Map<String, Object> loaded = readOne("{\"ID\":\"1\",\"NAME\":\"A\",\"AGE\":\"30\","
                + "\"BALANCE\":\"100.5\",\"BIRTHDATE\":\"2020-01-02\","
                + "\"LAST_LOGIN\":\"2020-01-02T10:00:00\","
                + "\"SESSION_ID\":\"123e4567-e89b-12d3-a456-426614174000\",\"ACTIVE\":\"true\"}", lenient);
        assertEquals(1L, loaded.get("ID"));
        assertEquals(30, loaded.get("AGE"));
        assertEquals(new BigDecimal("100.5"), loaded.get("BALANCE"));
        assertEquals(LocalDate.of(2020, 1, 2), loaded.get("BIRTHDATE"));
        assertEquals(LocalDateTime.of(2020, 1, 2, 10, 0), loaded.get("LAST_LOGIN"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), loaded.get("SESSION_ID"));
        assertEquals(true, loaded.get("ACTIVE"));
    }

    @Test
    void readUnknownFieldIsSkippedAndCounted() throws Exception {
        String content = "{}\n" + "{\"ID\":1,\"NAME\":\"A\",\"UNKNOWN\":{\"x\":1},\"AGE\":20}\n";
        List<String> columns = cols();
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types(), FILE_HINT)) {
            List<Map<String, Object>> loaded = reader.readAll();
            assertEquals(2, loaded.size());
            assertEquals(20, loaded.get(1).get("AGE"));
            assertEquals(3, reader.getParsedFieldCount());
            assertEquals(1, reader.getSkippedFieldCount(), "nested unknown field counts as skipped");
        }
    }

    // ── Write-side type validation ───────────────────────────────────────

    @Test
    void writeNestedObjectIntoTypedColumnFails() throws Exception {
        File file = tempDir.resolve("w1.jsonl").toFile();
        Map<String, Object> bad = row(1L, "A", new LinkedHashMap<>(Map.of("a", 1)));
        DieselIOException e = assertThrows(DieselIOException.class, () -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file));
                 JsonlRowWriter writer = new JsonlRowWriter(bw, cols(), types())) {
                writer.writeRow(bad);
            }
        });
        assertContains(e.getMessage(), "record 1");
        assertContains(e.getMessage(), "'AGE'");
    }

    @Test
    void writeFloatingPointIntoIntegerColumnFails() throws Exception {
        File file = tempDir.resolve("w2.jsonl").toFile();
        Map<String, Object> bad = row(1L, "A", 25.5);
        DieselIOException e = assertThrows(DieselIOException.class, () -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file));
                 JsonlRowWriter writer = new JsonlRowWriter(bw, cols(), types())) {
                writer.writeRow(bad);
            }
        });
        assertContains(e.getMessage(), "record 1");
        assertContains(e.getMessage(), "'AGE'");
        assertContains(e.getMessage(), "floating-point");
    }

    @Test
    void writeBigDecimalIntoLongColumnFailsWithRecordNumber() throws Exception {
        Map<String, Object> bad = row(new BigDecimal("5"), "A", 1);
        DieselIOException e = assertThrows(DieselIOException.class, () -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempDir.resolve("w3.jsonl").toFile()));
                 JsonlRowWriter writer = new JsonlRowWriter(bw, cols(), types())) {
                writer.writeRow(row(1L, "B", 1));
                writer.writeRow(bad);
            }
        });
        assertContains(e.getMessage(), "record 2");
        assertContains(e.getMessage(), "'ID'");
    }

    @Test
    void writeTypedMismatchAndBooleanMismatchFail() throws Exception {
        assertThrows(DieselIOException.class, () -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempDir.resolve("w4.jsonl").toFile()));
                 JsonlRowWriter writer = new JsonlRowWriter(bw, cols(), types())) {
                writer.writeRow(row(LocalDate.of(2020, 1, 1), "A", 1));
            }
        });
        assertThrows(DieselIOException.class, () -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempDir.resolve("w5.jsonl").toFile()));
                 JsonlRowWriter writer = new JsonlRowWriter(bw, cols(), types())) {
                writer.writeRow(row(1L, "A", 1, null, null, null, null, 42));
            }
        });
    }

    @Test
    void writeNestedObjectIntoStringColumnAllowed() throws Exception {
        File file = tempDir.resolve("w6.jsonl").toFile();
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> columnTypes = Map.of("ID", Long.class, "DATA", String.class);
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "Alice");
        user.put("age", 30);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("user", user);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("DATA", nested);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns, columnTypes)) {
            writer.writeRow(r);
        }
        String written = Files.readString(file.toPath());
        assertTrue(written.contains("\"DATA\":{\"user\":{\"name\":\"Alice\",\"age\":30}}")
                || written.contains("\"DATA\":{\"user\":{\"age\":30,\"name\":\"Alice\"}}"),
                "nested JSON must contain the correct key-value pairs regardless of map iteration order");

        Map<String, Object> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, columns, columnTypes, file.getPath())) {
            loaded = reader.readAll().get(0);
        }
        assertNotNull(loaded.get("DATA"), "nested structure must round-trip as a String column value");
        String data = (String) loaded.get("DATA");
        assertTrue(data.contains("\"name\":\"Alice\"") && data.contains("\"age\":30"));
    }

    // ── Projection ───────────────────────────────────────────────────────

    @Test
    void nextProjectedReturnsOnlyRequestedColumns() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":20}\n"
                + "{\"ID\":2,\"NAME\":\"B\",\"AGE\":30}\n";
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT, JSON_COLUMN)) {
            reader.setProjection(List.of("NAME", "ID"));
            assertEquals(List.of("NAME", "ID"), reader.getProjectionItems());

            Object[] row0 = reader.nextProjected();
            assertEquals(List.of("A", 1L), List.of(row0[0], row0[1]));
            Object[] row1 = reader.nextProjected();
            assertEquals("B", row1[0]);
            assertEquals(2L, row1[1]);
            assertThrows(java.util.NoSuchElementException.class, reader::nextProjected);

            assertEquals(4, reader.getParsedFieldCount());
            assertEquals(2, reader.getSkippedFieldCount(), "AGE is skipped in both rows");
        }
    }

    @Test
    void nextProjectedMapKeysAreProjectionItems() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":20}\n";
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT)) {
            reader.setProjection(List.of("AGE"));
            Map<String, Object> row = reader.nextProjectedMap();
            assertEquals(Map.of("AGE", 20), row);
        }
    }

    @Test
    void projectionDoesNotAffectFullReads() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":20}\n";
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT)) {
            reader.setProjection(List.of("AGE"));
            Map<String, Object> full = reader.next();
            assertEquals(20, full.get("AGE"));
            assertEquals("A", full.get("NAME"));
            assertEquals(1L, full.get("ID"));
        }
    }

    @Test
    void dotPathProjectionExtractsNestedLeaf() throws Exception {
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> columnTypes = Map.of("ID", Long.class, "DATA", String.class);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("user", new LinkedHashMap<>(Map.of(
                "address", new LinkedHashMap<>(Map.of("city", "Moscow", "zip", "101000")))));
        String content = "{\"ID\":1,\"DATA\":" + compactJson(nested) + ",\"EXTRA\":5}\n";

        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, columnTypes, FILE_HINT, JSON_COLUMN)) {
            reader.setProjection(List.of("DATA.user.address.city", "ID"));
            assertEquals(List.of("DATA.user.address.city", "ID"), reader.getProjectionItems());

            Object[] row0 = reader.nextProjected();
            assertEquals("Moscow", row0[0], "dot path must extract the nested scalar leaf");
            assertEquals(1L, row0[1]);

            assertEquals(2, reader.getParsedFieldCount());
            assertEquals(1, reader.getSkippedFieldCount(), "EXTRA field is skipped");
        }
    }

    @Test
    void dotPathProjectionReturnsNullForAbsentPath() throws Exception {
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> columnTypes = Map.of("ID", Long.class, "DATA", String.class);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("user", Map.of("name", "Alice"));
        String content = "{\"ID\":1,\"DATA\":" + compactJson(nested) + "}\n";

        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, columnTypes, FILE_HINT, JSON_COLUMN)) {
            reader.setProjection(List.of("DATA.user.address.city", "DATA.user.name", "DATA"));
            Object[] row0 = reader.nextProjected();
            assertEquals(List.of("DATA.user.address.city", "DATA.user.name", "DATA"), reader.getProjectionItems());
            assertNull(row0[0], "absent path extracts to null");
            assertEquals("Alice", row0[1]);
            assertEquals("{\"user\":{\"name\":\"Alice\"}}", row0[2], "plain column keeps full JSON text");
        }
    }

    @Test
    void unresolvedProjectionItemsAreDropped() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":20}\n";
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT)) {
            reader.setProjection(List.of("NOPE", "ID", "AGE"));
            assertEquals(List.of("ID", "AGE"), reader.getProjectionItems());
            Object[] row0 = reader.nextProjected();
            assertEquals(1L, row0[0]);
            assertEquals(20, row0[1]);
        }
    }

    @Test
    void clearingProjectionRestoresFullRows() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":20}\n";
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT)) {
            reader.setProjection(List.of("AGE"));
            reader.setProjection(null);
            Map<String, Object> full = reader.readAll().get(0);
            assertEquals(8, full.size(), "full-row map always exposes every schema column");
            assertEquals("A", full.get("NAME"));
            assertEquals(1L, full.get("ID"));
            assertEquals(20, full.get("AGE"));
            assertEquals(3, reader.getParsedFieldCount());
        }
    }

    @Test
    void projectionParsesOnlyRequestedFieldsFromWideRows() throws Exception {
        int columnCount = 40;
        int rows = 500;
        String content = wideRows(columnCount, rows);
        List<String> columns = wideColumns(columnCount);

        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, wideTypes(columnCount), FILE_HINT, JSON_COLUMN)) {
            reader.setProjection(List.of("C0", "C1", "C2"));
            for (int i = 0; i < rows; i++) {
                Object[] projected = reader.nextProjected();
                assertEquals(3, projected.length);
                assertEquals(i, projected[0]);
                assertEquals(wideValue(i), projected[1]);
                assertEquals(wideValue(i), projected[2]);
            }
            assertEquals(3L * rows, reader.getParsedFieldCount());
            assertEquals(37L * rows, reader.getSkippedFieldCount(),
                    "all non-projected columns (incl. nested C38) must be skipped at token level");
        }
    }

    @Test
    void projectionBenchmarkSkipsUnrequestedFields() throws Exception {
        int columnCount = 40;
        int rows = 20_000;
        String content = wideRows(columnCount, rows);
        List<String> columns = wideColumns(columnCount);

        long fullNanos;
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, wideTypes(columnCount), FILE_HINT, JSON_COLUMN)) {
            long start = System.nanoTime();
            long parsed = 0;
            Object[] row;
            while (reader.hasNext()) {
                row = reader.nextArray();
                parsed += reader.getParsedFieldCount() - parsed; // single reader: re-tally per row is wrong
            }
            fullNanos = System.nanoTime() - start;
            assertTrue(parsed >= 0);
        }

        long projectedNanos;
        long parsedTotal = 0;
        long skippedTotal = 0;
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns, wideTypes(columnCount), FILE_HINT, JSON_COLUMN)) {
            reader.setProjection(List.of("C0", "C1", "C2"));
            long start = System.nanoTime();
            Object[] row;
            while (reader.hasNext()) {
                row = reader.nextProjected();
                assertEquals(3, row.length);
            }
            projectedNanos = System.nanoTime() - start;
            parsedTotal = reader.getParsedFieldCount();
            skippedTotal = reader.getSkippedFieldCount();
        }

        double fullMs = fullNanos / 1_000_000.0;
        double projectedMs = projectedNanos / 1_000_000.0;
        System.out.printf("jsonl projection benchmark prompt41: full=%.1fms projected=%.1fms "
                + "(parsed=%d skipped=%d) speedup=%.1fx%n",
                fullMs, projectedMs, parsedTotal, skippedTotal, fullMs / Math.max(projectedMs, 0.001));

        assertEquals(3L * rows, parsedTotal);
        assertEquals(37L * rows, skippedTotal);
        assertTrue(projectedNanos < fullNanos,
                "projected read must be faster than full-read (capture of unrequested nested values is skipped)");
    }

    // ── Schema sidecar file ──────────────────────────────────────────────

    @Test
    void schemaSidecarWriteReadVerifyRoundTrip() throws Exception {
        JsonlSchemaManager manager = new JsonlSchemaManager(cols(), types());
        Path sidecar = tempDir.resolve("USERS.schema.json");
        manager.writeSchemaFile(sidecar);

        JsonlSchemaManager.SchemaDescriptor descriptor = manager.readSchemaFile(sidecar);
        assertNotNull(descriptor);
        assertEquals(JsonlSchemaManager.SCHEMA_FORMAT_VERSION, descriptor.formatVersion());
        assertEquals(8, descriptor.columns().size());
        assertEquals(new JsonlSchemaManager.SchemaColumn("ID", "Long"), descriptor.columns().get(0));
        assertEquals(new JsonlSchemaManager.SchemaColumn("ACTIVE", "Boolean"), descriptor.columns().get(7));

        assertTrue(manager.verifySchemaFile(descriptor).isEmpty(), "self-written sidecar must verify clean");
    }

    @Test
    void schemaSidecarMissingReadReturnsNull() {
        JsonlSchemaManager manager = new JsonlSchemaManager(cols(), types());
        assertNull(manager.readSchemaFile(tempDir.resolve("MISSING.schema.json")));
        List<String> problems = manager.verifySchemaFile(null);
        assertEquals(1, problems.size());
        assertContains(problems.get(0), "missing");
    }

    @Test
    void schemaSidecarDetectsTypeMismatch() throws Exception {
        JsonlSchemaManager manager = new JsonlSchemaManager(cols(), types());
        Path sidecar = tempDir.resolve("T.schema.json");
        manager.writeSchemaFile(sidecar);
        JsonlSchemaManager.SchemaDescriptor descriptor = manager.readSchemaFile(sidecar);

        List<JsonlSchemaManager.SchemaColumn> altered = new ArrayList<>(descriptor.columns());
        altered.set(2, new JsonlSchemaManager.SchemaColumn("AGE", "String"));
        JsonlSchemaManager.SchemaDescriptor bad = new JsonlSchemaManager.SchemaDescriptor(
                descriptor.formatVersion(), altered);

        List<String> problems = manager.verifySchemaFile(bad);
        assertEquals(1, problems.size());
        assertContains(problems.get(0), "'AGE'");
        assertContains(problems.get(0), "type");
    }

    @Test
    void schemaSidecarFormatVersionTooNewIsReported() throws Exception {
        JsonlSchemaManager manager = new JsonlSchemaManager(List.of("A"), null);
        JsonlSchemaManager.SchemaDescriptor future = new JsonlSchemaManager.SchemaDescriptor(
                JsonlSchemaManager.SCHEMA_FORMAT_VERSION + 1,
                new JsonlSchemaManager.SchemaDescriptor(1,
                        List.of(new JsonlSchemaManager.SchemaColumn("A", "String"))).columns());
        List<String> problems = manager.verifySchemaFile(future);
        assertTrue(problems.stream().anyMatch(p -> p.contains("format version")));
    }

    // ── Storage integration ──────────────────────────────────────────────

    @Test
    void storageSaveRejectsInvalidTypedRowAndKeepsPreviousFile() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("SVALID", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.saveToFile("SVALID");
        String original = Files.readString(tempDir.resolve("SVALID.jsonl"));

        Map<String, Object> bad = row(2L, "Eve", new LinkedHashMap<>(Map.of("a", 1)));
        storage.insert(bad);
        assertThrows(DieselIOException.class, () -> storage.saveToFile("SVALID"));

        assertEquals(original, Files.readString(tempDir.resolve("SVALID.jsonl")),
                "failed validation must leave the previous target untouched");
        assertFalse(Files.exists(tempDir.resolve("SVALID.jsonl.tmp")), "no stray temp file");
        storage.close();
    }

    @Test
    void storageLoadRejectsNestedObjectInTypedColumnAndRollsBack() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("SLOAD", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.saveToFile("SLOAD");

        File target = tempDir.resolve("SLOAD.jsonl").toFile();
        try (FileWriter fw = new FileWriter(target)) {
            fw.write("{\"ID\":2,\"NAME\":\"Eve\",\"AGE\":{\"a\":1}}\n");
        }

        DieselIOException e = assertThrows(DieselIOException.class, () -> storage.loadFromFile("SLOAD"));
        assertContains(e.getMessage(), "'AGE'");
        assertEquals(1, storage.scan().size(), "previous in-memory rows must be restored");
        assertEquals(1L, storage.scan().get(0).get("ID"));
        storage.close();
    }

    // ── Wide-file helpers for projection/benchmark ───────────────────────

    private static List<String> wideColumns(int count) {
        List<String> cols = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            cols.add("C" + i);
        }
        return cols;
    }

    private static Map<String, Class<?>> wideTypes(int count) {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            t.put("C" + i, String.class);
        }
        t.put("C0", Integer.class);
        return t;
    }

    private static String wideValue(int i) {
        return "value-alpha-beta-gamma-" + i;
    }

    private static String wideRows(int columnCount, int rows) {
        StringBuilder sb = new StringBuilder(rows * 200);
        for (int i = 0; i < rows; i++) {
            sb.append("{\"C0\":").append(i);
            for (int c = 1; c < columnCount; c++) {
                if (c == 38) {
                    sb.append(",\"C38\":{\"arr\":[1,2,3,4,5,6,7,8,9,10]}");
                } else {
                    sb.append(",\"C").append(c).append("\":\"").append(wideValue(i)).append('"');
                }
            }
            sb.append("}\n");
        }
        return sb.toString();
    }

    // ── Small helpers ─────────────────────────────────────────────────────

    private static Map<String, Object> readOne(String line) throws Exception {
        try (BufferedReader br = new BufferedReader(new StringReader(line + "\n"));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT)) {
            return reader.readAll().get(0);
        }
    }

    private static Map<String, Object> readOne(String line, JsonParserConfig config) throws Exception {
        try (BufferedReader br = new BufferedReader(new StringReader(line + "\n"));
             JsonlRowReader reader = new JsonlRowReader(br, cols(), types(), FILE_HINT, config)) {
            return reader.readAll().get(0);
        }
    }

    private static String compactJson(Map<String, Object> value) throws Exception {
        java.io.StringWriter sw = new java.io.StringWriter();
        try (BufferedWriter bw = new BufferedWriter(sw);
             JsonlRowWriter writer = new JsonlRowWriter(bw, List.of("__probe__"), null)) {
            writer.writeRow(Map.of("__probe__", value));
        }
        String line = sw.toString().trim();
        int inner = line.indexOf(":{") + 1;
        int last = line.lastIndexOf('}');
        return line.substring(inner, last);
    }

    private static void assertContains(String haystack, String needle) {
        assertNotNull(haystack);
        assertTrue(haystack.contains(needle), "expected message <" + haystack + "> to contain <" + needle + ">");
    }
}