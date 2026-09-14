package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 44: JSONL schema inference and evolution - the
 * {@code jsonl.schema.mode} config (strict / inferred / hybrid), schema
 * expansion on new observed fields, sidecar {@code <name>.schema.json} with
 * the data-file mtime/size stamp, re-inference on data change, type-change
 * fail-fast with {@code file:line:field} coordinates and typo detection with
 * a nearest-column edit-distance hint.
 */
class JsonlSchemaModeTest {

    @TempDir
    Path tempDir;

    private static final String FILE_HINT = "schema_mode_test.jsonl";

    private static final List<String> COLS = List.of("ID", "NAME");
    private static final Map<String, Class<?>> TYPES = new LinkedHashMap<>();
    static {
        TYPES.put("ID", Long.class);
        TYPES.put("NAME", String.class);
    }

    private static JsonParserConfig config(JsonParserConfig.SchemaMode mode) {
        return new JsonParserConfig.Builder().schemaMode(mode)
                .nestedMode(JsonParserConfig.NestedMode.JSON_COLUMN).build();
    }

    // ── Config / manager plumbing ─────────────────────────────────────────

    @Test
    void defaultSchemaModeIsHybrid() {
        assertEquals(JsonParserConfig.SchemaMode.HYBRID, JsonParserConfig.defaults().schemaMode());
    }

    @Test
    void builderSchemaModeNullResetsToHybrid() {
        assertEquals(JsonParserConfig.SchemaMode.HYBRID,
                new JsonParserConfig.Builder().schemaMode(null).build().schemaMode());
        assertEquals(JsonParserConfig.SchemaMode.STRICT,
                new JsonParserConfig.Builder().schemaMode(JsonParserConfig.SchemaMode.STRICT).build().schemaMode());
    }

    @Test
    void schemaManagerSchemaModeReflectsConfig() {
        JsonlSchemaManager manager = new JsonlSchemaManager(COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        assertEquals(JsonParserConfig.SchemaMode.INFERRED, manager.schemaMode());
    }

    // ── Typo detection ────────────────────────────────────────────────────

    @Test
    void suggestNearestColumnReturnsCloseColumn() {
        JsonlSchemaManager manager = new JsonlSchemaManager(COLS, TYPES);
        assertEquals("AGE", new JsonlSchemaManager(List.of("ID", "NAME", "AGE"), TYPES)
                .suggestNearestColumn("AG"));
        assertEquals("NAME", new JsonlSchemaManager(COLS, TYPES).suggestNearestColumn("NAEM"));
    }

    @Test
    void suggestNearestColumnReturnsNullForDistantOrBlank() {
        JsonlSchemaManager manager = new JsonlSchemaManager(COLS, TYPES);
        assertNull(manager.suggestNearestColumn("XYZ"));
        assertNull(manager.suggestNearestColumn(""));
        assertNull(new JsonlSchemaManager(List.of(), null).suggestNearestColumn("A"));
    }

    // ── Strict mode (reader) ──────────────────────────────────────────────

    @Test
    void strictReaderUnknownFieldFailsWithTypoHint() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"NAEM\":5}",
                config(JsonParserConfig.SchemaMode.STRICT)));
        assertContains(e.getMessage(), FILE_HINT);
        assertContains(e.getMessage(), "line 1");
        assertContains(e.getMessage(), "'NAEM'");
        assertContains(e.getMessage(), "strict schema mode");
        assertContains(e.getMessage(), "'NAME'");
    }

    @Test
    void strictReaderFarUnknownFieldHasNoHint() {
        DieselIOException e = assertThrows(DieselIOException.class, () -> readOne(
                "{\"ID\":1,\"NAME\":\"A\",\"EXTRA\":5}",
                config(JsonParserConfig.SchemaMode.STRICT)));
        assertContains(e.getMessage(), "'EXTRA'");
        assertFalse(e.getMessage().contains("did you mean"),
                "no hint when no column is within the edit distance");
    }

    @Test
    void strictReaderMissingFieldYieldsNull() throws Exception {
        Map<String, Object> loaded = readOne("{\"ID\":1}",
                config(JsonParserConfig.SchemaMode.STRICT));
        assertEquals(1L, loaded.get("ID"));
        assertNull(loaded.get("NAME"), "missing field becomes null in every mode");
    }

    @Test
    void strictReaderIsCompatibleWithHybridWarnOnUnknown() throws Exception {
        Map<String, Object> loaded = readOne("{\"ID\":1,\"NAME\":\"A\",\"EXTRA\":5}",
                config(JsonParserConfig.SchemaMode.HYBRID));
        assertEquals(1L, loaded.get("ID"));
        assertEquals("A", loaded.get("NAME"));
    }

    // ── Strict mode (storage) ─────────────────────────────────────────────

    @Test
    void strictStorageLoadRejectsUnknownFieldAndRollsBack() throws Exception {
        JsonParserConfig strict = config(JsonParserConfig.SchemaMode.STRICT);
        JsonlRowStorage storage = new JsonlRowStorage("SSTRICT", COLS, TYPES, strict);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice"));
        storage.saveToFile("SSTRICT");

        overwrite("SSTRICT.jsonl", "{\"ID\":2,\"NAME\":\"Eve\",\"NAEM\":\"x\"}\n");

        DieselIOException e = assertThrows(DieselIOException.class, () -> storage.loadFromFile("SSTRICT"));
        assertContains(e.getMessage(), "'NAEM'");
        assertContains(e.getMessage(), "'NAME'");
        assertEquals(1, storage.scan().size(), "previous in-memory rows must be restored");
        assertEquals(1L, storage.scan().get(0).get("ID"));
        storage.close();
    }

    // ── Hybrid mode ───────────────────────────────────────────────────────

    private static final String HYBRID_CONTENT =
            "{\"ID\":1,\"NAME\":\"Alice\",\"EXTRA\":42}\n"
                    + "{\"ID\":2,\"NAME\":\"Bob\"}\n"
                    + "{\"ID\":3,\"NAME\":\"Cid\",\"EXTRA\":7}\n";

    private void writeHybridFile(String tableName) throws Exception {
        overwrite(tableName + ".jsonl", HYBRID_CONTENT);
    }

    @Test
    void hybridLoadExpandsSchemaAndTypesNewField() throws Exception {
        writeHybridFile("HYB");
        JsonlRowStorage storage = new JsonlRowStorage("HYB", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        storage.setDataDir(tempDir.toString());
        storage.open();

        storage.loadFromFile("HYB");

        assertEquals(List.of("ID", "NAME", "EXTRA"), storage.getColumns(),
                "new observed field is appended to the schema in first-seen order");
        assertEquals(Long.class, storage.getColumnTypes().get("EXTRA"));
        List<Map<String, Object>> rows = storage.scan();
        assertEquals(3, rows.size());
        assertEquals(42L, rows.get(0).get("EXTRA"));
        assertNull(rows.get(1).get("EXTRA"), "rows missing the new field get null");
        assertEquals(7L, rows.get(2).get("EXTRA"));
        storage.close();
    }

    @Test
    void hybridLoadReinfersOnDataChange() throws Exception {
        writeHybridFile("HYB2");
        JsonlRowStorage storage = new JsonlRowStorage("HYB2", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("HYB2");
        assertEquals(3, storage.getColumns().size());

        overwrite("HYB2.jsonl",
                "{\"ID\":1,\"NAME\":\"Alice\",\"EXTRA\":42,\"EXTRA2\":true}\n");
        storage.loadFromFile("HYB2");

        assertEquals(List.of("ID", "NAME", "EXTRA", "EXTRA2"), storage.getColumns(),
                "a changed data file is re-inferred and the new field expands the schema again");
        assertEquals(Boolean.class, storage.getColumnTypes().get("EXTRA2"));
        assertEquals(Boolean.TRUE, storage.scan().get(0).get("EXTRA2"));
        storage.close();
    }

    @Test
    void hybridRoundTripSaveReloadKeepsExpandedColumn() throws Exception {
        writeHybridFile("HYB3");
        JsonlRowStorage storage = new JsonlRowStorage("HYB3", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("HYB3");
        assertEquals(3, storage.getColumns().size());

        storage.saveToFile("HYB3");
        JsonlRowStorage reloaded = new JsonlRowStorage("HYB3", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        reloaded.setDataDir(tempDir.toString());
        reloaded.open();
        reloaded.loadFromFile("HYB3");
        assertEquals(List.of("ID", "NAME", "EXTRA"), reloaded.getColumns());
        assertEquals(3, reloaded.scan().size());
        assertEquals(42L, reloaded.scan().get(0).get("EXTRA"));
        reloaded.close();
        storage.close();
    }

    @Test
    void hybridNestedNewFieldStoredAsJsonText() throws Exception {
        overwrite("HYBNEST.jsonl",
                "{\"ID\":1,\"NAME\":\"A\",\"NEST\":{\"a\":1,\"b\":[true]}}\n");
        JsonlRowStorage storage = new JsonlRowStorage("HYBNEST", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("HYBNEST");

        assertEquals(String.class, storage.getColumnTypes().get("NEST"),
                "nested values are capturable only into a String column");
        String text = (String) storage.scan().get(0).get("NEST");
        assertNotNull(text);
        assertContains(text, "\"a\":1");
        storage.close();
    }

    @Test
    void hybridSaveWritesSchemaSidecarWithDataStamp() throws Exception {
        writeHybridFile("HYBCAR");
        JsonlRowStorage storage = new JsonlRowStorage("HYBCAR", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.HYBRID));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("HYBCAR");
        storage.saveToFile("HYBCAR");

        Path sidecar = tempDir.resolve("HYBCAR.schema.json");
        assertTrue(Files.exists(sidecar), "hybrid/inferred modes persist the schema sidecar");
        JsonlSchemaManager manager = new JsonlSchemaManager(COLS, TYPES);
        JsonlSchemaManager.SchemaDescriptor descriptor = manager.readSchemaFile(sidecar);
        assertNotNull(descriptor);
        assertNotNull(descriptor.data(), "sidecar records the data-file stamp");
        assertTrue(JsonlSchemaManager.SchemaStamp.isFresh(descriptor.data(), tempDir.resolve("HYBCAR.jsonl")),
                "stamp matches the data file that was just written");
        storage.close();
    }

    // ── Inferred mode ─────────────────────────────────────────────────────

    @Test
    void inferredAdoptsSchemaAndWritesSidecar() throws Exception {
        overwrite("INF.jsonl", "{\"A\":10}\n{\"A\":20,\"B\":\"x\"}\n");
        JsonlRowStorage storage = new JsonlRowStorage("INF", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("INF");

        assertEquals(List.of("A", "B"), storage.getColumns(),
                "inferred mode replaces the schema with the data-derived one");
        assertEquals(Long.class, storage.getColumnTypes().get("A"));
        assertEquals(String.class, storage.getColumnTypes().get("B"));
        assertEquals(20L, storage.scan().get(1).get("A"));
        assertTrue(Files.exists(tempDir.resolve("INF.schema.json")), "inferred mode writes the sidecar");
        storage.close();
    }

    @Test
    void inferredReusesFreshSidecarWithoutReinference() throws Exception {
        overwrite("INFR.jsonl", "{\"A\":10}\n{\"A\":20,\"B\":\"x\"}\n");
        JsonlRowStorage first = new JsonlRowStorage("INFR", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        first.setDataDir(tempDir.toString());
        first.open();
        first.loadFromFile("INFR");
        first.saveToFile("INFR"); // refreshes the sidecar stamp to match the file
        first.close();

        JsonlRowStorage second = new JsonlRowStorage("INFR", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        second.setDataDir(tempDir.toString());
        second.open();
        second.loadFromFile("INFR");
        assertEquals(List.of("A", "B"), second.getColumns());
        assertEquals(2, second.scan().size());
        second.close();
    }

    @Test
    void inferredTypeChangeFailsWithCoordinates() throws Exception {
        overwrite("INFTC.jsonl", "{\"X\":1}\n{\"X\":\"s\"}\n");
        JsonlRowStorage storage = new JsonlRowStorage("INFTC", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(7L, "keep"));

        DieselIOException e = assertThrows(DieselIOException.class, () -> storage.loadFromFile("INFTC"));
        assertContains(e.getMessage(), "INFTC.jsonl");
        assertContains(e.getMessage(), "'X'");
        assertContains(e.getMessage(), "line 1");
        assertContains(e.getMessage(), "line 2");
        assertContains(e.getMessage(), "number");
        assertContains(e.getMessage(), "string");
        assertEquals(1, storage.scan().size(), "failed inference must leave rows untouched");
        storage.close();
    }

    @Test
    void inferredEmptyFileYieldsEmptySchema() throws Exception {
        overwrite("INFMT.jsonl", "");
        JsonlRowStorage storage = new JsonlRowStorage("INFMT", COLS, TYPES,
                config(JsonParserConfig.SchemaMode.INFERRED));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("INFMT");
        assertTrue(storage.getColumns().isEmpty(), "no data means nothing to infer");
        assertTrue(storage.scan().isEmpty());
        storage.close();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private void overwrite(String name, String content) throws Exception {
        try (FileWriter fw = new FileWriter(tempDir.resolve(name).toFile())) {
            fw.write(content);
        }
    }

    private static Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < vals.length; i++) {
            r.put(COLS.get(i), vals[i]);
        }
        return r;
    }

    private static Map<String, Object> readOne(String line, JsonParserConfig config) throws Exception {
        try (BufferedReader br = new BufferedReader(new StringReader(line + "\n"));
             JsonlRowReader reader = new JsonlRowReader(br, COLS, TYPES, FILE_HINT, config)) {
            return reader.readAll().get(0);
        }
    }

    private static void assertContains(String haystack, String needle) {
        assertNotNull(haystack);
        assertTrue(haystack.contains(needle), "expected message <" + haystack + "> to contain <" + needle + ">");
    }
}