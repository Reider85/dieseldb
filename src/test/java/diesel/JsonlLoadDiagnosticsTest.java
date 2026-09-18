package diesel;

import ch.qos.logback.classic.Level;
import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 48: JSONL load-error diagnostics and garbage tolerance -
 * {@code file:line:field} (dot-notation JSON path) on every failure, the
 * {@code jsonl.load.error.mode = fail | skip_row} policy, blank-line / BOM /
 * non-object / truncated-last-line tolerance, and the final WARNING with the
 * skipped-row count.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlLoadDiagnosticsTest {

    @TempDir
    Path tempDir;

    private static final String ID_NAME_TYPES_HINT = "ID (Long), NAME (String)";

    private static List<String> idNameCols() {
        return List.of("ID", "NAME");
    }

    private static Map<String, Class<?>> idNameTypes() {
        return Map.of("ID", Long.class, "NAME", String.class);
    }

    private List<Map<String, Object>> read(String name, List<String> columns, Map<String, Class<?>> types,
                                          JsonParserConfig config) throws Exception {
        File file = tempDir.resolve(name).toFile();
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, file.getPath(), config)) {
            return reader.readAll();
        }
    }

    // ── file:line:field diagnostics (incl. dot-notation path) ────────

    @Test
    void diagnosticsCarryFileLineFieldCoordinates() throws Exception {
        File file = tempDir.resolve("coord.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\", \"AGE\": 30}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\", \"AGE\": \"abc\"}\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br,
                     List.of("ID", "NAME", "AGE"),
                     Map.of("ID", Long.class, "NAME", String.class, "AGE", Integer.class),
                     file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }

        assertTrue(e.getMessage().contains("coord.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertTrue(e.getMessage().contains("field 'AGE'"), e.getMessage());
        assertTrue(e.getMessage().contains("strict mode"), e.getMessage());
    }

    @Test
    void diagnosticsCarryDotNotationPathForNestedFields() throws Exception {
        File file = tempDir.resolve("dot.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"user\": {\"address\": {\"city\": 42}}}\n");
            fw.write("{\"ID\": 2, \"user\": {\"address\": {\"city\": \"abc\"}}}\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br,
                     List.of("ID", "user.address.city"),
                     Map.of("ID", Long.class, "user.address.city", Integer.class),
                     file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }

        assertTrue(e.getMessage().contains("dot.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertTrue(e.getMessage().contains("user.address.city"), e.getMessage());
    }

    // ── Tolerance: blank lines, BOM, non-object lines ────────────────

    @Test
    void emptyAndWhitespaceOnlyLinesAreSkipped() throws Exception {
        File file = tempDir.resolve("blank.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("\n");
            fw.write("   \t \n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        List<Map<String, Object>> loaded = read("blank.jsonl", idNameCols(), idNameTypes(),
                JsonParserConfig.defaults());
        assertEquals(2, loaded.size());
        assertEquals(2L, loaded.get(1).get("ID"));
    }

    @Test
    void bomIsStrippedOnFirstLine() throws Exception {
        File file = tempDir.resolve("bom.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("\uFEFF{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        List<Map<String, Object>> loaded = read("bom.jsonl", idNameCols(), idNameTypes(),
                JsonParserConfig.defaults());
        assertEquals(2, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals("Alice", loaded.get(0).get("NAME"));
    }

    @Test
    void arrayInsteadOfObjectFailsWithLineNumber() throws Exception {
        File file = tempDir.resolve("array.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("[1, 2, 3]\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }

        assertTrue(e.getMessage().contains("array.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertTrue(e.getMessage().contains("must be a single JSON object"), e.getMessage());
        assertTrue(e.getMessage().contains("START_ARRAY"), e.getMessage());
    }

    @Test
    void scalarInsteadOfObjectFailsWithLineNumber() throws Exception {
        File file = tempDir.resolve("scalar.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("42\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }

        assertTrue(e.getMessage().contains("scalar.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 1"), e.getMessage());
        assertTrue(e.getMessage().contains("must be a single JSON object"), e.getMessage());
    }

    @Test
    void brokenLineInTheMiddleFailsWithLineNumber() throws Exception {
        File file = tempDir.resolve("mid.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": }\n");
            fw.write("{\"ID\": 3, \"NAME\": \"Carol\"}\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }
        assertTrue(e.getMessage().contains("mid.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
    }

    // ── Truncated last line (interrupted append, prompt 49 gluing) ───

    @Test
    void truncatedLastLineIsDiagnosedAsPossibleTruncatedRecord() throws Exception {
        File file = tempDir.resolve("trunc.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }

        assertTrue(e.getMessage().contains("trunc.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("truncat"), e.getMessage());
    }

    // ── skip_row policy ──────────────────────────────────────────────

    @Test
    void skipRowDropsBrokenLineAndKeepsValidRows() throws Exception {
        File file = tempDir.resolve("skip.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": }\n");
            fw.write("{\"ID\": 3, \"NAME\": \"Carol\"}\n");
        }

        JsonParserConfig config = JsonParserConfig.builder()
                .loadErrorMode(JsonParserConfig.LoadErrorMode.SKIP_ROW)
                .build();

        List<Map<String, Object>> loaded;
        long skipped;
        try (Slf4jLogCapture capture = new Slf4jLogCapture(JsonlRowReader.class)) {
            try (BufferedReader br = new BufferedReader(new FileReader(file));
                 JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath(), config)) {
                loaded = reader.readAll();
                skipped = reader.getSkippedRowCount();
            }
            List<ch.qos.logback.classic.spi.ILoggingEvent> rowWarnings =
                    capture.eventsMatching(Level.WARN, "malformed JSONL row");
            assertEquals(1, rowWarnings.size(), "one per-row WARNING with coordinates + reason");
            assertTrue(rowWarnings.get(0).getFormattedMessage().contains("skip.jsonl"));
            assertTrue(rowWarnings.get(0).getFormattedMessage().contains("line 2"));
            assertEquals(1, capture.eventsMatching(Level.WARN, "skipped 1 malformed line(s)").size(),
                    "single final WARNING with the skipped count");
        }

        assertEquals(2, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals("Carol", loaded.get(1).get("NAME"));
        assertEquals(1, skipped);
    }

    @Test
    void skipRowSkipsNonObjectLine() throws Exception {
        File file = tempDir.resolve("skiparr.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("[5, 6, 7]\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        JsonParserConfig config = JsonParserConfig.builder()
                .loadErrorMode(JsonParserConfig.LoadErrorMode.SKIP_ROW)
                .build();
        List<Map<String, Object>> loaded;
        long skipped;
        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath(), config)) {
            loaded = reader.readAll();
            skipped = reader.getSkippedRowCount();
        }

        assertEquals(2, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals(2L, loaded.get(1).get("ID"));
        assertEquals(1, skipped);
    }

    @Test
    void skipRowSkipsTruncatedLastLine() throws Exception {
        File file = tempDir.resolve("skiptrunc.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob");
        }

        JsonParserConfig config = JsonParserConfig.builder()
                .loadErrorMode(JsonParserConfig.LoadErrorMode.SKIP_ROW)
                .build();
        List<Map<String, Object>> loaded;
        long skipped;
        try (Slf4jLogCapture capture = new Slf4jLogCapture(JsonlRowReader.class)) {
            try (BufferedReader br = new BufferedReader(new FileReader(file));
                 JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath(), config)) {
                loaded = reader.readAll();
                skipped = reader.getSkippedRowCount();
            }
            assertEquals(1, capture.eventsMatching(Level.WARN, "malformed JSONL row").size());
            List<ch.qos.logback.classic.spi.ILoggingEvent> trunc =
                    capture.eventsMatching(Level.WARN, "truncat");
            assertTrue(!trunc.isEmpty(), "skipped truncated row still diagnosed");
        }

        assertEquals(1, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals(1, skipped);
    }

    @Test
    void failModeDoesNotSkipAndReportsNoSkippedRows() throws Exception {
        File file = tempDir.resolve("fail.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("[1, 2, 3]\n");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file));
             JsonlRowReader reader = new JsonlRowReader(br, idNameCols(), idNameTypes(), file.getPath())) {
            assertThrows(DieselIOException.class, reader::readAll);
            assertEquals(0, reader.getSkippedRowCount(), "fail mode never skips");
        }
    }

    // ── Storage end-to-end ───────────────────────────────────────────

    @Test
    void skipRowStorageLoadCommitsValidRows() throws Exception {
        File file = tempDir.resolve("STORAGE_SKIP.jsonl").toFile();
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": }\n");
            fw.write("{\"ID\": 3, \"NAME\": \"Carol\"}\n");
        }

        JsonParserConfig config = JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .loadErrorMode(JsonParserConfig.LoadErrorMode.SKIP_ROW)
                .build();

        JsonlRowStorage storage = new JsonlRowStorage("STORAGE_SKIP", idNameCols(), idNameTypes(), config);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("STORAGE_SKIP");

        assertEquals(2, storage.scan().size(), "skip_row must not roll back the partial load");
        assertEquals(1L, storage.scan().get(0).get("ID"));
        assertEquals("Carol", storage.scan().get(1).get("NAME"));
        storage.close();
    }

    @Test
    void failStorageLoadRollsBackWithDiagnostics() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("FAIL_STORE", idNameCols(), idNameTypes());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(Map.of("ID", 99L, "NAME", "KEEP"));
        storage.saveToFile("FAIL_STORE");

        File target = new File(tempDir.toString(), "FAIL_STORE.jsonl");
        try (FileWriter fw = new FileWriter(target)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("[9, 9, 9]\n");
        }

        DieselIOException e = assertThrows(DieselIOException.class, () -> storage.loadFromFile("FAIL_STORE"));
        assertTrue(e.getMessage().contains("FAIL_STORE.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertEquals(1, storage.scan().size(), "fail mode rolls back to the previous rows");
        assertEquals(99L, storage.scan().get(0).get("ID"));
        storage.close();
    }

    // ── Config resolution ────────────────────────────────────────────

    @Test
    void loadErrorModePropertyResolution() {
        String previous = System.getProperty("jsonl.load.error.mode");
        try {
            assertEquals(JsonParserConfig.LoadErrorMode.FAIL,
                    JsonParserConfig.defaults().loadErrorMode(), "config.properties default is fail");

            System.setProperty("jsonl.load.error.mode", "skip_row");
            assertEquals(JsonParserConfig.LoadErrorMode.SKIP_ROW,
                    JsonParserConfig.defaults().loadErrorMode(), "system property wins");

            System.setProperty("jsonl.load.error.mode", "bogus");
            assertEquals(JsonParserConfig.LoadErrorMode.FAIL,
                    JsonParserConfig.defaults().loadErrorMode(), "invalid override falls back to fail");
        } finally {
            if (previous == null) {
                System.clearProperty("jsonl.load.error.mode");
            } else {
                System.setProperty("jsonl.load.error.mode", previous);
            }
        }
    }
}