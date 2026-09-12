package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowReader;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 31: reader correctness fixes - strict boolean parsing, extra-field
 * warning, and defensive insert copies.
 */
class ReaderCorrectnessTest {

    @TempDir
    Path tempDir;

    private String prevLoadErrorMode;
    private String prevNullRepresentation;

    @BeforeEach
    void saveConfig() {
        prevLoadErrorMode = System.getProperty("storage.load.error.mode");
        prevNullRepresentation = System.getProperty("storage.null.representation");
        System.setProperty("storage.null.representation", "legacy");
    }

    @AfterEach
    void restoreConfig() {
        restoreProperty("storage.load.error.mode", prevLoadErrorMode);
        restoreProperty("storage.null.representation", prevNullRepresentation);
    }

    private void restoreProperty(String key, String prev) {
        if (prev == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, prev);
        }
    }

    private List<Map<String, Object>> readAllCsv(File csv, List<String> cols, Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(csv));
             CsvRowReader reader = new CsvRowReader(br, cols, types, csv.getPath())) {
            reader.readHeader();
            return reader.readAll();
        }
    }

    private List<Map<String, Object>> readAllTsv(File tsv, List<String> cols, Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(tsv));
             TsvRowReader reader = new TsvRowReader(br, cols, types, tsv.getPath())) {
            reader.readHeader();
            return reader.readAll();
        }
    }

    // ── Strict Boolean: accepted synonyms ───────────────────────────

    @Test
    void csvStrictBooleanParsesSynonyms() throws Exception {
        List<String> cols = List.of("BOOL");
        Map<String, Class<?>> types = Map.of("BOOL", Boolean.class);

        File csv = tempDir.resolve("bool.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("BOOL\n");
            bw.write("true\nFALSE\n1\n0\nYes\nNO\nT\nf\n  true  \n");
        }

        List<Map<String, Object>> rows = readAllCsv(csv, cols, types);
        assertEquals(9, rows.size());
        assertEquals(Boolean.TRUE, rows.get(0).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(1).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(2).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(3).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(4).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(5).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(6).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(7).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(8).get("BOOL"), "whitespace-trimmed value should parse");
    }

    @Test
    void tsvStrictBooleanParsesSynonyms() throws Exception {
        List<String> cols = List.of("BOOL");
        Map<String, Class<?>> types = Map.of("BOOL", Boolean.class);

        File tsv = tempDir.resolve("bool.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("BOOL\n");
            bw.write("true\nFALSE\n1\n0\nYes\nNO\nT\nf\n");
        }

        List<Map<String, Object>> rows = readAllTsv(tsv, cols, types);
        assertEquals(8, rows.size());
        assertEquals(Boolean.TRUE, rows.get(0).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(1).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(2).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(3).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(4).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(5).get("BOOL"));
        assertEquals(Boolean.TRUE, rows.get(6).get("BOOL"));
        assertEquals(Boolean.FALSE, rows.get(7).get("BOOL"));
    }

    // ── Strict Boolean: invalid values per load-error mode ──────────

    @Test
    void csvStrictBooleanInvalidFailsWithDiagnostics() throws Exception {
        List<String> cols = List.of("ID", "BOOL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "BOOL", Boolean.class);

        File csv = tempDir.resolve("bool_bad.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,BOOL\n1,true\n2,maybe\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> readAllCsv(csv, cols, types));

        String msg = ex.getMessage();
        assertTrue(msg.contains(csv.getPath()), "should contain file name: " + msg);
        assertTrue(msg.contains("line 3"), "should contain line 3: " + msg);
        assertTrue(msg.contains("BOOL"), "should contain column name: " + msg);
        assertTrue(msg.contains("maybe"), "should contain the bad value: " + msg);
        assertTrue(msg.contains("Boolean"), "should contain expected type: " + msg);
    }

    @Test
    void tsvStrictBooleanInvalidFailsWithDiagnostics() throws Exception {
        List<String> cols = List.of("ID", "BOOL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "BOOL", Boolean.class);

        File tsv = tempDir.resolve("bool_bad.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tBOOL\n1\ttrue\n2\tmaybe\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> readAllTsv(tsv, cols, types));

        String msg = ex.getMessage();
        assertTrue(msg.contains(tsv.getPath()), "should contain file name: " + msg);
        assertTrue(msg.contains("line 3"), "should contain line 3: " + msg);
        assertTrue(msg.contains("maybe"), "should contain the bad value: " + msg);
        assertTrue(msg.contains("Boolean"), "should contain expected type: " + msg);
    }

    @Test
    void csvStrictBooleanInvalidSkipValueKeepsRowWithNull() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_value");
        List<String> cols = List.of("ID", "BOOL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "BOOL", Boolean.class);

        File csv = tempDir.resolve("bool_skipval.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,BOOL\n1,true\n2,maybe\n3,false\n");
        }

        List<Map<String, Object>> rows = readAllCsv(csv, cols, types);
        assertEquals(3, rows.size(), "all rows kept");
        assertEquals(Boolean.TRUE, rows.get(0).get("BOOL"));
        assertNull(rows.get(1).get("BOOL"), "bad boolean becomes null");
        assertEquals(Boolean.FALSE, rows.get(2).get("BOOL"));
    }

    @Test
    void csvStrictBooleanInvalidSkipRowDropsRow() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_row");
        List<String> cols = List.of("ID", "BOOL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "BOOL", Boolean.class);

        File csv = tempDir.resolve("bool_skiprow.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,BOOL\n1,true\n2,maybe\n3,false\n");
        }

        List<Map<String, Object>> rows = readAllCsv(csv, cols, types);
        assertEquals(2, rows.size(), "bad row should be dropped");
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals(3L, rows.get(1).get("ID"));
    }

    @Test
    void tsvStrictBooleanInvalidSkipValueKeepsRowWithNull() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_value");
        List<String> cols = List.of("ID", "BOOL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "BOOL", Boolean.class);

        File tsv = tempDir.resolve("bool_skipval.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tBOOL\n1\ttrue\n2\tmaybe\n3\tfalse\n");
        }

        List<Map<String, Object>> rows = readAllTsv(tsv, cols, types);
        assertEquals(3, rows.size(), "all rows kept");
        assertEquals(Boolean.TRUE, rows.get(0).get("BOOL"));
        assertNull(rows.get(1).get("BOOL"), "bad boolean becomes null");
        assertEquals(Boolean.FALSE, rows.get(2).get("BOOL"));
    }

    // ── Extra fields warning (once per file) ────────────────────────

    @Test
    void csvExtraFieldsWarnOnceAndIgnoreExtra() throws Exception {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File csv = tempDir.resolve("extra.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,NAME\n1,Alice,extra1\n2,Bob\n3,Carla,extra3,extra4\n");
        }

        Logger logger = Logger.getLogger(CsvRowReader.class.getName());
        List<LogRecord> records = new java.util.ArrayList<>();
        Handler handler = captureHandler(records);
        logger.addHandler(handler);
        try {
            List<Map<String, Object>> rows = readAllCsv(csv, cols, types);
            assertEquals(3, rows.size(), "all rows parsed");
            assertEquals("Alice", rows.get(0).get("NAME"));
            assertEquals("Bob", rows.get(1).get("NAME"));
            assertEquals("Carla", rows.get(2).get("NAME"));

            List<LogRecord> warnings = records.stream()
                    .filter(r -> r.getLevel() == Level.WARNING && r.getMessage().contains("extra fields"))
                    .toList();
            assertEquals(1, warnings.size(), "exactly one WARNING per file, got: " + warnings);
            assertTrue(warnings.get(0).getMessage().contains("line 2"),
                    "warning should mention the first offending line: " + warnings.get(0).getMessage());
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void tsvExtraFieldsWarnOnceAndIgnoreExtra() throws Exception {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File tsv = tempDir.resolve("extra.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tNAME\n1\tAlice\textra1\n2\tBob\n3\tCarla\textra3\textra4\n");
        }

        Logger logger = Logger.getLogger(TsvRowReader.class.getName());
        List<LogRecord> records = new java.util.ArrayList<>();
        Handler handler = captureHandler(records);
        logger.addHandler(handler);
        try {
            List<Map<String, Object>> rows = readAllTsv(tsv, cols, types);
            assertEquals(3, rows.size(), "all rows parsed");
            assertEquals("Alice", rows.get(0).get("NAME"));
            assertEquals("Bob", rows.get(1).get("NAME"));
            assertEquals("Carla", rows.get(2).get("NAME"));

            List<LogRecord> warnings = records.stream()
                    .filter(r -> r.getLevel() == Level.WARNING && r.getMessage().contains("extra fields"))
                    .toList();
            assertEquals(1, warnings.size(), "exactly one WARNING per file, got: " + warnings);
            assertTrue(warnings.get(0).getMessage().contains("line 2"),
                    "warning should mention the first offending line: " + warnings.get(0).getMessage());
        } finally {
            logger.removeHandler(handler);
        }
    }

    // ── insert() stores a detached copy ─────────────────────────────

    @Test
    void csvInsertStoresCopyNotOriginalMap() {
        CsvRowStorage storage = new CsvRowStorage("T",
                List.of("ID", "NAME"), Map.of("ID", Long.class, "NAME", String.class));
        storage.open();
        Map<String, Object> input = new HashMap<>();
        input.put("ID", 1L);
        input.put("NAME", "Alice");
        storage.insert(input);

        input.put("NAME", "MUTATED");
        input.put("EXTRA", 42);

        assertEquals("Alice", storage.scan().get(0).get("NAME"), "stored row must be a detached copy");
        assertNull(storage.scan().get(0).get("EXTRA"));
        storage.close();
    }

    @Test
    void tsvInsertStoresCopyNotOriginalMap() {
        TsvRowStorage storage = new TsvRowStorage("T",
                List.of("ID", "NAME"), Map.of("ID", Long.class, "NAME", String.class));
        storage.open();
        Map<String, Object> input = new HashMap<>();
        input.put("ID", 1L);
        input.put("NAME", "Alice");
        storage.insert(input);

        input.put("NAME", "MUTATED");
        input.put("EXTRA", 42);

        assertEquals("Alice", storage.scan().get(0).get("NAME"), "stored row must be a detached copy");
        assertNull(storage.scan().get(0).get("EXTRA"));
        storage.close();
    }

    private Handler captureHandler(List<LogRecord> records) {
        return new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
    }
}