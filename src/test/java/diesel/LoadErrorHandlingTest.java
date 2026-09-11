package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 27: load error handling with file:line:column diagnostics.
 * Mode is selected via {@code storage.load.error.mode = fail | skip_row | skip_value}.
 */
class LoadErrorHandlingTest {

    @TempDir
    Path tempDir;

    private String prevLoadErrorMode;
    private String prevNullRepresentation;

    @BeforeEach
    void saveConfig() {
        prevLoadErrorMode = System.getProperty("storage.load.error.mode");
        prevNullRepresentation = System.getProperty("storage.null.representation");
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

    // ── CSV fail mode ──────────────────────────────────────────────

    @Test
    void csvBrokenValueMidFileReportsLineAndColumn() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class, "NAME", String.class);

        File csv = tempDir.resolve("users.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,AGE,NAME\n1,30,Alice\n2,abc,Bob\n3,31,Carla\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> {
            try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
                 CsvRowReader reader = new CsvRowReader(br, cols, types, csv.getPath())) {
                reader.readHeader();
                while (reader.hasNext()) {
                    reader.next();
                }
            }
        });

        String msg = ex.getMessage();
        assertTrue(msg.contains(csv.getPath()), "should contain file name: " + msg);
        assertTrue(msg.contains("3"), "should contain line number 3: " + msg);
        assertTrue(msg.contains("AGE"), "should contain column name: " + msg);
        assertTrue(msg.contains("abc"), "should contain the bad value: " + msg);
        assertTrue(msg.contains("Integer"), "should contain expected type: " + msg);
    }

    @Test
    void csvBrokenValueFailsWithLineNumber() throws Exception {
        List<String> cols = List.of("ID", "AGE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class);

        File csv = tempDir.resolve("bad.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,AGE\n1,25\n2,xyz\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> {
            try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
                 CsvRowReader reader = new CsvRowReader(br, cols, types, "bad.csv")) {
                reader.readHeader();
                while (reader.hasNext()) {
                    reader.next();
                }
            }
        });

        assertTrue(ex.getMessage().contains("bad.csv:line 3"), "expected bad.csv:3 in: " + ex.getMessage());
    }

    // ── TSV fail mode ──────────────────────────────────────────────

    @Test
    void tsvBrokenValueMidFileReportsLineAndColumn() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class, "NAME", String.class);

        File tsv = tempDir.resolve("users.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tAGE\tNAME\n1\t30\tAlice\n2\tnumbers\tBob\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> {
            try (var br = new java.io.BufferedReader(new java.io.FileReader(tsv));
                 TsvRowReader reader = new TsvRowReader(br, cols, types, tsv.getPath())) {
                reader.readHeader();
                while (reader.hasNext()) {
                    reader.next();
                }
            }
        });

        String msg = ex.getMessage();
        assertTrue(msg.contains(tsv.getPath()), "should contain file name: " + msg);
        assertTrue(msg.contains("3"), "should contain line number 3: " + msg);
        assertTrue(msg.contains("AGE"), "should contain column name: " + msg);
        assertTrue(msg.contains("Integer"), "should contain expected type: " + msg);
    }

    // ── CSV truncated quoted field ─────────────────────────────────

    @Test
    void csvTruncatedQuotedFieldReportsError() throws Exception {
        List<String> cols = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File csv = tempDir.resolve("trunc.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,DATA\n1,\"Alice\n");
        }

        DieselIOException ex = assertThrows(DieselIOException.class, () -> {
            try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
                 CsvRowReader reader = new CsvRowReader(br, cols, types, "trunc.csv")) {
                reader.readHeader();
                while (reader.hasNext()) {
                    reader.next();
                }
            }
        });

        String msg = ex.getMessage();
        assertTrue(msg.contains("unterminated"), "expected 'unterminated' in: " + msg);
        assertTrue(msg.contains("2"), "should contain line number 2: " + msg);
    }

    // ── CSV skip_row mode ──────────────────────────────────────────

    @Test
    void csvSkipRowModeDropsBadRow() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_row");
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class);

        File csv = tempDir.resolve("skip.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,AGE\n1,25\n2,bad\n3,35\n");
        }

        List<Map<String, Object>> loaded;
        try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
             CsvRowReader reader = new CsvRowReader(br, cols, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size(), "bad row should be dropped");
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals(25, loaded.get(0).get("AGE"));
        assertEquals(3L, loaded.get(1).get("ID"));
        assertEquals(35, loaded.get(1).get("AGE"));
    }

    // ── CSV skip_value mode ────────────────────────────────────────

    @Test
    void csvSkipValueModeKeepsRowWithNull() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_value");
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class);

        File csv = tempDir.resolve("skipval.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,AGE\n1,25\n2,bad\n3,35\n");
        }

        List<Map<String, Object>> loaded;
        try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
             CsvRowReader reader = new CsvRowReader(br, cols, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(3, loaded.size(), "all rows kept");
        assertEquals(25, loaded.get(0).get("AGE"));
        assertNull(loaded.get(1).get("AGE"), "bad value becomes null");
        assertEquals(35, loaded.get(2).get("AGE"));
    }

    // ── TSV skip_row mode ──────────────────────────────────────────

    @Test
    void tsvSkipRowModeDropsBadRow() throws Exception {
        System.setProperty("storage.load.error.mode", "skip_row");
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class);

        File tsv = tempDir.resolve("skip.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tAGE\n1\t25\n2\tbad\n3\t35\n");
        }

        List<Map<String, Object>> loaded;
        try (var br = new java.io.BufferedReader(new java.io.FileReader(tsv));
             TsvRowReader reader = new TsvRowReader(br, cols, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size(), "bad row should be dropped");
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals(3L, loaded.get(1).get("ID"));
    }

    // ── Storage transactional rollback ─────────────────────────────

    @Test
    void csvStorageLoadFailureRollsBack() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> cols = List.of("ID", "AGE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class);

        CsvRowStorage storage = new CsvRowStorage("ROLLBACK_TEST", cols, types);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(Map.of("ID", 1L, "AGE", 10));
        storage.saveToFile("ROLLBACK_TEST");

        File csv = tempDir.resolve("ROLLBACK_TEST.csv").toFile();
        java.nio.file.Files.writeString(csv.toPath(), "ID,AGE\n1,bad\n");

        assertThrows(DieselIOException.class, () -> storage.loadFromFile("ROLLBACK_TEST"));
        assertEquals(1, storage.scan().size(), "rows must be rolled back");
        assertEquals(10, storage.scan().get(0).get("AGE"), "original value preserved");
    }

    // ── getLineNumber() contract ───────────────────────────────────

    @Test
    void csvLineNumberTracksPhysicalLines() throws Exception {
        List<String> cols = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File csv = tempDir.resolve("lines.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))) {
            bw.write("ID,DATA\n1,hello\n2,world\n");
        }

        try (var br = new java.io.BufferedReader(new java.io.FileReader(csv));
             CsvRowReader reader = new CsvRowReader(br, cols, types)) {
            assertEquals(0, reader.getLineNumber(), "before readHeader");
            reader.readHeader();
            assertEquals(1, reader.getLineNumber(), "after header");
            reader.next();
            assertEquals(2, reader.getLineNumber(), "after first data row");
            reader.next();
            assertEquals(3, reader.getLineNumber(), "after second data row");
        }
    }

    @Test
    void tsvLineNumberTracksPhysicalLines() throws Exception {
        List<String> cols = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File tsv = tempDir.resolve("lines.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsv))) {
            bw.write("ID\tDATA\n1\thello\n2\tworld\n");
        }

        try (var br = new java.io.BufferedReader(new java.io.FileReader(tsv));
             TsvRowReader reader = new TsvRowReader(br, cols, types)) {
            assertEquals(0, reader.getLineNumber(), "before readHeader");
            reader.readHeader();
            assertEquals(1, reader.getLineNumber(), "after header");
            reader.next();
            assertEquals(2, reader.getLineNumber(), "after first data row");
            reader.next();
            assertEquals(3, reader.getLineNumber(), "after second data row");
        }
    }
}
