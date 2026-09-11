package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.CsvRowWriter;
import diesel.storage.TsvRowReader;
import diesel.storage.TsvRowWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 26: distinguishing NULL from the empty string in CSV/TSV.
 * Mode is selected via {@code storage.null.representation = legacy | sentinel}.
 */
class NullSentinelTest {

    @TempDir
    Path tempDir;

    private String prevNullRepresentation;

    @BeforeEach
    void saveConfig() {
        prevNullRepresentation = System.getProperty("storage.null.representation");
    }

    @AfterEach
    void restoreConfig() {
        if (prevNullRepresentation == null) {
            System.clearProperty("storage.null.representation");
        } else {
            System.setProperty("storage.null.representation", prevNullRepresentation);
        }
    }

    // ── TSV sentinel mode ───────────────────────────────────────────

    @Test
    void tsvSentinelRoundTripDistinguishesNullAndEmpty() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "NAME", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("NAME", null);
        row.put("DATA", "");

        File tsvFile = tempDir.resolve("sentinel.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        String content = new String(Files.readAllBytes(tsvFile.toPath()))
                .replace("\r\n", "\n");
        assertTrue(content.contains("1\t\\N\t"),
                "null should be written as \\N, got: " + content.replace("\n", "\\n"));

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertNull(loaded.get(0).get("NAME"), "\\N must round-trip as null");
        assertEquals("", loaded.get(0).get("DATA"), "empty string must round-trip as empty string, not null");
    }

    @Test
    void tsvSentinelRoundTripsLiteralBackslashN() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("DATA", "\\N");

        File tsvFile = tempDir.resolve("literalN.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        String content = new String(Files.readAllBytes(tsvFile.toPath()))
                .replace("\r\n", "\n");
        assertTrue(content.contains("\t\\\\N\n"),
                "literal \\N should be escaped to \\\\N, got: " + content.replace("\n", "\\n"));

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals("\\N", loaded.get(0).get("DATA"), "literal \\N must not be confused with null");
    }

    @Test
    void tsvSentinelRoundTripsTabsAndBackslashes() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("DATA", " \t \\back");

        File tsvFile = tempDir.resolve("backslash.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(" \t \\back", loaded.get(0).get("DATA"));
    }

    @Test
    void tsvLegacyModeKeepsOldBehavior() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("NAME", null);

        File tsvFile = tempDir.resolve("legacy.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        String content = new String(Files.readAllBytes(tsvFile.toPath()))
                .replace("\r\n", "\n");
        assertTrue(content.startsWith("ID\tNAME\n1\t\n"),
                "legacy null should be an empty field, got: " + content.replace("\n", "\\n"));

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertNull(loaded.get(0).get("NAME"));
    }

    // ── CSV sentinel mode ───────────────────────────────────────────

    @Test
    void csvSentinelRoundTripDistinguishesNullAndEmpty() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "NAME", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("NAME", null);
        row.put("DATA", "");

        File csvFile = tempDir.resolve("sentinel.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
             CsvRowWriter writer = new CsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        String content = new String(Files.readAllBytes(csvFile.toPath()))
                .replace("\r\n", "\n");
        assertTrue(content.contains("1,,\"\""),
                "empty string should be quoted \"\", got: " + content.replace("\n", "\\n"));

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertNull(loaded.get(0).get("NAME"), "unquoted empty field must round-trip as null");
        assertEquals("", loaded.get(0).get("DATA"), "quoted empty field must round-trip as empty string");
    }

    @Test
    void csvSentinelRoundTripsQuotedValues() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("DATA", "a,b \"quoted\" \nnewline");

        File csvFile = tempDir.resolve("quoted.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
             CsvRowWriter writer = new CsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals("a,b \"quoted\" \nnewline", loaded.get(0).get("DATA"));
    }

    @Test
    void csvLegacyModeKeepsOldBehavior() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("NAME", null);

        File csvFile = tempDir.resolve("legacy.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
             CsvRowWriter writer = new CsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(row);
        }

        String content = new String(Files.readAllBytes(csvFile.toPath()))
                .replace("\r\n", "\n");
        assertTrue(content.startsWith("ID,NAME\n1,\n"),
                "legacy null should be an unquoted empty field, got: " + content.replace("\n", "\\n"));

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertNull(loaded.get(0).get("NAME"));
    }

    // ── Cross-mode reading (migration semantics) ────────────────────

    @Test
    void tsvLegacyReadsBackslashNAsLiteral() throws Exception {
        System.setProperty("storage.null.representation", "legacy");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File tsvFile = tempDir.resolve("legacyN.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile))) {
            bw.write("ID\tDATA\n1\t\\N\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals("\\N", loaded.get(0).get("DATA"), "legacy mode must read \\N as literal text");
    }

    @Test
    void sentinelTsvReadsLegacyEmptyFieldAsEmptyString() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File tsvFile = tempDir.resolve("legacyEmpty.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile))) {
            bw.write("ID\tDATA\n1\t\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals("", loaded.get(0).get("DATA"),
                "empty field in sentinel mode reads as empty string (legacy nulls need migration)");
    }

    @Test
    void sentinelCsvReadsLegacyNullAsNull() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        File csvFile = tempDir.resolve("legacyNull.csv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile))) {
            bw.write("ID,DATA\n1,\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CsvRowReader reader = new CsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertNull(loaded.get(0).get("DATA"),
                "legacy null (unquoted empty) must still read as null in sentinel mode");
    }
}