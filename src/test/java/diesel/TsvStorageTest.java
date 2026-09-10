package diesel;

import diesel.storage.StorageFactory;
import diesel.storage.TsvRowReader;
import diesel.storage.TsvRowStorage;
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
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TsvStorageTest {

    @TempDir
    Path tempDir;

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

    private Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        List<String> c = cols();
        for (int i = 0; i < vals.length; i++) {
            r.put(c.get(i), vals[i]);
        }
        return r;
    }

    // ── Writer / Reader escaping ────────────────────────────────────

    @Test
    void escapeAndUnescapeRoundTrip() {
        String[] inputs = {"plain", "has\ttab", "has\nnewline", "has\\backslash", "has\r\nboth"};
        for (String input : inputs) {
            String escaped = TsvRowWriter.escapeValue(input);
            String unescaped = TsvRowReader.unescape(escaped);
            assertEquals(input, unescaped, "Round-trip failed for: " + input);
        }
    }

    @Test
    void escapeNullReturnsEmpty() {
        assertEquals("", TsvRowWriter.escapeValue(null));
    }

    @Test
    void escapeBigDecimalUsesPlainString() {
        BigDecimal val = new BigDecimal("123456.789012");
        assertEquals("123456.789012", TsvRowWriter.escapeValue(val));
    }

    // ── Writer + Reader integration ─────────────────────────────────

    @Test
    void writerReaderRoundTrip() throws Exception {
        List<String> columns = cols();
        Map<String, Class<?>> types = types();
        Map<String, Object> r1 = row(1L, "Alice", 25, new BigDecimal("100.50"),
                LocalDate.of(1998, 5, 20), LocalDateTime.of(2023, 10, 15, 14, 30),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), true);
        Map<String, Object> r2 = row(2L, "Bob", 30, new BigDecimal("200.75"),
                LocalDate.of(1993, 8, 15), LocalDateTime.of(2023, 10, 16, 9, 0),
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), false);

        File tsvFile = tempDir.resolve("test.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            writer.writeRow(r1);
            writer.writeRow(r2);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertEquals("Alice", loaded.get(0).get("NAME"));
        assertEquals(25, loaded.get(0).get("AGE"));
        assertEquals(new BigDecimal("100.50"), loaded.get(0).get("BALANCE"));
        assertEquals(LocalDate.of(1998, 5, 20), loaded.get(0).get("BIRTHDATE"));
        assertEquals(LocalDateTime.of(2023, 10, 15, 14, 30), loaded.get(0).get("LAST_LOGIN"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), loaded.get(0).get("SESSION_ID"));
        assertEquals(true, loaded.get(0).get("ACTIVE"));

        assertEquals(2L, loaded.get(1).get("ID"));
        assertEquals("Bob", loaded.get(1).get("NAME"));
        assertEquals(30, loaded.get(1).get("AGE"));
        assertEquals(false, loaded.get(1).get("ACTIVE"));
    }

    @Test
    void nullValuesRoundTrip() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File tsvFile = tempDir.resolve("nulls.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", 1L);
            row.put("NAME", null);
            writer.writeRow(row);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
        assertNull(loaded.get(0).get("NAME"));
    }

    @Test
    void emptyTableRoundTrip() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File tsvFile = tempDir.resolve("empty.tsv").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tsvFile));
             TsvRowWriter writer = new TsvRowWriter(bw, columns)) {
            writer.writeHeader();
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile));
             TsvRowReader reader = new TsvRowReader(br, columns, types)) {
            reader.readHeader();
            loaded = reader.readAll();
        }

        assertTrue(loaded.isEmpty());
    }

    @Test
    void specialCharsRoundTrip() throws Exception {
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("DATA", "tab\there\nand\nnew\\lines\\\\back");

        File tsvFile = tempDir.resolve("special.tsv").toFile();
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

        assertEquals(1, loaded.size());
        assertEquals("tab\there\nand\nnew\\lines\\\\back", loaded.get(0).get("DATA"));
    }

    // ── TsvRowStorage unit tests ────────────────────────────────────

    @Test
    void storageInsertAndScan() {
        TsvRowStorage storage = new TsvRowStorage("TEST", cols(), types());
        storage.open();
        storage.insert(row(1L, "Alice", 25, new BigDecimal("100"), null, null, null, true));
        storage.insert(row(2L, "Bob", 30, new BigDecimal("200"), null, null, null, false));

        List<Map<String, Object>> scanned = storage.scan();
        assertEquals(2, scanned.size());
        assertEquals("Alice", scanned.get(0).get("NAME"));
        assertEquals("Bob", scanned.get(1).get("NAME"));
        storage.close();
    }

    @Test
    void storageUpdateAndDelete() {
        TsvRowStorage storage = new TsvRowStorage("TEST", cols(), types());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.insert(row(2L, "Bob", 30, null, null, null, null, false));

        storage.update(0, row(1L, "ALICE", 26, null, null, null, null, true));
        assertEquals("ALICE", storage.scan().get(0).get("NAME"));
        assertEquals(26, storage.scan().get(0).get("AGE"));

        storage.delete(1);
        assertEquals(1, storage.scan().size());
        assertEquals("ALICE", storage.scan().get(0).get("NAME"));
        storage.close();
    }

    @Test
    void storageSaveAndLoadTsv() {
        TsvRowStorage storage = new TsvRowStorage("TSV_TEST", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, new BigDecimal("100.50"),
                LocalDate.of(1998, 5, 20), LocalDateTime.of(2023, 10, 15, 14, 30),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), true));
        storage.insert(row(2L, "Bob", 30, new BigDecimal("200.75"),
                LocalDate.of(1993, 8, 15), LocalDateTime.of(2023, 10, 16, 9, 0),
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), false));
        storage.saveToFile("TSV_TEST");

        assertTrue(new File(tempDir.toString(), "TSV_TEST.tsv").exists());
        assertTrue(new File(tempDir.toString(), "TSV_TEST.table").exists());

        TsvRowStorage loaded = new TsvRowStorage("TSV_TEST", cols(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("TSV_TEST");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("Alice", rows.get(0).get("NAME"));
        assertEquals(new BigDecimal("100.50"), rows.get(0).get("BALANCE"));
        assertEquals(LocalDate.of(1998, 5, 20), rows.get(0).get("BIRTHDATE"));
        assertEquals(LocalDateTime.of(2023, 10, 15, 14, 30), rows.get(0).get("LAST_LOGIN"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), rows.get(0).get("SESSION_ID"));
        assertEquals(true, rows.get(0).get("ACTIVE"));
        assertEquals(2L, rows.get(1).get("ID"));
        assertEquals("Bob", rows.get(1).get("NAME"));
        storage.close();
        loaded.close();
    }

    @Test
    void storageLoadMissingFileIsNoop() {
        TsvRowStorage storage = new TsvRowStorage("NONEXISTENT", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.loadFromFile("NONEXISTENT");
        assertEquals(1, storage.scan().size());
        storage.close();
    }

    @Test
    void storageSaveLoadSpecialChars() {
        TsvRowStorage storage = new TsvRowStorage("SPECIAL", List.of("ID", "DATA"),
                Map.of("ID", Long.class, "DATA", String.class));
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r1 = new LinkedHashMap<>();
        r1.put("ID", 1L);
        r1.put("DATA", "has\ttab");
        Map<String, Object> r2 = new LinkedHashMap<>();
        r2.put("ID", 2L);
        r2.put("DATA", "has\nnewline");
        Map<String, Object> r3 = new LinkedHashMap<>();
        r3.put("ID", 3L);
        r3.put("DATA", "has\\backslash");
        storage.insert(r1);
        storage.insert(r2);
        storage.insert(r3);
        storage.saveToFile("SPECIAL");

        TsvRowStorage loaded = new TsvRowStorage("SPECIAL", List.of("ID", "DATA"),
                Map.of("ID", Long.class, "DATA", String.class));
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("SPECIAL");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(3, rows.size());
        assertEquals("has\ttab", rows.get(0).get("DATA"));
        assertEquals("has\nnewline", rows.get(1).get("DATA"));
        assertEquals("has\\backslash", rows.get(2).get("DATA"));
        storage.close();
        loaded.close();
    }

    // ── StorageFactory integration ──────────────────────────────────

    @Test
    void storageFactoryCreatesTsv() {
        var storage = StorageFactory.create("tsv", "FACT_TEST", cols(), types());
        assertInstanceOf(TsvRowStorage.class, storage);
    }

    @Test
    void storageFactoryDefaultIsInMemory() {
        var storage = StorageFactory.create(null, "FACT_TEST", cols(), types());
        assertInstanceOf(diesel.storage.InMemoryRowStorage.class, storage);
    }

    // ── Database integration with TSV ───────────────────────────────

    @Test
    void databaseWithTsvStorage() {
        String prev = System.getProperty("diesel.storage.type");
        try {
            System.setProperty("diesel.storage.type", "tsv");
            Database db = new Database(tempDir.toString());
            db.executeQuery("CREATE TABLE DBTSV (ID LONG, NAME STRING, SCORE INTEGER)", null);
            db.executeQuery("INSERT INTO DBTSV (ID, NAME, SCORE) VALUES (1, 'Alice', 100)", null);
            db.executeQuery("INSERT INTO DBTSV (ID, NAME, SCORE) VALUES (2, 'Bob', 200)", null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> result =
                    (List<Map<String, Object>>) db.executeQuery("SELECT * FROM DBTSV ORDER BY ID", null);
            assertEquals(2, result.size());
            assertEquals("Alice", result.get(0).get("NAME"));
            assertEquals("Bob", result.get(1).get("NAME"));

            Table table = db.getTable("DBTSV");
            assertInstanceOf(TsvRowStorage.class, table.getStorage());

            File tsvFile = new File(tempDir.toString(), "DBTSV.tsv");
            assertTrue(tsvFile.exists(), "TSV file should be created on disk");
        } finally {
            if (prev == null) {
                System.clearProperty("diesel.storage.type");
            } else {
                System.setProperty("diesel.storage.type", prev);
            }
        }
    }

    @Test
    void databaseCreatesTsvStorageForPrimaryKeyTable() {
        String prev = System.getProperty("diesel.storage.type");
        try {
            System.setProperty("diesel.storage.type", "tsv");
            Database db = new Database(tempDir.toString());
            db.executeQuery("CREATE TABLE DBTSVPK (ID LONG PRIMARY KEY, NAME STRING)", null);
            db.executeQuery("INSERT INTO DBTSVPK (ID, NAME) VALUES (1, 'Alice')", null);
            Table table = db.getTable("DBTSVPK");
            assertInstanceOf(TsvRowStorage.class, table.getStorage());
        } finally {
            if (prev == null) {
                System.clearProperty("diesel.storage.type");
            } else {
                System.setProperty("diesel.storage.type", prev);
            }
        }
    }
}
