package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlRowWriter;
import diesel.storage.StorageFactory;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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

class JsonlStorageTest {

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

    private static Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        List<String> c = cols();
        for (int i = 0; i < vals.length; i++) {
            r.put(c.get(i), vals[i]);
        }
        return r;
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

        File jsonlFile = tempDir.resolve("test.jsonl").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            writer.writeRow(r1);
            writer.writeRow(r2);
        }
        assertEquals(2, Files.readAllLines(jsonlFile.toPath()).size(),
                "one JSON object per line");

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, jsonlFile.getPath())) {
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
        assertEquals(false, loaded.get(1).get("ACTIVE"));
    }

    @Test
    void nullAndEmptyStringRoundTrip() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("nulls.jsonl").toFile();
        Map<String, Object> r1 = new LinkedHashMap<>();
        r1.put("ID", 1L);
        r1.put("NAME", null);
        Map<String, Object> r2 = new LinkedHashMap<>();
        r2.put("ID", 2L);
        r2.put("NAME", "");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            writer.writeRow(r1);
            writer.writeRow(r2);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size());
        assertNull(loaded.get(0).get("NAME"));
        assertEquals("", loaded.get(1).get("NAME"));
    }

    @Test
    void emptyTableRoundTrip() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("empty.jsonl").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            writer.flush();
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
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
        row.put("DATA", "tab\there\nnewline\\backslash\"quote\u00E9\u4E2D\uD83D\uDE00");

        File jsonlFile = tempDir.resolve("special.jsonl").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            writer.writeRow(row);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        assertEquals("tab\there\nnewline\\backslash\"quote\u00E9\u4E2D\uD83D\uDE00", loaded.get(0).get("DATA"));
    }

    @Test
    void nestedStructureWrittenAsJsonAndReadBackAsJsonColumn() throws Exception {
        List<String> columns = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("city", "Moscow");
        nested.put("tags", List.of("a", "b"));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("DATA", nested);

        File jsonlFile = tempDir.resolve("nested.jsonl").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            writer.writeRow(row);
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
            loaded = reader.readAll();
        }

        assertEquals(1, loaded.size());
        String stored = (String) loaded.get(0).get("DATA");
        assertEquals("{\"city\":\"Moscow\",\"tags\":[\"a\",\"b\"]}", stored);

        String[] lines = Files.readAllLines(jsonlFile.toPath()).toArray(new String[0]);
        assertEquals("{\"ID\":1,\"DATA\":{\"city\":\"Moscow\",\"tags\":[\"a\",\"b\"]}}", lines[0]);
    }

    @Test
    void nonFiniteFloatRejectedAtWrite() throws Exception {
        List<String> columns = List.of("ID", "SCORE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "SCORE", Double.class);

        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("ID", 1L);
        bad.put("SCORE", Double.NaN);

        File jsonlFile = tempDir.resolve("bad.jsonl").toFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(jsonlFile));
             JsonlRowWriter writer = new JsonlRowWriter(bw, columns)) {
            assertThrows(DieselIOException.class, () -> writer.writeRow(bad));
        }
    }

    // ── Reader edge cases ───────────────────────────────────────────

    @Test
    void blankLinesSkipped() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("blank.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("\n");
            fw.write("   \n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, jsonlFile.getPath())) {
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size());
        assertEquals("Bob", loaded.get(1).get("NAME"));
    }

    @Test
    void bomStrippedOnLoad() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("bom.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("\uFEFF{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size());
        assertEquals(1L, loaded.get(0).get("ID"));
    }

    @Test
    void invalidJsonLineFailsWithFileLineContext() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("bad.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": }\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, jsonlFile.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }
        assertTrue(e.getMessage().contains("bad.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
    }

    @Test
    void nonObjectLineFails() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("array.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("[1, 2, 3]\n");
        }

        DieselIOException e;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, jsonlFile.getPath())) {
            e = assertThrows(DieselIOException.class, reader::readAll);
        }
        assertTrue(e.getMessage().contains("must be a single JSON object"), e.getMessage());
        assertTrue(e.getMessage().contains("line 1"), e.getMessage());
    }

    @Test
    void missingFieldBecomesNullAndUnknownFieldIgnored() throws Exception {
        List<String> columns = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);

        File jsonlFile = tempDir.resolve("partial.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("{\"ID\": 1, \"EXTRA\": \"x\"}\n");
            fw.write("{\"ID\": 2, \"NAME\": \"Bob\"}\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types)) {
            loaded = reader.readAll();
        }

        assertEquals(2, loaded.size());
        assertNull(loaded.get(0).get("NAME"), "missing field -> null");
        assertEquals("Bob", loaded.get(1).get("NAME"));
    }

    @Test
    void jsonBooleansAndNumbersConvertToColumns() throws Exception {
        List<String> columns = List.of("ID", "AGE", "ACTIVE");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "AGE", Integer.class, "ACTIVE", Boolean.class);
        JsonParserConfig lenient = JsonParserConfig.builder()
                .typeCoercion(JsonParserConfig.CoercionMode.LENIENT)
                .build();

        File jsonlFile = tempDir.resolve("conv.jsonl").toFile();
        try (FileWriter fw = new FileWriter(jsonlFile)) {
            fw.write("{\"ID\": 7, \"AGE\": 30, \"ACTIVE\": true}\n");
            fw.write("{\"ID\": \"8\", \"AGE\": \"40\", \"ACTIVE\": \"false\"}\n");
        }

        List<Map<String, Object>> loaded;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonlFile));
             JsonlRowReader reader = new JsonlRowReader(br, columns, types, jsonlFile.getPath(), lenient)) {
            loaded = reader.readAll();
        }

        assertEquals(7L, loaded.get(0).get("ID"));
        assertEquals(30, loaded.get(0).get("AGE"));
        assertEquals(true, loaded.get(0).get("ACTIVE"));
        assertEquals(8L, loaded.get(1).get("ID"));
        assertEquals(40, loaded.get(1).get("AGE"));
        assertEquals(false, loaded.get(1).get("ACTIVE"));
    }

    // ── JsonlRowStorage unit tests ──────────────────────────────────

    @Test
    void storageSaveAndLoad() {
        JsonlRowStorage storage = new JsonlRowStorage("JSONL_TEST", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, new BigDecimal("100.50"),
                LocalDate.of(1998, 5, 20), LocalDateTime.of(2023, 10, 15, 14, 30),
                UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), true));
        storage.insert(row(2L, "Bob", 30, new BigDecimal("200.75"),
                LocalDate.of(1993, 8, 15), LocalDateTime.of(2023, 10, 16, 9, 0),
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), false));
        storage.saveToFile("JSONL_TEST");

        assertTrue(new File(tempDir.toString(), "JSONL_TEST.jsonl").exists());

        JsonlRowStorage loaded = new JsonlRowStorage("JSONL_TEST", cols(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("JSONL_TEST");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("Alice", rows.get(0).get("NAME"));
        assertEquals(new BigDecimal("100.50"), rows.get(0).get("BALANCE"));
        assertEquals(true, rows.get(0).get("ACTIVE"));
        assertEquals(2L, rows.get(1).get("ID"));
        assertEquals("Bob", rows.get(1).get("NAME"));
        storage.close();
        loaded.close();
    }

    @Test
    void storageUpdateDeletePersisted() {
        JsonlRowStorage storage = new JsonlRowStorage("UD", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.insert(row(2L, "Bob", 30, null, null, null, null, false));
        storage.insert(row(3L, "Carol", 35, null, null, null, null, true));
        storage.update(1, row(2L, "ROBERT", 31, null, null, null, null, false));
        storage.delete(2);
        storage.saveToFile("UD");

        JsonlRowStorage loaded = new JsonlRowStorage("UD", cols(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("UD");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals("Alice", rows.get(0).get("NAME"));
        assertEquals("ROBERT", rows.get(1).get("NAME"));
        assertEquals(31, rows.get(1).get("AGE"));
        storage.close();
        loaded.close();
    }

    @Test
    void storageNestedColumnRoundTrip() {
        JsonlRowStorage storage = new JsonlRowStorage("NESTED", List.of("ID", "DATA"),
                Map.of("ID", Long.class, "DATA", String.class));
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "Alice");
        user.put("age", 30);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("user", user);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("DATA", nested);
        storage.insert(r);
        storage.saveToFile("NESTED");

        JsonlRowStorage loaded = new JsonlRowStorage("NESTED", List.of("ID", "DATA"),
                Map.of("ID", Long.class, "DATA", String.class));
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("NESTED");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("{\"user\":{\"name\":\"Alice\",\"age\":30}}", rows.get(0).get("DATA"));
        storage.close();
        loaded.close();
    }

    @Test
    void storageLoadMissingFileIsNoop() {
        JsonlRowStorage storage = new JsonlRowStorage("NONEXISTENT", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.loadFromFile("NONEXISTENT");
        assertEquals(1, storage.scan().size());
        storage.close();
    }

    @Test
    void storageInterruptedSaveKeepsPreviousFile() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("CRASH", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.saveToFile("CRASH");
        String original = Files.readString(new File(tempDir.toString(), "CRASH.jsonl").toPath());

        File target = new File(tempDir.toString(), "CRASH.jsonl");
        File tmp = new File(tempDir.toString(), "CRASH.jsonl.tmp");
        Files.writeString(tmp.toPath(), "partial garbage");

        JsonlRowStorage reloaded = new JsonlRowStorage("CRASH", cols(), types());
        reloaded.setDataDir(tempDir.toString());
        reloaded.open();
        reloaded.loadFromFile("CRASH");
        assertEquals(1, reloaded.scan().size());

        Files.delete(tmp.toPath());
        assertEquals(original, Files.readString(target.toPath()));
        storage.close();
        reloaded.close();
    }

    @Test
    void storageInvalidFileRollsBackPreviousRows() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("ROLLBACK", cols(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 25, null, null, null, null, true));
        storage.saveToFile("ROLLBACK");

        File target = new File(tempDir.toString(), "ROLLBACK.jsonl");
        try (FileWriter fw = new FileWriter(target)) {
            fw.write("{\"ID\": 1, \"NAME\": \"Alice\", \"AGE\": }\n");
        }

        assertThrows(DieselIOException.class, () -> storage.loadFromFile("ROLLBACK"));
        assertEquals(1, storage.scan().size(), "previous in-memory rows must be restored");
        assertEquals("Alice", storage.scan().get(0).get("NAME"));
        storage.close();
    }

    @Test
    void storageLargeStreamingRead() {
        int count = 100_000;
        JsonlRowStorage storage = new JsonlRowStorage("BIG", List.of("ID", "NAME"),
                Map.of("ID", Long.class, "NAME", String.class));
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.beginBulkUpdate();
        for (long i = 0; i < count; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", i);
            r.put("NAME", "name-" + i);
            storage.insert(r);
        }
        storage.endBulkUpdate();
        storage.saveToFile("BIG");

        JsonlRowStorage loaded = new JsonlRowStorage("BIG", List.of("ID", "NAME"),
                Map.of("ID", Long.class, "NAME", String.class));
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("BIG");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(count, rows.size());
        assertEquals(0L, rows.get(0).get("ID"));
        assertEquals(99_999L, rows.get(count - 1).get("ID"));
        assertEquals("name-12345", rows.get(12_345).get("NAME"));
        storage.close();
        loaded.close();
    }

    // ── StorageFactory integration ──────────────────────────────────

    @Test
    void storageFactoryCreatesJsonl() {
        var storage = StorageFactory.create("jsonl", "FACT_TEST", cols(), types());
        assertInstanceOf(JsonlRowStorage.class, storage);
    }

    @Test
    void storageFactoryDefaultIsInMemory() {
        var storage = StorageFactory.create(null, "FACT_TEST", cols(), types());
        assertInstanceOf(diesel.storage.InMemoryRowStorage.class, storage);
    }

    // ── Database integration with JSONL ─────────────────────────────

    @Test
    void databaseWithJsonlStorage() {
        String prev = System.getProperty("diesel.storage.type");
        try {
            System.setProperty("diesel.storage.type", "jsonl");
            Database db = new Database(tempDir.toString());
            db.executeQuery("CREATE TABLE DBJSONL (ID LONG, NAME STRING, SCORE INTEGER)", null);
            db.executeQuery("INSERT INTO DBJSONL (ID, NAME, SCORE) VALUES (1, 'Alice', 100)", null);
            db.executeQuery("INSERT INTO DBJSONL (ID, NAME, SCORE) VALUES (2, 'Bob', 200)", null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> result =
                    (List<Map<String, Object>>) db.executeQuery("SELECT * FROM DBJSONL ORDER BY ID", null);
            assertEquals(2, result.size());
            assertEquals("Alice", result.get(0).get("NAME"));
            assertEquals("Bob", result.get(1).get("NAME"));

            Table table = db.getTable("DBJSONL");
            assertInstanceOf(JsonlRowStorage.class, table.getStorage());

            File jsonlFile = new File(tempDir.toString(), "DBJSONL.jsonl");
            assertTrue(jsonlFile.exists(), "JSONL file should be created on disk");
        } finally {
            if (prev == null) {
                System.clearProperty("diesel.storage.type");
            } else {
                System.setProperty("diesel.storage.type", prev);
            }
        }
    }
}