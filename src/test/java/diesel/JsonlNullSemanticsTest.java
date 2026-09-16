package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlRowWriter;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 47: JSONL NULL semantics - null vs missing field vs empty string.
 *
 * <p>Write semantics: explicit null → JSON {@code null}, empty string → {@code ""},
 * absent value → key not written. Read semantics: missing key follows the
 * {@code jsonl.missing.field} policy ({@code null}/{@code default} backward-compatible
 * null, {@code error} fails the row). The per-row presence mask keeps all three
 * states distinct through a load→save round trip.
 */
@Tag("storage")
class JsonlNullSemanticsTest {

    @TempDir
    Path tempDir;

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static JsonlRowWriter newWriter(StringWriter sw) throws Exception {
        JsonlRowWriter w = new JsonlRowWriter(sw, COLS);
        return w;
    }

    // ── Writer: omitted vs explicit null vs empty ───────────────────

    @Test
    void writerOmitsMissingKeysKeepsNullAndEmpty() throws Exception {
        StringWriter sw = new StringWriter();
        try (JsonlRowWriter writer = newWriter(sw)) {
            // fully present row 1: NAME=null, AGE=30
            Object[] r1 = {1L, null, 30};
            writer.writeRow(r1, new boolean[]{true, true, true});
            // row 2: NAME missing, AGE=null (present but null)
            Object[] r2 = {2L, "ignored", null};
            writer.writeRow(r2, new boolean[]{true, false, true});
            // row 3: NAME=""
            Object[] r3 = {3L, "", 30};
            writer.writeRow(r3, new boolean[]{true, true, true});
        }
        String[] lines = sw.toString().split("\n");
        assertEquals("{\"ID\":1,\"NAME\":null,\"AGE\":30}", lines[0]);
        assertEquals("{\"ID\":2,\"AGE\":null}", lines[1]);
        assertEquals("{\"ID\":3,\"NAME\":\"\",\"AGE\":30}", lines[2]);
    }

    @Test
    void writerWithNullPresentMaskIsAllPresentFallback() throws Exception {
        StringWriter sw = new StringWriter();
        try (JsonlRowWriter writer = newWriter(sw)) {
            Object[] r = {1L, null, 30};
            writer.writeRow(r, null);
            writer.writeRow(r);
        }
        String[] lines = sw.toString().split("\n");
        // pre-prompt-47 behaviour: every column written, null as JSON null
        assertEquals("{\"ID\":1,\"NAME\":null,\"AGE\":30}", lines[0]);
        assertEquals("{\"ID\":1,\"NAME\":null,\"AGE\":30}", lines[1]);
    }

    // ── Reader: presence flags + missing-field policy ───────────────

    @Test
    void readerExposesPresenceFlagsPerRow() throws Exception {
        String jsonl = "{\"ID\":1,\"NAME\":null,\"AGE\":30}\n"
                + "{\"ID\":2}\n"
                + "{\"ID\":3,\"NAME\":\"\",\"AGE\":40}\n";
        try (JsonlRowReader reader = new JsonlRowReader(
                new BufferedReader(new StringReader(jsonl)), COLS, null)) {
            assertTrue(reader.hasNext());
            Object[] row = reader.nextArray();
            boolean[] present = reader.getLastRowPresent();
            assertNotNull(present);
            assertTrue(present[0], "ID present");
            assertTrue(present[1], "NAME explicit null still present");
            assertTrue(present[2], "AGE present");
            assertNull(row[1], "explicit JSON null reads back as null");

            assertTrue(reader.hasNext());
            reader.nextArray();
            boolean[] p2 = reader.getLastRowPresent();
            assertTrue(p2[0]);
            assertFalse(p2[1], "NAME key absent");
            assertFalse(p2[2], "AGE key absent");

            assertTrue(reader.hasNext());
            Object[] row3 = reader.nextArray();
            boolean[] p3 = reader.getLastRowPresent();
            assertTrue(p3[1], "empty string field is present");
            assertEquals("", row3[1], "empty string reads back as empty, not null");
        }
    }

    @Test
    void readerErrorModeThrowsOnMissingColumnWithCoordinates() throws Exception {
        JsonParserConfig error = JsonParserConfig.builder()
                .missingField(JsonParserConfig.MissingFieldMode.ERROR)
                .build();
        String jsonl = "{\"ID\":1,\"NAME\":\"x\",\"AGE\":30}\n"
                + "{\"ID\":2,\"NAME\":\"y\"}\n";
        DieselIOException e;
        try (JsonlRowReader reader = new JsonlRowReader(
                new BufferedReader(new StringReader(jsonl)), COLS, null, "users.jsonl", error)) {
            assertNotNull(reader.nextArray());
            e = assertThrows(DieselIOException.class, reader::nextArray);
        }
        assertTrue(e.getMessage().contains("users.jsonl"), e.getMessage());
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        assertTrue(e.getMessage().contains("AGE"), e.getMessage());
    }

    @Test
    void readerDefaultModeMapsMissingToNull() throws Exception {
        String jsonl = "{\"ID\":1}\n";
        try (JsonlRowReader reader = new JsonlRowReader(
                new BufferedReader(new StringReader(jsonl)), COLS, null)) {
            Object[] row = reader.nextArray();
            assertNull(row[1]);
            assertNull(row[2]);
        }
    }

    // ── Full storage round trip ─────────────────────────────────────

    @Test
    void storageRoundTripPreservesNullVsMissing() throws Exception {
        File file = new File(tempDir.toString(), "P47.jsonl");
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\":1,\"NAME\":null,\"AGE\":30}\n");
            fw.write("{\"ID\":2,\"AGE\":40}\n");
        }

        JsonlRowStorage loaded = new JsonlRowStorage("P47", COLS, Map.of());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("P47");
        List<boolean[]> presence = loaded.getRowPresence();
        assertEquals(2, presence.size());
        assertTrue(presence.get(0)[1], "row 0 NAME was explicit null");
        assertFalse(presence.get(1)[1], "row 1 NAME key was absent");

        loaded.saveToFile("P47");
        String[] lines = Files.readAllLines(file.toPath()).toArray(new String[0]);
        assertEquals("{\"ID\":\"1\",\"NAME\":null,\"AGE\":\"30\"}", lines[0]);
        assertEquals("{\"ID\":\"2\",\"AGE\":\"40\"}", lines[1], "absent NAME must stay absent");
        loaded.close();
    }

    @Test
    void storageInsertedRowsAreAllPresent() {
        JsonlRowStorage storage = new JsonlRowStorage("P47INS", COLS, Map.of());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("NAME", null);
        storage.insert(r);
        List<boolean[]> presence = storage.getRowPresence();
        boolean[] p = presence.get(0);
        assertTrue(p[0]);
        assertTrue(p[1], "in-memory insert treats NAME as present (writes null)");
        assertTrue(p[2]);
        storage.close();
    }

    @Test
    void nestedObjectWithNullInsideRoundTrips() throws Exception {
        List<String> flattenCols = List.of("ID", "user.name", "user.phone");
        JsonlRowStorage storage = new JsonlRowStorage("P47NEST", flattenCols, Map.of());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("user.name", "Alice");
        r.put("user.phone", null);
        storage.insert(r);
        storage.saveToFile("P47NEST");

        File file = new File(tempDir.toString(), "P47NEST.jsonl");
        String line = Files.readString(file.toPath()).trim();
        assertEquals("{\"ID\":1,\"user\":{\"name\":\"Alice\",\"phone\":null}}", line,
                "explicit null leaf keeps its key inside the nested object");

        JsonlRowStorage loaded = new JsonlRowStorage("P47NEST", flattenCols, Map.of());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("P47NEST");
        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("user.name"));
        assertNull(rows.get(0).get("user.phone"));
        storage.close();
        loaded.close();
    }

    @Test
    void missingNestedLeafStaysAbsentOnRoundTrip() throws Exception {
        List<String> flattenCols = List.of("ID", "user.name", "user.address.city");
        File file = new File(tempDir.toString(), "P47NEST2.jsonl");
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\":1,\"user\":{\"name\":\"Alice\"}}\n");
        }

        JsonlRowStorage loaded = new JsonlRowStorage("P47NEST2", flattenCols, Map.of());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("P47NEST2");
        loaded.saveToFile("P47NEST2");

        String[] lines = Files.readAllLines(file.toPath()).toArray(new String[0]);
        assertEquals("{\"ID\":\"1\",\"user\":{\"name\":\"Alice\"}}", lines[0],
                "absent leaf must not be reconstructed as null key");
        loaded.close();
    }

    @Test
    void arrayWithNullElementsRoundTrips() throws Exception {
        List<String> jsonCols = List.of("ID", "TAGS");
        JsonlRowStorage storage = new JsonlRowStorage("P47ARR", jsonCols, Map.of());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("TAGS", Arrays.asList(1, null, 3));
        storage.insert(r);
        storage.saveToFile("P47ARR");

        File file = new File(tempDir.toString(), "P47ARR.jsonl");
        String line = Files.readString(file.toPath()).trim();
        assertEquals("{\"ID\":1,\"TAGS\":[1,null,3]}", line);

        JsonlRowStorage loaded = new JsonlRowStorage("P47ARR", jsonCols, Map.of());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("P47ARR");
        assertEquals("[1,null,3]", loaded.scan().get(0).get("TAGS"));
        storage.close();
        loaded.close();
    }

    @Test
    void backwardCompatAllColumnsWrittenPre47() throws Exception {
        // Old writer emitted every schema column including nulls. The new
        // reader flags all of them present, so saving an old file keeps its
        // explicit null keys.
        File file = new File(tempDir.toString(), "P47LEGACY.jsonl");
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("{\"ID\":1,\"NAME\":null,\"AGE\":null}\n");
        }

        JsonlRowStorage loaded = new JsonlRowStorage("P47LEGACY", COLS, Map.of());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("P47LEGACY");
        boolean[] p = loaded.getRowPresence().get(0);
        assertTrue(p[1]);
        assertTrue(p[2]);

        loaded.saveToFile("P47LEGACY");
        String[] lines = Files.readAllLines(file.toPath()).toArray(new String[0]);
        assertEquals("{\"ID\":\"1\",\"NAME\":null,\"AGE\":null}", lines[0],
                "legacy file round-trips byte-identically");
        loaded.close();
    }
}