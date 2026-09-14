package diesel;

import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 45 - JSONL nested structures: storage rules, dot-path resolution and
 * SQL glue for both {@code jsonl.nested.mode} values.
 */
class JsonlNestedModeTest {

    @TempDir
    Path tempDir;

    private static JsonParserConfig flatten() {
        return JsonParserConfig.builder()
                .nestedMode(JsonParserConfig.NestedMode.FLATTEN).build();
    }

    private static JsonParserConfig jsonColumn() {
        return JsonParserConfig.builder()
                .nestedMode(JsonParserConfig.NestedMode.JSON_COLUMN).build();
    }

    private static JsonParserConfig expand() {
        return JsonParserConfig.builder()
                .nestedMode(JsonParserConfig.NestedMode.FLATTEN)
                .arrayColumns(JsonParserConfig.ArrayColumnsMode.EXPAND).build();
    }

    // ── Flatten mode: dotted columns round-trip ───────────────────────

    @Test
    void flattenDepth3RoundTrip() {
        List<String> cols = List.of("ID", "user.name", "user.address.city", "user.address.country");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class,
                "user.name", String.class,
                "user.address.city", String.class,
                "user.address.country", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("F3", cols, types, flatten());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("user.name", "Alice");
        r.put("user.address.city", "Москва");
        r.put("user.address.country", "RU");
        storage.insert(r);
        storage.saveToFile("F3");

        JsonlRowStorage loaded = new JsonlRowStorage("F3", cols, types, flatten());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("F3");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(1, rows.size());
        assertEquals("Москва", rows.get(0).get("user.address.city"));
        assertEquals("RU", rows.get(0).get("user.address.country"));
        assertEquals("Alice", rows.get(0).get("user.name"));
        assertTrue(rows.get(0).containsKey("user.address.city"),
                "dotted leaf survives round-trip as a literal column");
        storage.close();
        loaded.close();
    }

    @Test
    void flattenMixedFlatAndNestedRoundTrip() {
        List<String> cols = List.of("ID", "user.name", "user.address.city", "settings");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class,
                "user.name", String.class,
                "user.address.city", String.class,
                "settings", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("FMX", cols, types, flatten());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("user.name", "Alice");
        r.put("user.address.city", "Москва");
        r.put("settings", "{\"notif\":true,\"theme\":\"dark\"}");
        storage.insert(r);
        storage.saveToFile("FMX");

        JsonlRowStorage loaded = new JsonlRowStorage("FMX", cols, types, flatten());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("FMX");

        Map<String, Object> row = loaded.scan().get(0);
        assertEquals("Москва", row.get("user.address.city"));
        assertEquals("{\"notif\":true,\"theme\":\"dark\"}",
                row.get("settings"), "whole-nested value stays in a JSON column");
        storage.close();
        loaded.close();
    }

    @Test
    void flattenDotPrefixSchemaConflictIsRejected() {
        assertThrows(RuntimeException.class,
                () -> new JsonlSchemaManager(List.of("user.address", "user.address.city"),
                        Map.of("user.address", String.class, "user.address.city", String.class),
                        flatten()),
                "a dot-prefix column pair is ambiguous in flatten mode");
    }

    // ── Json-column mode: whole structure in one column ───────────────

    @Test
    void jsonColumnDepth3RoundTripAndPathExtraction() {
        List<String> cols = List.of("ID", "PROFL");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "PROFL", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("J3", cols, types, jsonColumn());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("user", Map.of("name", "Alice", "address", Map.of("city", "Москва", "country", "RU")));
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("PROFL", inner);
        storage.insert(r);
        storage.saveToFile("J3");

        JsonlRowStorage loaded = new JsonlRowStorage("J3", cols, types, jsonColumn());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("J3");

        String jsonText = String.valueOf(loaded.scan().get(0).get("PROFL"));
        JsonlSchemaManager manager = new JsonlSchemaManager(cols, types, jsonColumn());
        JsonlSchemaManager.ProjectionSlot slot = manager.resolveProjectionItem("PROFL.user.address.city");
        assertEquals(List.of("user", "address", "city"), slot.segments());
        assertEquals("Москва", manager.extractPathValue(jsonText, slot.segments()));
        storage.close();
        loaded.close();
    }

    // ── Arrays ────────────────────────────────────────────────────────

    @Test
    void arrayOfObjectsRoundsTripAsJsonColumn() {
        List<String> cols = List.of("ID", "orgs");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "orgs", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("AOBJ", cols, types, flatten());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("orgs", "[{\"name\":\"Acme\",\"id\":7},{\"name\":\"Beta\",\"id\":8}]");
        storage.insert(r);
        storage.saveToFile("AOBJ");

        JsonlRowStorage loaded = new JsonlRowStorage("AOBJ", cols, types, flatten());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("AOBJ");
        assertEquals("[{\"name\":\"Acme\",\"id\":7},{\"name\":\"Beta\",\"id\":8}]",
                loaded.scan().get(0).get("orgs"),
                "array of objects is captured whole as one JSON column");
        storage.close();
        loaded.close();
    }

    @Test
    void scalarArrayDefaultsToJsonColumn() {
        List<String> cols = List.of("ID", "tags");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "tags", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("TAGSJ", cols, types, jsonColumn());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("tags", "[\"a\",\"b\"]");
        storage.insert(r);
        storage.saveToFile("TAGSJ");

        JsonlRowStorage loaded = new JsonlRowStorage("TAGSJ", cols, types, jsonColumn());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("TAGSJ");
        assertEquals("[\"a\",\"b\"]", loaded.scan().get(0).get("tags"));
        storage.close();
        loaded.close();
    }

    @Test
    void scalarArrayExpandsToIndexColumns() {
        List<String> cols = List.of("ID", "tags[0]", "tags[1]");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "tags[0]", String.class, "tags[1]", String.class);
        JsonlRowStorage storage = new JsonlRowStorage("TAGSE", cols, types, expand());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("tags[0]", "a");
        r.put("tags[1]", "b");
        storage.insert(r);
        storage.saveToFile("TAGSE");

        JsonlRowStorage loaded = new JsonlRowStorage("TAGSE", cols, types, expand());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("TAGSE");
        Map<String, Object> row = loaded.scan().get(0);
        assertEquals("a", row.get("tags[0]"));
        assertEquals("b", row.get("tags[1]"));
        storage.close();
        loaded.close();
    }

    // ── SQL glue: dot-paths work in both nested modes ────────────────

    @Test
    void sqlJsonPathWhereFindsRowsInJsonColumnMode() {
        Database db = new Database();
        db.executeQuery("CREATE TABLE JPF (ID LONG, PROFL STRING)", null);
        db.executeQuery("INSERT INTO JPF (ID, PROFL) VALUES (1, '{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Москва\",\"country\":\"RU\"}}}')", null);
        db.executeQuery("INSERT INTO JPF (ID, PROFL) VALUES (2, '{\"user\":{\"name\":\"Bob\",\"address\":{\"city\":\"Париж\",\"country\":\"FR\"}}}')", null);

        List<Map<String, Object>> rows = query(db, "SELECT * FROM JPF WHERE PROFL.user.address.city = 'Москва'");
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
    }

    @Test
    void sqlJsonPathProjectionExtractsLeafInJsonColumnMode() {
        Database db = new Database();
        db.executeQuery("CREATE TABLE JPF (ID LONG, PROFL STRING)", null);
        db.executeQuery("INSERT INTO JPF (ID, PROFL) VALUES (1, '{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Москва\"}}}')", null);
        db.executeQuery("INSERT INTO JPF (ID, PROFL) VALUES (2, '{\"user\":{\"name\":\"Bob\",\"address\":{\"city\":\"Париж\"}}}')", null);

        List<Map<String, Object>> proj = query(db, "SELECT PROFL.user.address.city FROM JPF");
        assertEquals(2, proj.size());
        assertEquals("Москва", proj.get(0).get("PROFL"));
        assertEquals("Париж", proj.get(1).get("PROFL"));
    }

    @Test
    void sqlDottedLeafWhereFindsRowsInFlattenMode() {
        Database db = new Database();
        db.executeQuery("CREATE TABLE FLATU (ID LONG, \"user.name\" STRING, \"user.address.city\" STRING)", null);
        db.executeQuery("INSERT INTO FLATU (ID, \"user.name\", \"user.address.city\") VALUES (1, 'Alice', 'Москва')", null);
        db.executeQuery("INSERT INTO FLATU (ID, \"user.name\", \"user.address.city\") VALUES (2, 'Bob', 'Париж')", null);

        List<Map<String, Object>> rows = query(db, "SELECT * FROM FLATU WHERE \"user.address.city\" = 'Москва'");
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("Alice", rows.get(0).get("user.name"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> query(Database db, String sql) {
        return (List<Map<String, Object>>) db.executeQuery(sql, null);
    }
}