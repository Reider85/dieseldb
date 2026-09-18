package diesel;

import diesel.storage.json.JsonParserConfig;
import diesel.storage.JsonlRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 55: {@link JsonlRowStorage} lazy deferred loads
 * ({@code jsonl.lazy.blocks}).
 *
 * <p>The storage defers the full parse when a plain rewrite-mode file has a
 * known schema; {@code readProjected()} then serves column subsets from real
 * disk byte ranges while classic row access (scan/insert/update/delete/save)
 * transparently materialises the full load. These tests prove the deferred and
 * materialised paths yield identical values, that deferred disk reads reflect
 * external appends (the pre-scan stamp self-invalidates), that mutations force
 * materialisation and stay visible, that saves round-trip the deferred rows,
 * and that append mode / compressed codecs never defer.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlLazyLoadTest {

    private static final String LAZY_KEY = "jsonl.lazy.blocks";
    private static final String CODEC_KEY = "jsonl.compression.codec";

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty(LAZY_KEY);
        System.clearProperty(CODEC_KEY);
    }

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static JsonParserConfig lazyConfig() {
        return JsonParserConfig.builder().lazyBlocks(true).build();
    }

    private JsonlRowStorage load(String table, JsonParserConfig config) {
        JsonlRowStorage storage = new JsonlRowStorage(table, cols(), types(), config);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile(table);
        return storage;
    }

    private void write(String table, String content) throws Exception {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    private static String simpleRow(int id, String name, Integer age) {
        String ageJson = age == null ? "null" : Integer.toString(age);
        return "{\"ID\":" + id + ",\"NAME\":\"" + name + "\",\"AGE\":" + ageJson + "}\n";
    }

    private static String fileRows(int n) {
        StringBuilder sb = new StringBuilder(n * 40);
        for (int i = 1; i <= n; i++) {
            sb.append(simpleRow(i, "name-" + i, i % 100));
        }
        return sb.toString();
    }

    @Test
    void lazyDeferServesProjectedReadsThenMaterialisesOnScan() throws Exception {
        String table = "DEFER";
        write(table, fileRows(2000));

        JsonlRowStorage storage;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            storage = load(table, lazyConfig());
            assertTrue(cap.events().stream().anyMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")),
                    "load must defer the full parse");
        }
        try {
            List<Map<String, Object>> projected = storage.readProjected(List.of("ID", "NAME"));
            assertEquals(2000, projected.size());
            assertEquals(1L, projected.get(0).get("ID"));
            assertEquals("name-2000", projected.get(1999).get("NAME"));
            assertEquals(2, projected.get(0).keySet().size());

            // A classic access materialises the full load
            List<Map<String, Object>> scan = storage.scan();
            assertEquals(2000, scan.size());
            assertEquals(3, scan.get(0).keySet().size());
            assertEquals(25, scan.get(24).get("AGE"));

            List<Map<String, Object>> after = storage.readProjected(List.of("ID", "NAME"));
            assertEquals(2000, after.size());
            assertEquals(projected, after);
        } finally {
            storage.close();
        }
    }

    @Test
    void lazyReadProjectedEqualsClassicProjectedAndScan() throws Exception {
        String table = "EQ";
        String content = "\uFEFF{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":25}\n"
                + "\n"
                + "{\"ID\":2,\"NAME\":\"Bob\"}\n"
                + "{\"ID\":3,\"NAME\":null,\"AGE\":30}\n"
                + "{\"ID\":4,\"NAME\":\"Ünïcødé 中 😀\",\"AGE\":40}\r\n";
        write(table, content);

        JsonlRowStorage lazy;
        JsonlRowStorage classic;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            lazy = load(table, lazyConfig());
            assertTrue(cap.events().stream().anyMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")));
        }
        classic = load(table, JsonParserConfig.builder().build());
        try {
            List<String> items = List.of("ID", "NAME", "AGE");
            List<Map<String, Object>> fromLazy = lazy.readProjected(items);
            List<Map<String, Object>> fromClassic = classic.readProjected(items);
            assertEquals(fromClassic, fromLazy, "deferred disk read must equal the classic materialised projection");

            List<Map<String, Object>> scans = classic.scan();
            assertEquals(4, scans.size());
            assertEquals(fromLazy, scans, "projection equals the full scan for these items");

            assertEquals(25, fromLazy.get(0).get("AGE"));
            assertEquals("Ünïcødé 中 😀", fromLazy.get(3).get("NAME"));
            assertEquals(null, fromLazy.get(1).get("AGE"), "missing field -> null");
        } finally {
            lazy.close();
            classic.close();
        }
    }

    @Test
    void flattenDotColumnsProjectThroughLazyBlocks() throws Exception {
        String table = "FLAT";
        // FLATTEN schema with exact leaf columns; a scalar array stays in json mode.
        List<String> flatCols = List.of("ID", "user.address.city", "user.address.country", "tags");
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("ID", Long.class);
        types.put("user.address.city", String.class);
        types.put("user.address.country", String.class);
        types.put("tags", String.class);

        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 150; i++) {
            sb.append("{\"ID\":").append(i).append(",\"user\":{\"address\":{\"city\":\"City")
                    .append(i).append("\",\"country\":\"RU\"}},\"tags\":[")
                    .append(i).append(",").append(i + 1).append("]}\n");
        }
        Files.write(tempDir.resolve(table + ".jsonl"), sb.toString().getBytes(StandardCharsets.UTF_8));

        JsonlRowStorage lazyConfigStorage = new JsonlRowStorage(table, flatCols, types, lazyConfig());
        lazyConfigStorage.setDataDir(tempDir.toString());
        lazyConfigStorage.open();
        try {
            boolean deferred;
            try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
                lazyConfigStorage.loadFromFile(table);
                deferred = cap.events().stream().anyMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]"));
            }
            assertTrue(deferred, "flat schema + lazy flag must defer");
            List<Map<String, Object>> projected = lazyConfigStorage.readProjected(List.of("ID", "user.address.city"));
            assertEquals(150, projected.size());
            assertEquals("City1", projected.get(0).get("user.address.city"));
            assertEquals("City150", projected.get(149).get("user.address.city"));
        } finally {
            lazyConfigStorage.close();
        }
    }

    @Test
    void externalAppendBecomesVisibleWithoutMaterialisation() throws Exception {
        String table = "EXT";
        write(table, fileRows(100));

        JsonlRowStorage storage;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            storage = load(table, lazyConfig());
            assertTrue(cap.events().stream().anyMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")));
        }
        try {
            assertEquals(100, storage.readProjected(List.of("ID")).size());
            // External append: the pre-scan stamp (mtime/size) no longer matches,
            // so the next block read re-scans the current file.
            Files.write(tempDir.resolve(table + ".jsonl"),
                    simpleRow(101, "external", 101).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
            List<Map<String, Object>> after = storage.readProjected(List.of("ID"));
            assertEquals(101, after.size());
            assertEquals(101L, after.get(100).get("ID"));
        } finally {
            storage.close();
        }
    }

    @Test
    void mutationForcesMaterialisationAndCommandsStayVisible() throws Exception {
        String table = "MUT";
        write(table, simpleRow(1, "before", 1) + simpleRow(2, "keep", 2));

        JsonlRowStorage storage = load(table, lazyConfig());
        try {
            storage.insert(mapOf(3L, "inserted", 3));
            storage.update(0, mapOf(1L, "updated", 11));
            storage.delete(1);
            List<Map<String, Object>> scan = storage.scan();
            assertEquals(2, scan.size());
            assertEquals("updated", scan.get(0).get("NAME"));
            assertEquals(11, scan.get(0).get("AGE"));
            assertEquals("inserted", scan.get(1).get("NAME"));

            List<Map<String, Object>> projected = storage.readProjected(List.of("ID", "NAME"));
            assertEquals(2, projected.size());
            assertEquals("updated", projected.get(0).get("NAME"));
            assertEquals("inserted", projected.get(1).get("NAME"));
        } finally {
            storage.close();
        }
    }

    @Test
    void saveAfterDeferredReadWritesAllDeferredRows() throws Exception {
        String table = "SAVE";
        write(table, simpleRow(1, "kept-a", 10) + simpleRow(2, "kept-b", 20));

        JsonlRowStorage storage = load(table, lazyConfig());
        try {
            // deferred read (no materialisation)
            assertEquals(2, storage.readProjected(List.of("ID")).size());
            storage.insert(mapOf(3L, "kept-c", 30));
            storage.saveToFile(table);
            assertEquals(3, storage.scan().size());
        } finally {
            storage.close();
        }

        JsonlRowStorage reloaded = load(table, JsonParserConfig.builder().build());
        try {
            List<Map<String, Object>> rows = reloaded.scan();
            assertEquals(3, rows.size());
            assertEquals("kept-a", rows.get(0).get("NAME"));
            assertEquals("kept-c", rows.get(2).get("NAME"));
        } finally {
            reloaded.close();
        }
    }

    @Test
    void appendWriteModeNeverDefers() throws Exception {
        String table = "APPEND";
        write(table, fileRows(50));

        JsonParserConfig cfg = JsonParserConfig.builder()
                .lazyBlocks(true)
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .build();
        JsonlRowStorage storage;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            storage = load(table, cfg);
            assertTrue(cap.events().stream().noneMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")),
                    "append mode must never defer");
        }
        try {
            assertEquals(50, storage.getInternalRows().size());
        } finally {
            storage.close();
        }
    }

    @Test
    void compressedCodecNeverDefers() throws Exception {
        String table = "ZLIB";
        // Produce a compressed file through a non-lazy writer
        System.setProperty(CODEC_KEY, "zstd");
        JsonlRowStorage writer = new JsonlRowStorage(table, cols(), types(), JsonParserConfig.builder().build());
        writer.setDataDir(tempDir.toString());
        writer.open();
        try {
            writer.insert(mapOf(1L, "z1", 1));
            writer.insert(mapOf(2L, "z2", 2));
            writer.saveToFile(table);
        } finally {
            writer.close();
        }

        JsonlRowStorage storage;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            storage = load(table, lazyConfig());
            assertTrue(cap.events().stream().noneMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")),
                    "compressed files (frame-based) must never defer");
        }
        try {
            assertEquals(2, storage.getInternalRows().size());
            List<Map<String, Object>> projected = storage.readProjected(List.of("ID", "NAME"));
            assertEquals(2, projected.size());
            assertEquals("z1", projected.get(0).get("NAME"));
        } finally {
            storage.close();
        }
    }

    @Test
    void withoutLazyFlagNothingIsDeferred() throws Exception {
        String table = "OFF";
        write(table, fileRows(10));
        JsonlRowStorage storage;
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowStorage")) {
            storage = load(table, JsonParserConfig.builder().build());
            assertTrue(cap.events().stream().noneMatch(e -> e.getFormattedMessage().startsWith("[JSONL-LAZY]")));
        }
        try {
            assertEquals(10, storage.getInternalRows().size());
        } finally {
            storage.close();
        }
    }

    @Test
    void deferredReadAnswersAllClassicReadsAfterMaterialisation() throws Exception {
        String table = "ALL";
        write(table, fileRows(777));
        JsonlRowStorage storage = load(table, lazyConfig());
        try {
            List<Map<String, Object>> before = storage.readProjected(List.of("ID"));
            assertNotNull(storage.getInternalRows());
            List<Map<String, Object>> after = storage.readProjected(List.of("ID"));
            assertEquals(before, after);
            assertFalse(after.isEmpty());
            assertEquals(777, after.size());
        } finally {
            storage.close();
        }
    }

    private static Map<String, Object> mapOf(long id, String name, int age) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", name);
        m.put("AGE", age);
        return m;
    }
}
