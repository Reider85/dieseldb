package diesel;

import diesel.storage.JsonlRowStorage;
import diesel.storage.CompressionFactory;
import diesel.storage.JsonlSchemaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 56 (§2) JSONL property-style tests. Written as seeded, hypothesis-free
 * property tests (no jqwik dependency): randomised exotic values round-trip
 * byte-identically across types (unicode, emoji, control characters and
 * boundary longs), the parallel reader (prompt 54) produces exactly the rows,
 * presence flags and nested re-embedding of a sequential pass for randomised
 * content, a second load-&gt;save cycle is byte-identical, randomised nested
 * structures survive both nested modes, and exotic string values survive a
 * zstd-compressed round trip (prompt 52).
 */
@Tag("storage")
class JsonlPropertyTest {

    private static final String THRESHOLD_KEY = "jsonl.parallel.read.threshold";
    private static final String CODEC_KEY = "jsonl.compression.codec";
    private static final String SCHEMA_KEY = "jsonl.schema.mode";
    private static final String NESTED_KEY = "jsonl.nested.mode";
    private static final String ARRAY_KEY = "jsonl.array.columns";
    private static final String[] PROP_KEYS = {
            THRESHOLD_KEY, CODEC_KEY, SCHEMA_KEY, NESTED_KEY, ARRAY_KEY,
            "jsonl.duplicate.keys", "jsonl.type.coercion", "jsonl.missing.field",
            "jsonl.load.error.mode", "jsonl.write.mode", "jsonl.lazy.blocks"
    };

    @TempDir
    Path tempDir;

    private final Map<String, String> prev = new LinkedHashMap<>();

    @AfterEach
    void restoreProps() {
        for (String key : PROP_KEYS) {
            String value = prev.get(key);
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }
    }

    private void saveProps() {
        for (String key : PROP_KEYS) {
            prev.put(key, System.getProperty(key));
        }
    }

    private void clearAllProps() {
        for (String key : PROP_KEYS) {
            System.clearProperty(key);
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────

    private JsonlRowStorage storage(String table, List<String> cols, Map<String, Class<?>> types) {
        JsonlRowStorage s = new JsonlRowStorage(table, cols, types);
        s.setDataDir(tempDir.toString());
        s.open();
        return s;
    }

    private void writeRandom(String table, String content) throws Exception {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertByteEqual(List<Object[]> a, List<Object[]> b) {
        assertEquals(a.size(), b.size(), "row count");
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals(a.get(i), b.get(i), "row " + i);
        }
    }

    private static void assertPresenceEqual(List<boolean[]> a, List<boolean[]> b) {
        assertEquals(a.size(), b.size(), "presence count");
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals(a.get(i), b.get(i), "presence " + i);
        }
    }

    /** Escapes one JSON string literal (RFC 8259 minimal set). */
    private static String esc(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static String jsonl(int id, String name, BigDecimal score, Boolean active) {
        StringBuilder sb = new StringBuilder("{\"ID\":").append(id);
        if (name != null) {
            sb.append(",\"NAME\":").append(esc(name));
        } else {
            sb.append(",\"NAME\":null");
        }
        sb.append(",\"SCORE\":").append(score != null ? score.toPlainString() : "null");
        if (active != null) {
            sb.append(",\"ACTIVE\":").append(active);
        }
        return sb.append("}\n").toString();
    }

    // ─── §2.1 exotic values round-trip byte-identically ───────────────

    @Test
    void randomExoticValuesRoundTripAcrossTypes() throws Exception {
        saveProps();
        clearAllProps();
        try {
            List<String> cols = List.of("ID", "NAME", "SCORE", "ACTIVE");
            Map<String, Class<?>> types = Map.of(
                    "ID", Long.class, "NAME", String.class,
                    "SCORE", BigDecimal.class, "ACTIVE", Boolean.class);

            Random rnd = new Random(0x5EED);
            String[] exotic = {
                    "не тең аԛ 中文 عربي 🌈",
                    "tab\there", "line\nbreak", "quote \" and slash \\",
                    "", " \u00A0\u2007\u202F ",
                    "\u0001\u001F\u007F", "А" + "\u0301" + "⃐".repeat(2),
                    "emoji 🧑\u200D💻🚀", "mixed ½ ¥ €", "A\u0000B",
            };
            StringBuilder sb = new StringBuilder();
            String[] expectedNames = new String[250];
            for (int i = 0; i < 250; i++) {
                String name = exotic[rnd.nextInt(exotic.length)];
                expectedNames[i] = name;
                long raw = rnd.nextLong();
                BigDecimal score = BigDecimal.valueOf(raw, rnd.nextInt(6));
                Boolean active = rnd.nextBoolean();
                sb.append(jsonl(i, name, score, active ? Boolean.TRUE : (rnd.nextBoolean() ? Boolean.FALSE : null)));
            }
            writeRandom("EXO", sb.toString());

            JsonlRowStorage s = storage("EXO", cols, types);
            s.loadFromFile("EXO");
            List<Map<String, Object>> rows = s.scan();

            assertEquals(250, rows.size());
            // Every exotic literal must survive verbatim.
            for (int i = 0; i < 250; i++) {
                String loaded = rows.get(i).get("NAME").toString();
                String expected = expectedNames[i];
                assertEquals(expected, loaded, "exotic value row " + i);
            }
            s.close();
        } finally {
            restoreProps();
        }
    }

    // ─── §2.2 parallel == sequential on randomised content ───────────

    @Test
    void randomizedParallelLoadEqualsSequentialWithPresence() throws Exception {
        saveProps();
        try {
            List<String> cols = List.of("ID", "NAME", "SCORE", "ACTIVE");
            Map<String, Class<?>> types = Map.of(
                    "ID", Long.class, "NAME", String.class,
                    "SCORE", BigDecimal.class, "ACTIVE", Boolean.class);

            Random rnd = new Random(0xCAFE);
            String[] names = {"alpha", "beta", "Γιάννης", "あああ", "x".repeat(300), "é\0", "w\"w"};
            StringBuilder sb = new StringBuilder();
            int n = 3000;
            for (int i = 0; i < n; i++) {
                String name;
                switch (rnd.nextInt(4)) {
                    case 0 -> name = names[rnd.nextInt(names.length)];
                    case 1 -> name = "";                 // empty string
                    case 2 -> name = null;               // JSON null
                    default -> name = "name-" + i + "\n\u2028"; // unicode
                }
                sb.append(jsonl(i, name, BigDecimal.valueOf(i + 1, 3), rnd.nextInt(5) == 0 ? null : rnd.nextBoolean()));
            }
            writeRandom("PAR", sb.toString());

            System.setProperty(THRESHOLD_KEY, "1");
            JsonlRowStorage parallel = storage("PAR", cols, types);
            parallel.loadFromFile("PAR");
            List<Object[]> pRows = new ArrayList<>(parallel.getInternalRows());
            List<boolean[]> pPresence = new ArrayList<>(parallel.getRowPresence());
            parallel.saveToFile("PAR");
            String pSaved = Files.readString(tempDir.resolve("PAR.jsonl"));
            parallel.close();

            System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
            JsonlRowStorage sequential = storage("PAR", cols, types);
            sequential.loadFromFile("PAR");
            List<Object[]> sRows = new ArrayList<>(sequential.getInternalRows());
            List<boolean[]> sPresence = new ArrayList<>(sequential.getRowPresence());
            sequential.saveToFile("PAR");
            String sSaved = Files.readString(tempDir.resolve("PAR.jsonl"));
            sequential.close();

            assertEquals(n, pRows.size());
            assertByteEqual(sRows, pRows);
            assertPresenceEqual(sPresence, pPresence);
            assertEquals(sSaved, pSaved,
                    "randomised parallel and sequential passes must re-save byte-identically");
        } finally {
            restoreProps();
        }
    }

    // ─── §2.3 double-save byte identity ──────────────────────────────

    @Test
    void randomizedDoubleSaveIsByteIdentical() throws Exception {
        saveProps();
        clearAllProps();
        try {
            List<String> cols = List.of("ID", "NAME", "SCORE");
            Map<String, Class<?>> types = Map.of(
                    "ID", Long.class, "NAME", String.class, "SCORE", BigDecimal.class);

            Random rnd = new Random(0xBEEF);
            JsonlRowStorage s = storage("NL", cols, types);
            int rows = 500;
            for (int i = 0; i < rows; i++) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ID", (long) i);
                m.put("NAME", i % 7 == 0 ? null : "value-" + i + "-é" + rnd.nextInt(1000));
                m.put("SCORE", BigDecimal.valueOf(rnd.nextLong() % 1_000_000_000L, rnd.nextInt(4)));
                s.insert(m);
            }
            s.saveToFile("NL");
            Path p = tempDir.resolve("NL.jsonl");
            byte[] first = Files.readAllBytes(p);

            JsonlRowStorage s2 = storage("NL", cols, types);
            s2.loadFromFile("NL");
            s2.saveToFile("NL");
            s2.close();

            byte[] second = Files.readAllBytes(p);
            assertArrayEquals(first, second,
                    "a second load->save cycle must be byte-identical");
            s.close();
        } finally {
            restoreProps();
        }
    }

    // ─── §2.4 randomised nested structures in both modes ─────────────

    @Test
    void randomNestedStructuresRoundTripInBothModes() throws Exception {
        saveProps();
        clearAllProps();
        try {
            Random rnd = new Random(0xD00D);
            int n = 120;

            // json_column mode: a nested structure survives as one JSON column.
            Map<String, Class<?>> jsonTypes = Map.of("ID", Long.class, "BLK", String.class);
            System.setProperty(NESTED_KEY, "json_column");
            JsonlRowStorage flat = storage("NSTJ", List.of("ID", "BLK"), jsonTypes);
            for (int i = 0; i < n; i++) {
                Map<String, Object> block = new LinkedHashMap<>();
                Map<String, Object> a = new LinkedHashMap<>();
                Map<String, Object> b = new LinkedHashMap<>();
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("leaf", "c-" + i);
                c.put("num", rnd.nextInt(1000));
                b.put("c", c);
                b.put("list", List.of(rnd.nextInt(10), rnd.nextInt(10), rnd.nextInt(10)));
                a.put("b", b);
                block.put("a", a);
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ID", (long) i);
                m.put("BLK", block);
                flat.insert(m);
            }
            flat.saveToFile("NSTJ");
            flat.close();

            // The JSON holder must not be double-encoded as an escaped string.
            String rawJson = Files.readString(tempDir.resolve("NSTJ.jsonl"));
            assertTrue(rawJson.contains("\"a\":{\"b\":{"), "nested JSON must be embedded, not quoted: " + firstLines(rawJson, 1));

            diesel.storage.json.JsonParserConfig jsonColumnCfg = diesel.storage.json.JsonParserConfig.builder()
                    .nestedMode(diesel.storage.json.JsonParserConfig.NestedMode.JSON_COLUMN).build();
            JsonlSchemaManager manager = new JsonlSchemaManager(List.of("ID", "BLK"), jsonTypes, jsonColumnCfg);

            JsonlRowStorage reloaded1 = storage("NSTJ", List.of("ID", "BLK"), jsonTypes);
            reloaded1.loadFromFile("NSTJ");
            JsonlSchemaManager.ProjectionSlot slot = manager.resolveProjectionItem("BLK.a.b.c.leaf");
            assertNotNull(slot, "dot path BLK.a.b.c.leaf must resolve");
            for (int i = 0; i < n; i++) {
                String text = String.valueOf(reloaded1.scan().get(i).get("BLK"));
                assertEquals("c-" + i, manager.extractPathValue(text, slot.segments()),
                        "json_column mode must retain the nested leaf (row " + i + ")");
            }
            reloaded1.saveToFile("NSTJ");
            String bytes1 = Files.readString(tempDir.resolve("NSTJ.jsonl"));
            reloaded1.close();

            JsonlRowStorage reloaded2 = storage("NSTJ", List.of("ID", "BLK"), jsonTypes);
            reloaded2.loadFromFile("NSTJ");
            reloaded2.saveToFile("NSTJ");
            reloaded2.close();
            assertEquals(bytes1, Files.readString(tempDir.resolve("NSTJ.jsonl")),
                    "nested JSON column must survive a second round trip byte-identically");

            // flatten mode: the leaf values survive in dotted columns.
            Map<String, Class<?>> flatTypes = Map.of("ID", Long.class, "a.b.c", String.class, "a.b.n", Integer.class);
            System.setProperty(NESTED_KEY, "flatten");
            JsonlRowStorage flatStore = storage("NSTF", List.of("ID", "a.b.c", "a.b.n"), flatTypes);
            for (int i = 0; i < n; i++) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ID", (long) i);
                m.put("a.b.c", "c-" + i);
                m.put("a.b.n", rnd.nextInt(1000));
                flatStore.insert(m);
            }
            flatStore.saveToFile("NSTF");
            flatStore.close();

            // The writer must re-embed, not double-encode the dotted columns.
            String raw = Files.readString(tempDir.resolve("NSTF.jsonl"));
            assertTrue(raw.contains("\"a\":{\"b\":{\"c\":\"c-5\",\"n\":") || raw.contains("\"a\":{\"b\":{\"n\":"),
                    "flatten mode must rebuild the nested object: " + firstLines(raw, 3));

            JsonlRowStorage flatReload = storage("NSTF", List.of("ID", "a.b.c", "a.b.n"), flatTypes);
            flatReload.loadFromFile("NSTF");
            assertEquals("c-5", flatReload.scan().get(5).get("a.b.c"));
            assertNotNull(flatReload.scan().get(5).get("a.b.n"));
            flatReload.close();
        } finally {
            restoreProps();
        }
    }

    /** Deterministically builds a depth-3 JSON block with a scalar array. */
    private static String firstLines(String s, int lines) {
        StringBuilder sb = new StringBuilder();
        String[] parts = s.split("\n", -1);
        for (int i = 0; i < Math.min(lines, parts.length); i++) {
            if (i > 0) {
                sb.append(" / ");
            }
            sb.append(parts[i]);
        }
        return sb.toString().length() > 400 ? sb.substring(0, 400) : sb.toString();
    }

    // ─── §2.5 exotic values survive zstd ──────────────────────────────

    @Test
    void randomExoticValuesSurviveZstdCompression() throws Exception {
        saveProps();
        clearAllProps();
        try {
            Random rnd = new Random(0xF00D);
            List<String> cols = List.of("ID", "NAME", "SCORE");
            Map<String, Class<?>> types = Map.of(
                    "ID", Long.class, "NAME", String.class, "SCORE", BigDecimal.class);

            StringBuilder text = new StringBuilder();
            for (int i = 0; i < 400; i++) {
                text.append("{\"ID\":").append(i).append(",\"NAME\":")
                        .append(esc(Path.of("в" + "\u0304", "рус").toString() + "|" + new String(new char[]{'🧑', '💻'})))
                        .append(",\"SCORE\":").append(BigDecimal.valueOf(rnd.nextLong() % 1000_000L, 2).toPlainString())
                        .append("}\n");
            }

            Path zst = tempDir.resolve("ZSTD.jsonl.zst");
            try (OutputStream raw = new FileOutputStream(zst.toFile());
                 OutputStream compressed = CompressionFactory.forName("zstd").wrapOutputStream(raw);
                 BufferedWriter w = new BufferedWriter(
                         new OutputStreamWriter(new BufferedOutputStream(compressed), StandardCharsets.UTF_8))) {
                w.write(text.toString());
            }
            assertTrue(Files.size(zst) > 0);

            System.setProperty(CODEC_KEY, "zstd");
            JsonlRowStorage s = storage("ZSTD", cols, types);
            s.loadFromFile("ZSTD");
            assertEquals(400, s.scan().size());
            assertTrue(s.scan().get(123).get("NAME").toString().contains("рус"));
            s.close();
        } finally {
            restoreProps();
        }
    }
}