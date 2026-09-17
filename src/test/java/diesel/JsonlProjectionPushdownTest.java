package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 55: JSONL projection pushdown.
 *
 * <p>When a FLATTEN reader is restricted to a subset of columns,
 * {@link JsonlRowReader#setProjection} arms either the top-level parser or the
 * FLATTEN walker to {@code skipValue()} whole unrequested sub-trees without
 * materialising them. These tests pin the parsed/skipped counters (a 3-of-50
 * projection must parse only the requested fields), prove projected values stay
 * identical to a full read, that {@link JsonlRowReader#nextArray()} always
 * parses full rows, that the rich-json path is untouched, and measure the
 * end-to-end storage speed-up as a {@code @LargeTest} (20k rows x 40 cols).
 */
@Tag("storage")
class JsonlProjectionPushdownTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty("jsonl.lazy.blocks");
    }

    // 40 plain columns + 10 FLATTEN leaves under "user".
    private static final int PLAIN_COLS = 40;

    private static List<String> flatColumns() {
        List<String> c = new ArrayList<>();
        for (int i = 0; i < PLAIN_COLS; i++) {
            c.add("C" + String.format("%02d", i));
        }
        c.add("user.address.city");
        c.add("user.address.street");
        c.add("user.address.zip");
        c.add("user.contact.email");
        c.add("user.contact.phone");
        c.add("user.contact.website");
        c.add("user.profile.bio");
        c.add("user.profile.avatar");
        c.add("user.privacy.public");
        c.add("user.metrics.score");
        return c;
    }

    private static Map<String, Class<?>> flatTypes() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        for (int i = 0; i < PLAIN_COLS; i++) {
            t.put("C" + String.format("%02d", i), Long.class);
        }
        t.put("user.address.city", String.class);
        t.put("user.address.street", String.class);
        t.put("user.address.zip", String.class);
        t.put("user.contact.email", String.class);
        t.put("user.contact.phone", String.class);
        t.put("user.contact.website", String.class);
        t.put("user.profile.bio", String.class);
        t.put("user.profile.avatar", String.class);
        t.put("user.privacy.public", Boolean.class);
        t.put("user.metrics.score", Long.class);
        return t;
    }

    private static String richRow(int n) {
        StringBuilder s = new StringBuilder(256);
        s.append('{');
        for (int i = 0; i < PLAIN_COLS; i++) {
            s.append("\"C").append(String.format("%02d", i)).append("\":").append(n + i).append(',');
        }
        s.append("\"user\":{")
                .append("\"address\":{\"city\":\"City").append(n)
                .append("\",\"street\":\"St").append(n)
                .append("\",\"zip\":\"Z").append(n).append("\"},")
                .append("\"contact\":{\"email\":\"e").append(n).append("@x.com")
                .append("\",\"phone\":\"+").append(n)
                .append("\",\"website\":\"w").append(n).append("\"},")
                .append("\"profile\":{\"bio\":\"b").append(n)
                .append("\",\"avatar\":\"a").append(n).append("\"},")
                .append("\"privacy\":{\"public\":").append((n & 1) == 0).append("},")
                .append("\"metrics\":{\"score\":").append(n * 7).append("}")
                .append("}}\n");
        return s.toString();
    }

    private static String richContent(int rows) {
        StringBuilder sb = new StringBuilder(rows * 256);
        for (int n = 1; n <= rows; n++) {
            sb.append(richRow(n));
        }
        return sb.toString();
    }

    private JsonlRowReader flattReader(String content) {
        JsonParserConfig config = JsonParserConfig.builder().build();
        JsonlSchemaManager schema = new JsonlSchemaManager(flatColumns(), flatTypes(), config);
        return new JsonlRowReader(new BufferedReader(new StringReader(content)), schema, "rich.jsonl", config);
    }

    private void write(String table, String content) throws Exception {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    // ── Pushdown allocations & counters ───────────────────────────────────

    @Test
    void pushdownParsesOnlyRequestedFieldsAndSkipsWholeSubtrees() throws Exception {
        String content = richContent(200);
        try (JsonlRowReader full = flattReader(content);
             JsonlRowReader proj = flattReader(content)) {

            while (full.hasNext()) {
                full.next();
            }
            long fullParsed = full.getParsedFieldCount();

            List<String> items = List.of("C02", "C05", "C39", "user.contact.email");
            proj.setProjection(items);
            assertEquals(items, proj.getProjectionItems());

            for (int n = 1; n <= 200; n++) {
                Object[] row = proj.nextProjected();
                assertEquals(n + 2L, row[0], "C02");
                assertEquals(n + 5L, row[1], "C05");
                assertEquals(n + 39L, row[2], "C39");
                assertEquals("e" + n + "@x.com", row[3], "nested leaf must still be parsed");
            }
            assertThrows(NoSuchElementException.class, proj::nextProjected);

            long parsed = proj.getParsedFieldCount();
            long skipped = proj.getSkippedFieldCount();
            // 50 fields/row; only 4 are requested -> ~200*4 parsed, ~200*46 skipped.
            assertEquals(4L * 200, parsed, "exactly the requested fields are parsed");
            assertTrue(skipped > parsed, "the bulk of the row must be token-skipped");
            assertTrue(parsed < fullParsed / 10,
                    "a 4-of-50 projection must parse a fraction of the full read (parsed=" + parsed
                            + ", full=" + fullParsed + ")");
        }
    }

    @Test
    void pushdownEqualsFullReadOnRequestedItems() throws Exception {
        String content = richContent(95);
        List<String> items = List.of("user.address.city", "C07", "user.metrics.score", "C39");

        List<Map<String, Object>> expected = new ArrayList<>();
        try (JsonlRowReader full = flattReader(content)) {
            full.setProjection(items);
            for (int n = 1; n <= 95; n++) {
                expected.add(full.nextProjectedMap());
            }
            assertThrows(NoSuchElementException.class, full::nextProjectedMap);
        }
        assertEquals("City1", expected.get(0).get("user.address.city"));
        assertEquals(46L, expected.get(38).get("C07"));
        assertEquals(40L, expected.get(0).get("C39"));

        try (JsonlRowReader proj = flattReader(content)) {
            proj.setProjection(items);
            for (int n = 1; n <= 95; n++) {
                assertEquals(expected.get(n - 1), proj.nextProjectedMap(),
                        "pushdown projection must equal a full-read projection on row " + n);
            }
        }
        assertNotNull(expected);
    }

    @Test
    void nextProjectedMatchesClassicProjectionAndScanForRichFlattenRows() throws Exception {
        String table = "RICH";
        write(table, richContent(120));
        List<String> items = List.of("user.address.city", "C00", "C01", "user.metrics.score");

        JsonlRowStorage storage = new JsonlRowStorage(table, flatColumns(), flatTypes(),
                JsonParserConfig.builder().build());
        storage.setDataDir(tempDir.toString());
        storage.open();
        try {
            storage.loadFromFile(table);
            List<Map<String, Object>> projected = storage.readProjected(items);
            assertEquals(120, projected.size());
            Map<String, Object> first = projected.get(0);
            assertEquals("City1", first.get("user.address.city"));
            assertEquals(1L, first.get("C00"));
            assertEquals(2L, first.get("C01"));
            assertEquals(7L, first.get("user.metrics.score"));

            Map<String, Object> fullRow = storage.scan().get(4);
            for (String item : items) {
                assertEquals(fullRow.get(item), projected.get(4).get(item),
                        "projection subset must agree with the full scan on " + item);
            }
            assertEquals(5L + 39L, fullRow.get("C39"), "row 5 (index 4) of the scan carries its full column set");
        } finally {
            storage.close();
        }
    }

    @Test
    void nextArrayAlwaysParsesFullRowsEvenAfterProjection() throws Exception {
        String content = richContent(30);
        try (JsonlRowReader reader = flattReader(content)) {
            reader.setProjection(List.of("C02"));
            Object[] row = reader.nextArray();
            assertEquals(50, row.length, "nextArray must materialise the whole row");
            assertEquals(3L, row[2], "full-row slots keep their physical column order");
            assertEquals("City1", row[40], "leaf 40 is the first user leaf after the 40 plain columns");
            assertEquals(50, reader.getParsedFieldCount(),
                    "nextArray parsing must not be reduced by the projection (one full row parsed)");
        }
    }

    @Test
    void richJsonColumnModeIsNotAffectedByPushdown() throws Exception {
        List<String> cols = List.of("ID", "DATA");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "DATA", String.class);
        String content = "{\"ID\":1,\"DATA\":{\"user\":{\"address\":{\"city\":\"Moscow\"}}},\"EXTRA\":5}\n";
        JsonParserConfig config = JsonParserConfig.builder()
                .nestedMode(JsonParserConfig.NestedMode.JSON_COLUMN)
                .build();
        try (JsonlRowReader reader = new JsonlRowReader(
                new BufferedReader(new StringReader(content)),
                new JsonlSchemaManager(cols, types, config),
                "json.jsonl", config)) {
            reader.setProjection(List.of("DATA.user.address.city", "ID"));
            Object[] row = reader.nextProjected();
            assertEquals("Moscow", row[0]);
            assertEquals(1L, row[1]);
            // JSON_COLUMN path resolves dot paths through extractPathValue: only the two
            // requested fields exist, one top-level EXTRA is skipped.
            assertEquals(2, reader.getParsedFieldCount());
            assertEquals(1, reader.getSkippedFieldCount(), "EXTRA skipped, no extra allocation");
        }
    }

    @Test
    void strictModeStillRejectsUnknownScalarUnderProjection() throws Exception {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "NAME", String.class);
        // EXTRA* are plain columns not present in type map (unknown top-level).
        String content = "{\"ID\":1,\"NAME\":\"A\",\"EXTRA\":5}\n";
        JsonParserConfig config = JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .build();
        try (JsonlRowReader proj = new JsonlRowReader(
                new BufferedReader(new StringReader(content)),
                new JsonlSchemaManager(cols, types, config),
                "strict.jsonl", config)) {
            proj.setProjection(List.of("ID", "NAME"));
            assertThrows(DieselIOException.class, proj::nextProjected,
                    "unknown scalar fields must surface exactly like a full strict read");
        }
    }

    // ── End-to-end faster-than-2x (heavy) ─────────────────────────────────

    @LargeTest
    @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
    @Test
    void projectedReadIsAtLeastTwiceAsFastAsFullLoad() throws Exception {
        String table = "PERF";
        StringBuilder file = new StringBuilder(20000 * 300);
        for (int n = 1; n <= 20000; n++) {
            file.append("{\"ID\":").append(n);
            for (int c = 0; c < 40; c++) {
                file.append(",\"C").append(String.format("%02d", c)).append("\":").append(n + c);
            }
            file.append(",\"NAME\":\"user-").append(n).append("-with-a-reasonably-long-name\"}\n");
        }
        write(table, file.toString());

        List<String> cols = new ArrayList<>();
        Map<String, Class<?>> types = new LinkedHashMap<>();
        cols.add("ID");
        types.put("ID", Long.class);
        cols.add("NAME");
        types.put("NAME", String.class);
        for (int c = 0; c < 40; c++) {
            String key = "C" + String.format("%02d", c);
            cols.add(key);
            types.put(key, Long.class);
        }

        List<String> projected = List.of("C02", "C05", "C39");

        long fastestClassic = Long.MAX_VALUE;
        long fastestProjected = Long.MAX_VALUE;
        for (int round = 0; round < 2; round++) {
            fastestClassic = Math.min(fastestClassic, timeClassicLoad(table, cols, types));
            fastestProjected = Math.min(fastestProjected, timeProjectedRead(table, cols, types, projected));
        }

        assertTrue(fastestProjected > 0, "projected read must materialise rows");
        double speedup = (double) fastestClassic / fastestProjected;
        System.out.printf("[JSONL-LAZY] perf full=%dms projected=%dms speedup=%.2fx%n",
                fastestClassic, fastestProjected, speedup);
        assertTrue(speedup >= 2.0,
                "SELECT of 3 of 40 columns must be at least 2x faster (speedup=" + speedup + ")");
    }

    private long timeClassicLoad(String table, List<String> cols, Map<String, Class<?>> types) throws Exception {
        long start = System.nanoTime();
        JsonlRowStorage storage = new JsonlRowStorage(table, cols, types, JsonParserConfig.builder().build());
        storage.setDataDir(tempDir.toString());
        storage.open();
        try {
            storage.loadFromFile(table);
            assertEquals(20000, storage.scan().size());
        } finally {
            storage.close();
        }
        return (System.nanoTime() - start) / 1_000_000L;
    }

    private long timeProjectedRead(String table, List<String> cols, Map<String, Class<?>> types,
                                   List<String> projected) throws Exception {
        long start = System.nanoTime();
        JsonlRowStorage storage = new JsonlRowStorage(table, cols, types,
                JsonParserConfig.builder().lazyBlocks(true).build());
        storage.setDataDir(tempDir.toString());
        storage.open();
        try {
            storage.loadFromFile(table);
            assertEquals(20000, storage.readProjected(projected).size());
        } finally {
            storage.close();
        }
        return (System.nanoTime() - start) / 1_000_000L;
    }
}