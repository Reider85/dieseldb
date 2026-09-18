package diesel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import diesel.storage.AtomicFileWriter;
import diesel.storage.CompressionFactory;
import diesel.storage.CsvRowStorage;
import diesel.storage.JsonlRowStorage;
import diesel.storage.JsonlSchemaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 56 (§1b) JSONL negative-scenario, semantic and benchmark tests. The
 * JSONL counterpart of {@link CsvStorageAdvancedTest} (prompt 38): each test
 * anchors the storage-level behaviour of a prior JSONL prompt - duplicate keys
 * (43), 2^53 precision guard (43), schema inference type-change rejection (44),
 * depth-3 nested round trips and dot-path SQL in both nested modes (45),
 * crash-safe rewrite and orphan .tmp (49/30), the three NULL states
 * null / "" / missing across repeated round trips (47), malformed-row
 * diagnostics with rollback (48) - and finally the cross-format quality-gate
 * benchmarks (1M rows CSV vs JSONL vs JSONL+zstd, memory per row + projection
 * on wide rows, append vs rewrite on 10k inserts) run as {@link LargeTest}s.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlStorageAdvancedTest {

    @TempDir
    Path tempDir;

    private static final long ROWS_1M = 1_000_000;

    private static final String[] JSONL_PROP_KEYS = {
            "jsonl.duplicate.keys",
            "jsonl.type.coercion",
            "jsonl.schema.mode",
            "jsonl.nested.mode",
            "jsonl.array.columns",
            "jsonl.missing.field",
            "jsonl.load.error.mode",
            "jsonl.write.mode",
            "jsonl.compaction.threshold",
            "jsonl.load.mode",
            "jsonl.table.mirror",
            "jsonl.compression.codec",
            "jsonl.parallel.read.threshold",
            "jsonl.lazy.blocks",
            "csv.load.mode",
            "csv.table.mirror"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveConfig() {
        for (String key : JSONL_PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreConfig() {
        for (Map.Entry<String, String> e : prevProps.entrySet()) {
            if (e.getValue() == null) {
                System.clearProperty(e.getKey());
            } else {
                System.setProperty(e.getKey(), e.getValue());
            }
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────

    private static List<String> cols(String... names) {
        return List.of(names);
    }

    private static Map<String, Class<?>> typed(String... pairs) {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            t.put(pairs[i], switch (pairs[i + 1]) {
                case "Long" -> Long.class;
                case "Integer" -> Integer.class;
                case "Boolean" -> Boolean.class;
                case "BigDecimal" -> java.math.BigDecimal.class;
                case "Double" -> Double.class;
                default -> String.class;
            });
        }
        return t;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    /** New storage (3-arg ctor picks up the current jsonl.* system properties). */
    private JsonlRowStorage jsonl(String table, List<String> schema, Map<String, Class<?>> types) {
        JsonlRowStorage storage = new JsonlRowStorage(table, schema, types);
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private void write(String table, String content) throws Exception {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    /** Flattened message text of an exception including every cause. */
    private static String messageChain(Throwable ex) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = ex; t != null; t = t.getCause()) {
            sb.append(t.getMessage()).append('\n');
        }
        return sb.toString();
    }

    private static final class Exploding {
        @Override
        public String toString() {
            throw new IllegalStateException("boom");
        }
    }

    // ─── Prompt 43/48: duplicate keys at storage level ───────────────

    @Test
    void duplicateKeysFailModeRejectsLoadAndRollsBack() throws Exception {
        System.setProperty("jsonl.duplicate.keys", "fail");
        write("DUPF",
                "{\"ID\":1,\"NAME\":\"Alice\",\"ID\":2}\n");

        JsonlRowStorage storage = jsonl("DUPF", cols("ID", "NAME"),
                typed("ID", "Long", "NAME", "String"));
        storage.insert(map("ID", 99L, "NAME", "keep"));

        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage.loadFromFile("DUPF"));
        String chain = messageChain(ex);
        assertTrue(chain.contains(tempDir.resolve("DUPF.jsonl").toString()),
                "diagnostics must name the file: " + chain);
        assertTrue(chain.toLowerCase(Locale.ROOT).contains("duplic"),
                "diagnostics must mention the duplicate key: " + chain);

        assertEquals(1, storage.scan().size(), "failed load must roll back to the previous rows");
        assertEquals("keep", storage.scan().get(0).get("NAME"));
        storage.close();
    }

    @Test
    void duplicateKeysLastWinsKeepsSecondValue() throws Exception {
        System.setProperty("jsonl.duplicate.keys", "last_wins");
        write("DUPL",
                "{\"ID\":1,\"NAME\":\"Alice\",\"ID\":2}\n");

        JsonlRowStorage storage = jsonl("DUPL", cols("ID", "NAME"),
                typed("ID", "Long", "NAME", "String"));
        storage.loadFromFile("DUPL");

        assertEquals(1, storage.scan().size());
        assertEquals(2L, storage.scan().get(0).get("ID"), "last value must win");
        assertEquals("Alice", storage.scan().get(0).get("NAME"));
        storage.close();
    }

    // ─── Prompt 43: 2^53 precision guard at storage level ────────────

    @Test
    void integerPastTwo53RejectedIntoDoubleButReadsExactInLong() throws Exception {
        System.setProperty("jsonl.schema.mode", "strict");
        write("PREC",
                "{\"ID\":1,\"VAL\":9007199254740993}\n");

        JsonlRowStorage dbl = jsonl("PREC", cols("ID", "VAL"),
                typed("ID", "Long", "VAL", "Double"));
        dbl.insert(map("ID", 7L, "VAL", 1.5));
        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> dbl.loadFromFile("PREC"),
                "9007199254740993 must be rejected in a DOUBLE column");
        String chain = messageChain(ex);
        assertTrue(chain.contains("9007199254740993"),
                "diagnostics must name the offending value: " + chain);
        assertEquals(1, dbl.scan().size(), "failed load must roll back");
        assertEquals(7L, dbl.scan().get(0).get("ID"));
        dbl.close();

        JsonlRowStorage lng = jsonl("PREC", cols("ID", "VAL"),
                typed("ID", "Long", "VAL", "Long"));
        lng.loadFromFile("PREC");
        assertEquals(9007199254740993L, lng.scan().get(0).get("VAL"),
                "the same literal must read exactly in a LONG column");
        lng.close();
    }

    // ─── Prompt 44: type change between rows in HYBRID mode ──────────

    @Test
    void hybridTypeChangeBetweenRowsFailsWithFieldDiagnosticsAndRollsBack() throws Exception {
        System.setProperty("jsonl.schema.mode", "hybrid");
        write("TYPECH",
                "{\"SCORE\":5}\n"
                        + "{\"SCORE\":\"abc\"}\n");

        JsonlRowStorage storage = jsonl("TYPECH", cols("SCORE"),
                typed("SCORE", "Long"));
        storage.insert(map("SCORE", 99L));

        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage.loadFromFile("TYPECH"),
                "a field changing type between rows must abort the hybrid load");
        String chain = messageChain(ex);
        assertTrue(chain.contains("SCORE"),
                "diagnostics must name the changing field: " + chain);
        assertTrue(chain.toLowerCase(Locale.ROOT).contains("type"),
                "diagnostics must explain the type change: " + chain);

        assertEquals(1, storage.scan().size(), "failed load must roll back");
        assertEquals(99L, storage.scan().get(0).get("SCORE"));
        storage.close();
    }

    // ─── Prompt 48: malformed / truncated files at storage level ─────

    @Test
    void zeroByteFileLoadsEmpty() throws Exception {
        Files.createFile(tempDir.resolve("EMPTY.jsonl"));

        JsonlRowStorage storage = jsonl("EMPTY", cols("ID", "NAME"),
                typed("ID", "Long", "NAME", "String"));
        storage.loadFromFile("EMPTY");

        assertTrue(storage.scan().isEmpty(), "a zero-byte file must load as an empty table");
        storage.close();
    }

    @Test
    void orphanTmpWarnsOnLoadWhenJsonlMissing() throws Exception {
        Path jsonl = tempDir.resolve("ORPHAN.jsonl");
        Files.createFile(AtomicFileWriter.tmpPath(jsonl));
        List<String> schema = cols("ID", "NAME");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String");

        try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.AtomicFileWriter")) {
            JsonlRowStorage storage = jsonl("ORPHAN", schema, types);
            storage.loadFromFile("ORPHAN"); // target missing -> must not throw
            assertTrue(storage.scan().isEmpty());
            storage.close();

            List<ILoggingEvent> warnings = capture.eventsMatching(Level.WARN, "Interrupted write");
            assertEquals(1, warnings.size(),
                    "expected exactly one interrupted-write WARNING: " + capture.events());
        }
    }

    @Test
    void corruptBinaryMidFileFailsAndRollsBack() throws Exception {
        byte[] head = "{\"ID\":1,\"NAME\":\"Alice\"}\n".getBytes(StandardCharsets.UTF_8);
        byte[] garbage = {0x00, (byte) 0xFF, (byte) 0xFE, 0x01, 0x02};
        byte[] tail = "{\"ID\":3,\"NAME\":\"Carol\"}\n".getBytes(StandardCharsets.UTF_8);
        byte[] content = new byte[head.length + garbage.length + tail.length];
        System.arraycopy(head, 0, content, 0, head.length);
        System.arraycopy(garbage, 0, content, head.length, garbage.length);
        System.arraycopy(tail, 0, content, head.length + garbage.length, tail.length);
        Files.write(tempDir.resolve("CORRUPT.jsonl"), content);

        JsonlRowStorage storage = jsonl("CORRUPT", cols("ID", "NAME"),
                typed("ID", "Long", "NAME", "String"));
        storage.insert(map("ID", 99L, "NAME", "keep"));

        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage.loadFromFile("CORRUPT"));
        assertTrue(messageChain(ex).contains(tempDir.resolve("CORRUPT.jsonl").toString()),
                "diagnostics must name the corrupt file: " + messageChain(ex));

        assertEquals(1, storage.scan().size(), "failed load must roll back");
        assertEquals("keep", storage.scan().get(0).get("NAME"));
        storage.close();
    }

    // ─── Prompt 47: null / "" / missing distinct across round trips ──

    @Test
    void nullVsEmptyVsMissingStayDistinctAcrossRepeatedRoundTrips() throws Exception {
        System.setProperty("jsonl.schema.mode", "strict");
        System.setProperty("jsonl.missing.field", "default");
        List<String> schema = cols("ID", "NAME", "NOTE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "NOTE", "String");
        write("THREEST",
                "{\"ID\":1,\"NAME\":null,\"NOTE\":\"\"}\n"
                        + "{\"ID\":2,\"NAME\":\"\"}\n"
                        + "{\"ID\":3}\n");

        JsonlRowStorage storage = jsonl("THREEST", schema, types);
        storage.loadFromFile("THREEST");

        List<Map<String, Object>> rows = storage.scan();
        assertEquals(3, rows.size());
        assertNull(rows.get(0).get("NAME"));
        assertEquals("", rows.get(0).get("NOTE"));
        assertEquals("", rows.get(1).get("NAME"));
        assertNull(rows.get(1).get("NOTE"));
        assertNull(rows.get(2).get("NAME"));
        assertNull(rows.get(2).get("NOTE"));

        List<boolean[]> presence = storage.getRowPresence();
        assertArrayEquals(new boolean[]{true, true, true}, presence.get(0));
        assertArrayEquals(new boolean[]{true, true, false}, presence.get(1));
        assertArrayEquals(new boolean[]{true, false, false}, presence.get(2));

        // First save materialises the three states on disk.
        storage.saveToFile("THREEST");
        Path file = tempDir.resolve("THREEST.jsonl");
        String v1 = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        assertTrue(v1.contains("\"NAME\":null"), "explicit null must be written as JSON null: " + v1);
        assertTrue(v1.contains("\"NOTE\":\"\""), "empty string must be written as \"\": " + v1);
        assertTrue(v1.endsWith("{\"ID\":3}\n"), "absent keys must stay omitted: " + v1);
        storage.close();

        // Reload and save again: the second write must be byte-identical.
        JsonlRowStorage reloaded = jsonl("THREEST", schema, types);
        reloaded.loadFromFile("THREEST");
        reloaded.saveToFile("THREEST");
        reloaded.close();
        String v2 = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        assertEquals(v1, v2, "repeated load->save must be byte-identical");
    }

    // ─── Prompt 45: depth-3 nesting + arrays in both nested modes ────

    @Test
    void depth3NestedWithArraysRoundTripInFlattenExpandMode() throws Exception {
        System.setProperty("jsonl.nested.mode", "flatten");
        System.setProperty("jsonl.array.columns", "expand");
        List<String> schema = cols("ID", "user.name", "user.address.city", "user.address.zip",
                "user.tags[0]", "user.tags[1]");
        Map<String, Class<?>> types = typed("ID", "Long",
                "user.name", "String", "user.address.city", "String",
                "user.address.zip", "Integer", "user.tags[0]", "String", "user.tags[1]", "String");

        JsonlRowStorage storage = jsonl("FLAT3", schema, types);
        storage.insert(map("ID", 1L, "user.name", "Alice", "user.address.city", "Moscow",
                "user.address.zip", 101101, "user.tags[0]", "a", "user.tags[1]", "b"));
        storage.insert(map("ID", 2L, "user.name", "Bob", "user.address.city", "Paris",
                "user.address.zip", 75000, "user.tags[0]", "c", "user.tags[1]", "d"));
        storage.saveToFile("FLAT3");

        // The writer must reconstruct a real nested object, not quote the dots.
        String raw = new String(Files.readAllBytes(tempDir.resolve("FLAT3.jsonl")), StandardCharsets.UTF_8);
        assertTrue(raw.contains("\"city\":\"Moscow\""), "nested leaf must be re-embedded: " + raw);
        assertTrue(raw.contains("\"tags\":[\"a\",\"b\"]"), "index columns must rebuild the array: " + raw);

        JsonlRowStorage loaded = jsonl("FLAT3", schema, types);
        loaded.loadFromFile("FLAT3");

        List<Map<String, Object>> back = loaded.scan();
        assertEquals(2, back.size());
        assertEquals("Moscow", back.get(0).get("user.address.city"));
        assertEquals(101101, back.get(0).get("user.address.zip"));
        assertEquals("a", back.get(0).get("user.tags[0]"));
        assertEquals("b", back.get(0).get("user.tags[1]"));
        assertEquals("Paris", back.get(1).get("user.address.city"));
        storage.close();
        loaded.close();
    }

    @Test
    void depth3NestedWithArraysRoundTripInJsonColumnMode() throws Exception {
        List<String> schema = cols("ID", "PROFL");
        Map<String, Class<?>> types = typed("ID", "Long", "PROFL", "String");

        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("user", map("name", "Alice",
                "address", map("city", "Moscow", "zip", 101101),
                "tags", List.of("a", "b")));

        JsonlRowStorage storage = jsonl("JSON3", schema, types);
        storage.insert(map("ID", 1L, "PROFL", inner));
        storage.saveToFile("JSON3");
        storage.close();

        // The nested structure must be embedded, not double-encoded.
        String raw0 = new String(Files.readAllBytes(tempDir.resolve("JSON3.jsonl")), StandardCharsets.UTF_8);
        assertTrue(raw0.contains("\"user\":{\"address\""),
                "json_column mode must embed the structure: " + raw0);

        JsonlRowStorage loaded = jsonl("JSON3", schema, types);
        loaded.loadFromFile("JSON3");
        String text = String.valueOf(loaded.scan().get(0).get("PROFL"));

        JsonlSchemaManager manager = new JsonlSchemaManager(schema, types,
                diesel.storage.json.JsonParserConfig.builder()
                        .nestedMode(diesel.storage.json.JsonParserConfig.NestedMode.JSON_COLUMN).build());
        JsonlSchemaManager.ProjectionSlot city = manager.resolveProjectionItem("PROFL.user.address.city");
        assertNotNull(city, "the address leaf must be addressable by dot path");
        assertEquals("Moscow", manager.extractPathValue(text, city.segments()));
        JsonlSchemaManager.ProjectionSlot tags = manager.resolveProjectionItem("PROFL.user.tags");
        assertTrue(String.valueOf(manager.extractPathValue(text, tags.segments())).startsWith("["),
                "arrays must survive as JSON structure: " + text);

        // A second load->save cycle must stay byte-identical (deterministic capture).
        loaded.saveToFile("JSON3");
        String v1 = new String(Files.readAllBytes(tempDir.resolve("JSON3.jsonl")), StandardCharsets.UTF_8);
        loaded.close();
        JsonlRowStorage again = jsonl("JSON3", schema, types);
        again.loadFromFile("JSON3");
        again.saveToFile("JSON3");
        again.close();
        String v2 = new String(Files.readAllBytes(tempDir.resolve("JSON3.jsonl")), StandardCharsets.UTF_8);
        assertEquals(v1, v2, "json_column nested round trip must be deterministic");
    }

    @Test
    void sqlDottedLeafWhereAndProjectionAtDepth3InFlattenMode() {
        Database db = new Database();
        db.executeQuery("CREATE TABLE FLAT3SQ (ID LONG, \"user.name\" STRING, "
                + "\"user.address.city\" STRING, \"user.address.country\" STRING)", null);
        db.executeQuery("INSERT INTO FLAT3SQ (ID, \"user.name\", \"user.address.city\", "
                + "\"user.address.country\") VALUES (1, 'Alice', 'Moscow', 'RU')", null);
        db.executeQuery("INSERT INTO FLAT3SQ (ID, \"user.name\", \"user.address.city\", "
                + "\"user.address.country\") VALUES (2, 'Bob', 'Paris', 'FR')", null);

        List<Map<String, Object>> rows = query(db,
                "SELECT * FROM FLAT3SQ WHERE \"user.address.city\" = 'Moscow' AND \"user.address.country\" = 'RU'");
        assertEquals(1, rows.size(), "depth-3 dot-path WHERE must find the row");
        assertEquals("Alice", rows.get(0).get("user.name"));
        assertEquals("Moscow", rows.get(0).get("user.address.city"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> query(Database db, String sql) {
        return (List<Map<String, Object>>) db.executeQuery(sql, null);
    }

    // ─── Prompt 49/30: interrupted rewrite keeps the previous file ───

    @Test
    void interruptedRewriteKeepsPreviousFileAndCleansTemp() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");

        JsonlRowStorage storage = jsonl("CRASHJ", schema, types);
        storage.insert(map("ID", 1L, "NAME", "Alice", "AGE", 25));
        storage.saveToFile("CRASHJ");

        Path jsonl = tempDir.resolve("CRASHJ.jsonl");
        byte[] v1 = Files.readAllBytes(jsonl);

        storage.insert(map("ID", 2L, "NAME", new Exploding(), "AGE", 30));
        assertThrows(RuntimeException.class, () -> storage.saveToFile("CRASHJ"));

        assertArrayEquals(v1, Files.readAllBytes(jsonl),
                "previous valid file must be untouched by the failed rewrite");
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(jsonl)), "no temp file may linger");
        storage.close();
    }

    // ─── Benchmarks (quality gate, documented in KNOWN_LIMITATIONS) ──

    static long ceilingMs() {
        String override = System.getProperty("diesel.perf.ceiling");
        if (override != null) {
            try {
                return Long.parseLong(override.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return 60_000;
    }

    private static List<String> benchCols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE", "TAG");
    }

    private static Map<String, Class<?>> benchTypes() {
        return typed("ID", "Long", "NAME", "String", "AGE", "Integer", "ACTIVE", "Boolean", "TAG", "String");
    }

    private static String csvRows1M() {
        StringBuilder sb = new StringBuilder((int) (ROWS_1M * 42L));
        sb.append("ID,NAME,AGE,ACTIVE,TAG\n");
        for (int i = 0; i < ROWS_1M; i++) {
            sb.append(i).append(",User_").append(i).append(',').append(18 + (i % 80))
                    .append(',').append(i % 2 == 0).append(",tag-").append(i).append('\n');
        }
        return sb.toString();
    }

    private static String jsonlRows1M() {
        StringBuilder sb = new StringBuilder((int) (ROWS_1M * 48L));
        for (int i = 0; i < ROWS_1M; i++) {
            sb.append("{\"ID\":").append(i)
                    .append(",\"NAME\":\"User_").append(i).append('"')
                    .append(",\"AGE\":").append(18 + (i % 80))
                    .append(",\"ACTIVE\":").append(i % 2 == 0)
                    .append(",\"TAG\":\"tag-").append(i).append("\"}\n");
        }
        return sb.toString();
    }

    @LargeTest
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void benchmarkOneMillionRowsCsvVsJsonlVsJsonlZstd() throws Exception {
        long ceiling = ceilingMs();
        List<String> cols = benchCols();
        Map<String, Class<?>> types = benchTypes();

        Files.write(tempDir.resolve("BENCH_1M.csv"), csvRows1M().getBytes(StandardCharsets.UTF_8));
        Files.write(tempDir.resolve("BENCH_1M.jsonl"), jsonlRows1M().getBytes(StandardCharsets.UTF_8));
        Path zst = tempDir.resolve("BENCH_1M.jsonl.zst");
        try (OutputStream fos = new FileOutputStream(zst.toFile());
             OutputStream compressed = CompressionFactory.forName("zstd").wrapOutputStream(fos);
             BufferedWriter w = new BufferedWriter(
                     new OutputStreamWriter(new BufferedOutputStream(compressed), StandardCharsets.UTF_8))) {
            w.write(jsonlRows1M());
        }

        long tCsv;
        {
            long start = System.nanoTime();
            CsvRowStorage csv = new CsvRowStorage("BENCH_1M", cols, types);
            csv.setDataDir(tempDir.toString());
            csv.open();
            csv.loadFromFile("BENCH_1M");
            assertEquals(ROWS_1M, csv.getInternalRows().size());
            csv.close();
            tCsv = (System.nanoTime() - start) / 1_000_000L;
        }

        long tJsonl;
        {
            long start = System.nanoTime();
            JsonlRowStorage jsonl = jsonl("BENCH_1M", cols, types);
            jsonl.loadFromFile("BENCH_1M");
            assertEquals(ROWS_1M, jsonl.getInternalRows().size());
            assertEquals(42L, jsonl.getInternalRows().get(42)[0]);
            jsonl.close();
            tJsonl = (System.nanoTime() - start) / 1_000_000L;
        }

        long tZstd;
        {
            System.setProperty("jsonl.compression.codec", "zstd");
            long start = System.nanoTime();
            JsonlRowStorage jsonl = jsonl("BENCH_1M", cols, types);
            jsonl.loadFromFile("BENCH_1M");
            assertEquals(ROWS_1M, jsonl.getInternalRows().size());
            jsonl.close();
            tZstd = (System.nanoTime() - start) / 1_000_000L;
        }

        System.out.printf(Locale.ROOT,
                "[JSONL-BENCH-1M] csv=%dms jsonl=%dms jsonl+zstd=%dms rows=%d%n",
                tCsv, tJsonl, tZstd, ROWS_1M);
        assertTrue(tCsv <= ceiling, "1M CSV load exceeded the ceiling: " + tCsv + "ms");
        assertTrue(tJsonl <= ceiling, "1M JSONL load exceeded the ceiling: " + tJsonl + "ms");
        assertTrue(tZstd <= ceiling, "1M JSONL+zstd load exceeded the ceiling: " + tZstd + "ms");
    }

    @LargeTest
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void benchmarkMemoryPerRowAndProjectionOnWideRows() throws Exception {
        String table = "WIDE";
        StringBuilder content = new StringBuilder(20_000 * 300);
        for (int n = 1; n <= 20_000; n++) {
            content.append("{\"ID\":").append(n);
            for (int c = 0; c < 40; c++) {
                content.append(",\"C").append(String.format(Locale.ROOT, "%02d", c)).append("\":").append(n + c);
            }
            content.append(",\"NAME\":\"user-").append(n).append("-with-a-reasonably-long-name\"}\n");
        }
        write(table, content.toString());

        List<String> cols = new ArrayList<>();
        Map<String, Class<?>> types = new LinkedHashMap<>();
        cols.add("ID");
        types.put("ID", Long.class);
        cols.add("NAME");
        types.put("NAME", String.class);
        for (int c = 0; c < 40; c++) {
            String key = "C" + String.format(Locale.ROOT, "%02d", c);
            cols.add(key);
            types.put(key, Long.class);
        }
        List<String> projected = List.of("C02", "C05", "C39");

        gc();
        long heapBefore = usedHeap();
        long tFull;
        {
            long start = System.nanoTime();
            JsonlRowStorage storage = jsonl(table, cols, types);
            storage.loadFromFile(table);
            assertEquals(20_000, storage.scan().size());
            storage.close();
            tFull = (System.nanoTime() - start) / 1_000_000L;
        }
        gc();
        long fullBytes = (usedHeap() - heapBefore);

        gc();
        long heapProjBefore = usedHeap();
        long tProj;
        {
            System.setProperty("jsonl.lazy.blocks", "true");
            long start = System.nanoTime();
            JsonlRowStorage storage = jsonl(table, cols, types);
            storage.loadFromFile(table);
            assertEquals(20_000, storage.readProjected(projected).size());
            storage.close();
            tProj = (System.nanoTime() - start) / 1_000_000L;
        }
        gc();
        long projBytes = (usedHeap() - heapProjBefore);

        long bytesPerRow = fullBytes / 20_000L;
        double speedup = (double) tFull / tProj;
        System.out.printf(Locale.ROOT,
                "[JSONL-BENCH-WIDE] full=%dms proj=%dms speedup=%.2fx fullHeap=%d bytes/row=%d projHeap=%d%n",
                tFull, tProj, speedup, fullBytes, bytesPerRow, projBytes);
        assertTrue(tFull > 0 && tProj > 0, "measurements must be non-zero");
    }

    @LargeTest
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void benchmarkAppendVsRewriteOnTenThousandInserts() throws Exception {
        int rounds = 10;
        int perRound = 1000;
        List<String> cols = benchCols();
        Map<String, Class<?>> types = benchTypes();
        long ceiling = ceilingMs();

        long rewriteMs;
        {
            System.setProperty("jsonl.write.mode", "rewrite");
            JsonlRowStorage storage = jsonl("APPROWR", cols, types);
            long start = System.nanoTime();
            for (int r = 0; r < rounds; r++) {
                for (int i = 0; i < perRound; i++) {
                    storage.insert(insertRow(r * perRound + i));
                }
                storage.saveToFile("APPROWR");
            }
            rewriteMs = (System.nanoTime() - start) / 1_000_000L;
            storage.close();
        }

        long appendMs;
        {
            System.setProperty("jsonl.write.mode", "append");
            JsonlRowStorage storage = jsonl("APPROWA", cols, types);
            long start = System.nanoTime();
            for (int r = 0; r < rounds; r++) {
                for (int i = 0; i < perRound; i++) {
                    storage.insert(insertRow(r * perRound + i));
                }
                storage.saveToFile("APPROWA");
            }
            appendMs = (System.nanoTime() - start) / 1_000_000L;
            assertEquals(10_000, storage.scan().size());
            storage.close();
        }

        double margin = (double) rewriteMs / Math.max(1, appendMs);
        System.out.printf(Locale.ROOT,
                "[JSONL-BENCH-APPEND] rewrite=%dms append=%dms margin=%.1fx rows=10000%n",
                rewriteMs, appendMs, margin);
        assertTrue(rewriteMs <= ceiling, "rewrite save exceeded the ceiling: " + rewriteMs + "ms");
        assertTrue(appendMs <= ceiling, "append save exceeded the ceiling: " + appendMs + "ms");
        assertTrue(appendMs <= rewriteMs,
                "incremental append must not be slower than full rewrites (rewrite="
                        + rewriteMs + "ms, append=" + appendMs + "ms)");
    }

    private static Map<String, Object> insertRow(int i) {
        return map("ID", (long) i, "NAME", "User_" + i, "AGE", 18 + (i % 80),
                "ACTIVE", i % 2 == 0, "TAG", "tag-" + i);
    }

    private static long usedHeap() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private static void gc() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}