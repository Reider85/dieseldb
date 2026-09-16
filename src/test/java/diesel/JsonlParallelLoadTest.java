package diesel;

import ch.qos.logback.classic.Level;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.JsonlRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Prompt 54: JSONL parallel reading through a byte-offset pre-scan.
 *
 * <p>The pre-scan ({@code JsonlParallelLoader.preScan}) records the offset and
 * physical line number of every non-blank line in one byte pass, detects a
 * UTF-8 BOM and blank lines, and is cached by the owning manager until the
 * file's mtime/size changes. Reads are then split on data-line boundaries and
 * merged in file order. These tests force either mode through the
 * {@code jsonl.parallel.read.threshold} system property (a low value forces the
 * parallel path, a value above the row count forces the sequential fall-back)
 * and prove the two paths produce identical rows, presence flags and file:line
 * diagnostics, that the merge order is deterministic, that compressed files
 * stay sequential, and - as a {@link LargeTest} - that a 1M-line plain file
 * loads faster in parallel on a multi-core machine.
 */
@Tag("storage")
class JsonlParallelLoadTest {

    private static final String THRESHOLD_KEY = "jsonl.parallel.read.threshold";
    private static final String CODEC_KEY = "jsonl.compression.codec";

    @TempDir
    Path tempDir;

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static JsonParserConfig strict() {
        return JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT).build();
    }

    @AfterEach
    void clearProperties() {
        System.clearProperty(THRESHOLD_KEY);
        System.clearProperty(CODEC_KEY);
    }

    private JsonlRowStorage load(String table, JsonParserConfig config) throws IOException {
        JsonlRowStorage storage = new JsonlRowStorage(table, COLS, types(), config);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile(table);
        return storage;
    }

    private static void assertRowsEqual(List<Object[]> expected, List<Object[]> actual) {
        assertEquals(expected.size(), actual.size(), "row count");
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "row " + i);
        }
    }

    private static void assertPresenceEqual(List<boolean[]> expected, List<boolean[]> actual) {
        assertEquals(expected.size(), actual.size(), "presence count");
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "presence " + i);
        }
    }

    /** Writes a plain UTF-8 JSONL file (exact bytes, so \r\n / lone \r survive). */
    private void write(String table, String content) throws IOException {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    // ── Equality: parallel == sequential ─────────────────────────────

    @Test
    void parallelMatchesSequentialOnRichContent() throws Exception {
        String table = "RICH";
        StringBuilder sb = new StringBuilder();
        sb.append("\uFEFF{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":25}\n");
        sb.append("\n");
        sb.append("   \t \n");
        sb.append("{\"ID\":2,\"NAME\":\"Bob\",\"AGE\":30}\r\n");
        sb.append("{\"ID\":3,\"NAME\":\"Carol\"}\n");
        sb.append("{\"ID\":4,\"NAME\":null,\"AGE\":40}\r");
        sb.append("{\"ID\":5,\"NAME\":\"Ünïcødé 中 😀\",\"AGE\":50}\n");
        for (int i = 6; i <= 5005; i++) {
            sb.append("{\"ID\":").append(i).append(",\"NAME\":\"name-").append(i)
                    .append("\",\"AGE\":").append(i % 100).append("}\n");
        }
        write(table, sb.toString());

        System.setProperty(THRESHOLD_KEY, "1");
        JsonlRowStorage parallel = load(table, strict());
        List<Object[]> parallelRows = new ArrayList<>(parallel.getInternalRows());
        List<boolean[]> parallelPresence = new ArrayList<>(parallel.getRowPresence());
        parallel.close();

        System.clearProperty(THRESHOLD_KEY);
        System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
        JsonlRowStorage sequential = load(table, strict());
        List<Object[]> sequentialRows = new ArrayList<>(sequential.getInternalRows());
        List<boolean[]> sequentialPresence = new ArrayList<>(sequential.getRowPresence());
        sequential.close();

        assertEquals(5005, parallelRows.size(), "blank lines / BOM must not shift the row count");
        assertRowsEqual(sequentialRows, parallelRows);
        assertPresenceEqual(sequentialPresence, parallelPresence);
        assertEquals(25, parallelRows.get(0)[2]);
        assertEquals("Ünïcødé 中 😀", parallelRows.get(4)[1]);
    }

    @Test
    void parallelLoadIsDeterministicAndKeepsFileOrder() throws Exception {
        String table = "DET";
        StringBuilder sb = new StringBuilder();
        int n = 7000;
        for (int i = 1; i <= n; i++) {
            sb.append("{\"ID\":").append(i).append(",\"NAME\":\"n").append(i)
                    .append("\",\"AGE\":").append(i % 97).append("}\n");
        }
        write(table, sb.toString());

        System.setProperty(THRESHOLD_KEY, "1");
        List<Object[]> first = null;
        for (int run = 0; run < 3; run++) {
            JsonlRowStorage storage = load(table, strict());
            List<Object[]> rows = new ArrayList<>(storage.getInternalRows());
            storage.close();
            if (first == null) {
                first = rows;
            } else {
                assertRowsEqual(first, rows);
            }
        }
        for (int i = 0; i < n; i++) {
            assertEquals((long) (i + 1), first.get(i)[0], "rows merge in file order at position " + i);
        }
    }

    @Test
    void skipRowDiagnosticsKeepSequentialFileLineCoordinates() throws Exception {
        String table = "SKIP";
        write(table,
                "{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":25}\n"
                        + "\n"
                        + "{\"ID\":2,\"NAME\":}\n"
                        + "{\"ID\":3,\"NAME\":\"Carol\",\"AGE\":35}\n"
                        + "{\"ID\":4,\"NAME\":\"Dave\",\"AGE\":}\n"
                        + "{\"ID\":5,\"NAME\":\"Eve\",\"AGE\":45}\n");

        JsonParserConfig skip = JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .loadErrorMode(JsonParserConfig.LoadErrorMode.SKIP_ROW).build();

        List<String> parallelWarnings;
        List<Object[]> parallelRows;
        System.setProperty(THRESHOLD_KEY, "1");
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowReader")) {
            JsonlRowStorage storage = load(table, skip);
            parallelRows = new ArrayList<>(storage.getInternalRows());
            storage.close();
            parallelWarnings = cap.eventsMatching(Level.WARN, "malformed JSONL row").stream()
                    .map(e -> e.getFormattedMessage()).sorted().toList();
        }

        List<String> sequentialWarnings;
        List<Object[]> sequentialRows;
        System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlRowReader")) {
            JsonlRowStorage storage = load(table, skip);
            sequentialRows = new ArrayList<>(storage.getInternalRows());
            storage.close();
            sequentialWarnings = cap.eventsMatching(Level.WARN, "malformed JSONL row").stream()
                    .map(e -> e.getFormattedMessage()).sorted().toList();
        }

        assertEquals(3, parallelRows.size());
        assertEquals(3, sequentialRows.size());
        assertRowsEqual(sequentialRows, parallelRows);
        assertEquals(sequentialWarnings, parallelWarnings,
                "partition readers must report the same file:line coordinates as a sequential pass");
        assertFalse(parallelWarnings.isEmpty());
        assertTrue(parallelWarnings.get(0).contains("line 3"), parallelWarnings.toString());
    }

    // ── Threshold + compressed fall-back ─────────────────────────────

    @Test
    void smallFileBelowThresholdUsesSequentialPath() throws Exception {
        String table = "SMALL";
        write(table, "{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":25}\n"
                + "{\"ID\":2,\"NAME\":\"Bob\",\"AGE\":30}\n"
                + "{\"ID\":3,\"NAME\":\"Carol\",\"AGE\":35}\n");

        System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlParallelLoader")) {
            JsonlRowStorage storage = load(table, strict());
            assertEquals(3, storage.getInternalRows().size());
            storage.close();
            assertTrue(cap.eventsMatching(Level.INFO, "JsonlParallelLoader").isEmpty(),
                    "below the threshold the sequential path must run");
        }
    }

    @Test
    void compressedFileStaysSequentialEvenWhenThresholdIsTiny() throws Exception {
        String table = "CPRESS";
        System.setProperty(CODEC_KEY, "zstd");
        JsonlRowStorage writer = new JsonlRowStorage(table, COLS, types(), strict());
        writer.setDataDir(tempDir.toString());
        writer.open();
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "name-" + i);
            r.put("AGE", i);
            writer.insert(r);
        }
        writer.saveToFile(table);
        writer.close();
        assertTrue(Files.exists(tempDir.resolve(table + ".jsonl.zst")));

        System.setProperty(THRESHOLD_KEY, "1");
        try (Slf4jLogCapture cap = new Slf4jLogCapture("diesel.storage.JsonlParallelLoader")) {
            JsonlRowStorage loaded = load(table, strict());
            assertEquals(3, loaded.getInternalRows().size());
            assertEquals("name-2", loaded.getInternalRows().get(1)[1]);
            loaded.close();
            assertTrue(cap.eventsMatching(Level.INFO, "JsonlParallelLoader").isEmpty(),
                    "compressed JSONL is frame-based and must not enter the parallel reader");
        }
    }

    // ── Nested-JSON holder marks survive the parallel load (prompt 45) ─

    @Test
    void nestedJsonHolderMarksAreCarriedBackToTheSharedSchemaManager() throws Exception {
        List<String> cols = List.of("ID", "settings");
        Map<String, Class<?>> types = Map.of("ID", Long.class, "settings", String.class);
        JsonParserConfig flatten = JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .nestedMode(JsonParserConfig.NestedMode.FLATTEN).build();

        String table = "NESTED";
        write(table, "{\"ID\":1,\"settings\":{\"notif\":true,\"theme\":\"dark\"}}\n"
                + "{\"ID\":2,\"settings\":{\"notif\":false,\"theme\":\"light\"}}\n"
                + "{\"ID\":3,\"settings\":{\"notif\":true,\"theme\":\"dark\"}}\n");

        System.setProperty(THRESHOLD_KEY, "1");
        JsonlRowStorage parallel = new JsonlRowStorage(table, cols, types, flatten);
        parallel.setDataDir(tempDir.toString());
        parallel.open();
        parallel.loadFromFile(table);
        parallel.saveToFile(table);
        parallel.close();
        String parallelSaved = Files.readString(tempDir.resolve(table + ".jsonl"));

        System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
        JsonlRowStorage sequential = new JsonlRowStorage(table, cols, types, flatten);
        sequential.setDataDir(tempDir.toString());
        sequential.open();
        sequential.loadFromFile(table);
        sequential.saveToFile(table);
        sequential.close();
        String sequentialSaved = Files.readString(tempDir.resolve(table + ".jsonl"));

        assertEquals(sequentialSaved, parallelSaved,
                "a parallel load must leave the same nested re-embedding as a sequential load");
        assertTrue(parallelSaved.contains("\"settings\":{\"notif\":true"),
                "nested holder must be re-embedded, not double-encoded as a string: " + parallelSaved);
        assertFalse(parallelSaved.contains("\"settings\":\"{"), parallelSaved);
    }

    // ── Public loader entry point ────────────────────────────────────

    @Test
    void indexManagerReturnsNullBelowThresholdAndRowsAboveIt() throws Exception {
        String table = "ENTRY";
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 1200; i++) {
            sb.append("{\"ID\":").append(i).append(",\"NAME\":\"n").append(i)
                    .append("\",\"AGE\":").append(i % 50).append("}\n");
        }
        write(table, sb.toString());
        File file = tempDir.resolve(table + ".jsonl").toFile();

        diesel.storage.JsonlIndexManager manager =
                new diesel.storage.JsonlIndexManager(table, COLS, JsonParserConfig.defaults());
        System.setProperty(THRESHOLD_KEY, Long.toString(Long.MAX_VALUE));
        assertEquals(null, manager.loadFromFileParallelArrays(file, COLS, types(), JsonParserConfig.defaults()));

        System.setProperty(THRESHOLD_KEY, "1");
        diesel.storage.JsonlParallelLoader.JsonlLoadResult result =
                manager.loadFromFileParallelArrays(file, COLS, types(), JsonParserConfig.defaults());
        assertEquals(1200, result.rows().size());
        assertEquals("n1", result.rows().get(0)[1]);
        assertEquals("n1200", result.rows().get(1199)[1]);
    }

    // ── 1M-line acceptance (heavy: runs only under -Pdiesel.largeTests) ─

    @LargeTest
    void oneMillionPlainRowsLoadFasterInParallel() throws Exception {
        assumeTrue(Runtime.getRuntime().availableProcessors() >= 2,
                "parallel speed-up needs at least 2 cores");

        String table = "BIG1M";
        Path file = tempDir.resolve(table + ".jsonl");
        int n = 1_000_000;
        long writeStart = System.nanoTime();
        try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 1; i <= n; i++) {
                bw.append("{\"ID\":").append(Integer.toString(i))
                        .append(",\"NAME\":\"name-").append(Integer.toString(i))
                        .append("\",\"AGE\":").append(Integer.toString(i % 100)).append("}\n");
            }
        }
        long writeMs = (System.nanoTime() - writeStart) / 1_000_000;

        long sequentialMs = bestLoad(table, Long.MAX_VALUE, n);
        long parallelMs = bestLoad(table, 1L, n);

        System.out.println("[JSONL-PARALLEL] seq=" + sequentialMs + "ms par=" + parallelMs
                + "ms rows=" + n + " write=" + writeMs + "ms cores="
                + Runtime.getRuntime().availableProcessors());

        assertTrue(parallelMs < sequentialMs,
                "parallel load of " + n + " rows must beat sequential: seq=" + sequentialMs
                        + "ms par=" + parallelMs + "ms");
    }

    private long bestLoad(String table, long threshold, int expectedRows) throws IOException {
        System.setProperty(THRESHOLD_KEY, Long.toString(threshold));
        long best = Long.MAX_VALUE;
        for (int run = 0; run < 2; run++) {
            JsonlRowStorage storage = new JsonlRowStorage(table, COLS, types(), strict());
            storage.setDataDir(tempDir.toString());
            storage.open();
            long start = System.nanoTime();
            storage.loadFromFile(table);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            assertEquals(expectedRows, storage.getInternalRows().size());
            assertEquals(1L, storage.getInternalRows().get(0)[0]);
            assertEquals((long) expectedRows, storage.getInternalRows().get(expectedRows - 1)[0]);
            best = Math.min(best, elapsed);
            storage.close();
        }
        return best;
    }
}
