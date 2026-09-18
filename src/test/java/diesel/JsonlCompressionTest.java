package diesel;

import com.github.luben.zstd.ZstdOutputStream;
import diesel.storage.JsonlDeltaManager;
import diesel.storage.JsonlRowStorage;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 52: JSONL compression via the common {@code CompressionCodec} /
 * {@code CompressionFactory} ({@code zstd} / {@code lz4} / {@code snappy} /
 * {@code none}).
 *
 * <p>The codec and level are resolved from the configuration keys
 * {@code jsonl.compression.codec} and {@code jsonl.compression.level}
 * (system property &gt; config.properties &gt; defaults), so these tests drive
 * the storage through {@link System#setProperty}. The physical write target is
 * {@code .jsonl} for {@code none} and {@code .jsonl.zst} / {@code .jsonl.lz4} /
 * {@code .jsonl.snappy} otherwise, while reads are transparent to the file's
 * actual format (suffix detection). Append mode is not supported with
 * compression and falls back to a full rewrite.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlCompressionTest {

    @TempDir
    Path tempDir;

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");
    private static final Map<String, Class<?>> TYPES;

    static {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        TYPES = Map.copyOf(t);
    }

    @AfterEach
    void clearCompressionProperties() {
        System.clearProperty("jsonl.compression.codec");
        System.clearProperty("jsonl.compression.level");
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private static Map<String, Object> row(long id, String name, int age) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        return r;
    }

    private static void setCodec(String codec) {
        System.setProperty("jsonl.compression.codec", codec);
    }

    private JsonParserConfig strictConfig() {
        return new JsonParserConfig.Builder().schemaMode(JsonParserConfig.SchemaMode.STRICT).build();
    }

    private JsonlRowStorage newStorage(String name) {
        return newStorage(name, strictConfig());
    }

    private JsonlRowStorage newStorage(String name, JsonParserConfig config) {
        JsonlRowStorage storage = new JsonlRowStorage(name, COLS, TYPES, config);
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private Path path(String name) {
        return tempDir.resolve(name + ".jsonl");
    }

    private Path compressedPath(String name, String codec) {
        return tempDir.resolve(name + ".jsonl" + suffixFor(codec));
    }

    private static void insert(JsonlRowStorage storage, long id, String name, int age) {
        storage.insert(row(id, name, age));
    }

    private List<Map<String, Object>> loadAll(JsonlRowStorage storage) {
        return storage.scan();
    }

    // ── Round trip + physical format ────────────────────────────────

    @Test
    void jsonlRoundTripAllCodecs() throws Exception {
        for (String codec : List.of("zstd", "lz4", "snappy")) {
            clearCompressionProperties();
            setCodec(codec);
            String table = "RT_" + codec;
            JsonlRowStorage storage = newStorage(table);
            insert(storage, 1, "Alice", 25);
            insert(storage, 2, "Bob", 30);
            storage.saveToFile(table);
            Path expectedFile = compressedPath(table, codec);
            assertTrue(Files.exists(expectedFile),
                    "saving with codec " + codec + " must produce " + expectedFile.getFileName());
            assertFalse(Files.exists(path(table)),
                    "compressed save must not write a plain .jsonl");
            storage.close();

            JsonlRowStorage loaded = newStorage(table);
            loaded.loadFromFile(table);
            List<Map<String, Object>> rows = loadAll(loaded);
            assertEquals(2, rows.size());
            assertEquals(1L, rows.get(0).get("ID"));
            assertEquals("Alice", rows.get(0).get("NAME"));
            assertEquals(25, rows.get(0).get("AGE"));
            assertEquals(2L, rows.get(1).get("ID"));
            assertEquals("Bob", rows.get(1).get("NAME"));
            assertEquals(30, rows.get(1).get("AGE"));
            loaded.close();
        }
    }

    @Test
    void noneKeepsPlainFormat() throws Exception {
        clearCompressionProperties();
        setCodec("none");
        String table = "PLAIN";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        insert(storage, 2, "Bob", 30);
        storage.saveToFile(table);
        storage.close();

        String content = Files.readString(path(table));
        assertEquals("{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":25}\n"
                + "{\"ID\":2,\"NAME\":\"Bob\",\"AGE\":30}\n", content,
                "codec=none must keep the exact pre-prompt-52 plain format");
    }

    // ── Transparent reads ────────────────────────────────────────────

    @Test
    void compressedFileReadsWhenCodecIsNone() throws Exception {
        clearCompressionProperties();
        setCodec("zstd");
        String table = "CODEFREE";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        storage.saveToFile(table);
        storage.close();

        clearCompressionProperties(); // codec=none from config.properties
        JsonlRowStorage loaded = newStorage(table);
        loaded.loadFromFile(table);
        assertEquals(1, loadAll(loaded).size(), "compressed file must read transparently with codec=none");
        assertEquals("Alice", loadAll(loaded).get(0).get("NAME"));
        loaded.close();
    }

    @Test
    void plainFileReadsWhenCompressionEnabled() throws Exception {
        clearCompressionProperties();
        setCodec("none");
        String table = "PLAINREAD";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        storage.saveToFile(table);
        storage.close();

        setCodec("zstd");
        JsonlRowStorage loaded = newStorage(table);
        loaded.loadFromFile(table);
        assertEquals(1, loadAll(loaded).size(), "plain file must read transparently with codec=zstd");
        assertEquals("Alice", loadAll(loaded).get(0).get("NAME"));
        loaded.close();
    }

    @Test
    void configuredCodecSuffixWinsOverStalePlain() throws Exception {
        clearCompressionProperties();
        setCodec("zstd");
        String table = "STALEPLAIN";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        storage.saveToFile(table);
        storage.close();

        try (FileWriter fw = new FileWriter(path(table).toFile())) {
            fw.write("{\"ID\":99,\"NAME\":\"STALE\",\"AGE\":99}\n");
        }

        setCodec("zstd");
        JsonlRowStorage withCodec = newStorage(table);
        withCodec.loadFromFile(table);
        assertEquals("Alice", loadAll(withCodec).get(0).get("NAME"),
                "the configured codec's own suffixed file wins over a stale plain file");
        withCodec.close();
    }

    @Test
    void codecChangeLeavesEarlierFileReadable() throws Exception {
        clearCompressionProperties();
        setCodec("zstd");
        String table = "CODECCHANGE";
        JsonlRowStorage zstdStore = newStorage(table);
        insert(zstdStore, 1, "Alice", 25);
        insert(zstdStore, 2, "Bob", 30);
        zstdStore.saveToFile(table);
        zstdStore.close();

        setCodec("lz4");
        JsonlRowStorage lz4Store = newStorage(table);
        insert(lz4Store, 3, "Carol", 35);
        insert(lz4Store, 4, "Dave", 40);
        lz4Store.saveToFile(table);
        lz4Store.close();

        setCodec("zstd");
        JsonlRowStorage viaZstd = newStorage(table);
        viaZstd.loadFromFile(table);
        List<Map<String, Object>> zstdRows = loadAll(viaZstd);
        assertEquals(2, zstdRows.size());
        assertEquals("Alice", zstdRows.get(0).get("NAME"));
        viaZstd.close();

        setCodec("lz4");
        JsonlRowStorage viaLz4 = newStorage(table);
        viaLz4.loadFromFile(table);
        List<Map<String, Object>> lz4Rows = loadAll(viaLz4);
        assertEquals(2, lz4Rows.size());
        assertEquals("Carol", lz4Rows.get(0).get("NAME"));
        viaLz4.close();
    }

    // ── Content fidelity (P47 null semantics / P51 control chars) ────

    @Test
    void specialCharsSurviveCompression() throws Exception {
        clearCompressionProperties();
        setCodec("zstd");
        String table = "SPECIAL";
        List<String> flatCols = List.of("ID", "DATA");
        JsonlRowStorage storage = new JsonlRowStorage(table, flatCols, Map.of(), strictConfig());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", 1L);
        r.put("DATA", "tab\there\nnewline\\backslash\"quote\u00E9\u4E2D\uD83D\uDE00");
        storage.insert(r);
        storage.saveToFile(table);
        storage.close();

        JsonlRowStorage loaded = new JsonlRowStorage(table, flatCols, Map.of(), strictConfig());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile(table);
        assertEquals("tab\there\nnewline\\backslash\"quote\u00E9\u4E2D\uD83D\uDE00",
                loadAll(loaded).get(0).get("DATA"));
        loaded.close();
    }

    @Test
    void nullSemanticsSurviveCompression() throws Exception {
        File plain = new File(tempDir.toString(), "PNS.jsonl");
        try (FileWriter fw = new FileWriter(plain)) {
            fw.write("{\"ID\":1,\"NAME\":null,\"AGE\":30}\n");
            fw.write("{\"ID\":2,\"AGE\":40}\n");
        }
        clearCompressionProperties(); // load the plain file with codec=none
        JsonlRowStorage base = newStorage("PNS");
        base.loadFromFile("PNS");
        List<boolean[]> presence = base.getRowPresence();
        assertEquals(2, presence.size());
        assertTrue(presence.get(0)[1], "row 0 NAME was explicit null");
        assertFalse(presence.get(1)[1], "row 1 NAME key was absent");
        base.close();

        setCodec("zstd");
        JsonlRowStorage rewritten = newStorage("PNS");
        rewritten.loadFromFile("PNS");
        rewritten.saveToFile("PNS");
        assertTrue(Files.exists(compressedPath("PNS", "zstd")), "save must produce the compressed file");
        rewritten.close();

        clearCompressionProperties();
        JsonlRowStorage loaded = newStorage("PNS");
        loaded.loadFromFile("PNS");
        List<boolean[]> compressedPresence = loaded.getRowPresence();
        assertEquals(2, compressedPresence.size());
        assertTrue(compressedPresence.get(0)[1], "compressed round-trip keeps explicit null");
        assertFalse(compressedPresence.get(1)[1], "compressed round-trip keeps absent key");
        assertEquals("null", nullOrJsonNull(loadAll(loaded).get(0).get("NAME")));
        assertNull(loadAll(loaded).get(1).get("NAME"), "absent key stays null in scan()");
        loaded.close();
    }

    private static String nullOrJsonNull(Object value) {
        return value == null ? "null" : String.valueOf(value);
    }

    // ── Config edge cases ─────────────────────────────────────────────

    @Test
    void unknownCodecIsRejected() {
        clearCompressionProperties();
        setCodec("brfl");
        String table = "BADCODEC";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        assertThrows(IllegalArgumentException.class, () -> storage.saveToFile(table),
                "unknown codec names must be rejected at save time");
        storage.close();
    }

    @Test
    void zstdLevelsAreClampedAndRoundTrip() throws Exception {
        for (String level : List.of("100", "0", "-5", "abc", "10")) {
            clearCompressionProperties();
            setCodec("zstd");
            System.setProperty("jsonl.compression.level", level);
            String table = "LVL_" + level.replace("-", "m");
            JsonlRowStorage storage = newStorage(table);
            insert(storage, 1, "Alice", 25);
            insert(storage, 2, "Bob", 30);
            storage.saveToFile(table);
            storage.close();

            JsonlRowStorage loaded = newStorage(table);
            loaded.loadFromFile(table);
            assertEquals(2, loadAll(loaded).size(), "level '" + level + "' must still round-trip");
            loaded.close();
        }
    }

    @Test
    void systemPropertyOverridesConfigProperties() throws Exception {
        setCodec("zstd");
        String table = "SYSOVERRIDE";
        JsonlRowStorage storage = newStorage(table);
        insert(storage, 1, "Alice", 25);
        storage.saveToFile(table);
        storage.close();
        assertTrue(Files.exists(compressedPath(table, "zstd")),
                "system property zstd must win over config.properties codec=none");
        assertFalse(Files.exists(path(table)), "no plain file when the property forces zstd");
    }

    // ── Append mode interplay ─────────────────────────────────────────

    @Test
    void appendModeFallsBackToRewriteWithCompression() throws Exception {
        clearCompressionProperties();
        setCodec("zstd");
        String table = "APZ";
        JsonParserConfig appendConfig = new JsonParserConfig.Builder()
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .build();
        JsonlRowStorage storage = newStorage(table, appendConfig);
        insert(storage, 1, "Alice", 25);
        insert(storage, 2, "Bob", 30);
        insert(storage, 3, "Carol", 35);
        storage.saveToFile(table);
        insert(storage, 4, "Dave", 40);
        storage.saveToFile(table);
        storage.close();

        assertTrue(Files.exists(compressedPath(table, "zstd")), "append+compression must rewrite to .zst");
        assertFalse(Files.exists(path(table)), "no plain append file with compression");

        JsonlRowStorage loaded = newStorage(table);
        loaded.loadFromFile(table);
        assertEquals(4, loadAll(loaded).size(), "full rewrite fallback must keep all rows");
        loaded.close();
    }

    @Test
    void appendModeWorksWithNoneCodec() throws Exception {
        clearCompressionProperties();
        setCodec("none");
        String table = "APN";
        JsonParserConfig appendConfig = new JsonParserConfig.Builder()
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .build();
        JsonlRowStorage storage = newStorage(table, appendConfig);
        for (int i = 1; i <= 4; i++) {
            insert(storage, i, "user" + i, 20 + i);
            storage.saveToFile(table);
        }
        storage.close();

        assertTrue(Files.exists(path(table)), "codec=none keeps the plain append file");
        assertFalse(Files.exists(compressedPath(table, "zstd")), "no compressed file with codec=none");
        assertFalse(Files.exists(tempDir.resolve(table + JsonlDeltaManager.DELTA_FILE_SUFFIX)),
                "appends without deletions leave no delta sidecar");
        assertEquals(4, Files.readAllLines(path(table)).size(), "all rows appended to the base file");

        JsonlRowStorage loaded = newStorage(table);
        loaded.loadFromFile(table);
        assertEquals(4, loadAll(loaded).size());
        loaded.close();
    }

    // ── Size + timing acceptance ──────────────────────────────────────

    @Test
    void compressedSizesMeetFourTimesReduction() throws Exception {
        clearCompressionProperties();
        setCodec("none");
        String plainTable = "SIZEPLAIN";
        JsonlRowStorage plain = newStorage(plainTable);
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "name-" + i + " " + "AAAA".repeat(50));
            r.put("AGE", i % 100);
            plain.insert(r);
        }
        plain.saveToFile(plainTable);
        plain.close();
        long plainBytes = Files.size(path(plainTable));

        setCodec("zstd");
        String zstdTable = "SIZEZSTD";
        JsonlRowStorage zstd = newStorage(zstdTable);
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "name-" + i + " " + "AAAA".repeat(50));
            r.put("AGE", i % 100);
            zstd.insert(r);
        }
        zstd.saveToFile(zstdTable);
        zstd.close();
        long zstdBytes = Files.size(compressedPath(zstdTable, "zstd"));

        assertTrue(zstdBytes * 4 <= plainBytes,
                "zstd must compress repetitive data at least 4x: plain=" + plainBytes
                        + " compressed=" + zstdBytes);

        setCodec("lz4");
        String lz4Table = "SIZELZ4";
        JsonlRowStorage lz4 = newStorage(lz4Table);
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "name-" + i + " " + "AAAA".repeat(50));
            r.put("AGE", i % 100);
            lz4.insert(r);
        }
        lz4.saveToFile(lz4Table);
        lz4.close();
        long lz4Bytes = Files.size(compressedPath(lz4Table, "lz4"));
        assertTrue(lz4Bytes * 4 <= plainBytes,
                "lz4 must compress repetitive data at least 4x: plain=" + plainBytes
                        + " compressed=" + lz4Bytes);

        setCodec("snappy");
        String snappyTable = "SIZESNAPPY";
        JsonlRowStorage snappy = newStorage(snappyTable);
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "name-" + i + " " + "AAAA".repeat(50));
            r.put("AGE", i % 100);
            snappy.insert(r);
        }
        snappy.saveToFile(snappyTable);
        snappy.close();
        long snappyBytes = Files.size(compressedPath(snappyTable, "snappy"));
        assertTrue(snappyBytes * 4 <= plainBytes,
                "snappy must compress repetitive data at least 4x: plain=" + plainBytes
                        + " compressed=" + snappyBytes);

        System.out.println("JSONL compression sizes (500 repetitive rows): plain=" + plainBytes
                + " zstd=" + zstdBytes + " lz4=" + lz4Bytes + " snappy=" + snappyBytes);
    }

    @Test
    void saveAndLoadTimingMeasurement() throws Exception {
        clearCompressionProperties();
        setCodec("none");
        String plainTable = "TIMEPLAIN";
        measureSaveLoad(plainTable, "none", false);

        setCodec("zstd");
        String zstdTable = "TIMEZSTD";
        measureSaveLoad(zstdTable, "zstd", true);
    }

    private void measureSaveLoad(String table, String codec, boolean assertRatio) throws Exception {
        JsonlRowStorage storage = newStorage(table);
        int n = 5000;
        for (int i = 0; i < n; i++) {
            storage.insert(row(i, "name-" + String.format("%05d", i) + " " + "AAAA".repeat(20), i % 100));
        }
        long saveStart = System.nanoTime();
        storage.saveToFile(table);
        long saveNs = System.nanoTime() - saveStart;
        storage.close();
        long plainLoadNs = Long.MAX_VALUE;
        if (assertRatio) {
            clearCompressionProperties();
            setCodec("none");
            String plainTable = table.replace("TIME", "TIMEPLAINCOMP");
            JsonlRowStorage plain = newStorage(plainTable);
            for (int i = 0; i < n; i++) {
                plain.insert(row(i, "name-" + String.format("%05d", i) + " " + "AAAA".repeat(20), i % 100));
            }
            plain.saveToFile(plainTable);
            plain.close();
            long best = Long.MAX_VALUE;
            for (int i = 0; i < 3; i++) {
                JsonlRowStorage probe = newStorage(plainTable);
                long t = System.nanoTime();
                probe.loadFromFile(plainTable);
                best = Math.min(best, System.nanoTime() - t);
                probe.close();
            }
            plainLoadNs = best;
        }
        long bestLoad = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            JsonlRowStorage probe = newStorage(table);
            long t = System.nanoTime();
            probe.loadFromFile(table);
            bestLoad = Math.min(bestLoad, System.nanoTime() - t);
            probe.close();
        }
        System.out.println("JSONL compression timing codec=" + codec + ": save=" + (saveNs / 1_000_000)
                + "ms load(min of 3)=" + (bestLoad / 1_000_000) + "ms");
        if (assertRatio) {
            assertTrue(bestLoad <= plainLoadNs * 6 + 500_000_000L,
                    "sequential zstd load must not be much slower than plain: zstd=" + (bestLoad / 1_000_000)
                            + "ms plain=" + (plainLoadNs / 1_000_000) + "ms");
        }
    }

    // ── Hybrid schema inference over a compressed file ────────────────

    @Test
    void schemaInferenceReadsCompressedFile() throws Exception {
        String table = "TINF";
        String jsonl = "{\"ID\":1,\"NAME\":\"Alice\",\"EXTRA\":10}\n"
                + "{\"ID\":2,\"NAME\":\"Bob\",\"EXTRA\":20}\n";
        try (ZstdOutputStream zos = new ZstdOutputStream(
                Files.newOutputStream(compressedPath(table, "zstd")), 3)) {
            zos.write(jsonl.getBytes(StandardCharsets.UTF_8));
        }

        // HYBRID mode (default) with no sidecar -> schema inference over the compressed file.
        JsonlRowStorage storage = newStorage(table, JsonParserConfig.defaults());
        storage.loadFromFile(table);
        List<Map<String, Object>> rows = loadAll(storage);
        assertEquals(2, rows.size());
        Map<String, Object> first = rows.get(0);
        assertEquals(1L, first.get("ID"));
        assertEquals("Alice", first.get("NAME"));
        assertEquals(10L, first.get("EXTRA"), "inference must expand the schema with the compressed EXTRA field");
        storage.close();
    }

    private static String suffixFor(String codec) {
        return switch (codec) {
            case "zstd" -> ".zst";
            case "lz4" -> ".lz4";
            case "snappy" -> ".snappy";
            default -> throw new IllegalArgumentException(codec);
        };
    }
}