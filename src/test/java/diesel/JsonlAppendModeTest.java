package diesel;

import diesel.storage.JsonlDeltaManager;
import diesel.storage.JsonlRowStorage;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 49: JSONL write modes - append ({@code jsonl.write.mode=append})
 * vs full rewrite (default {@code rewrite}).
 *
 * <p>In append mode new rows are physically appended to the base
 * {@code .jsonl} file at save time (with a fsync barrier); deletions and
 * updates are recorded in a {@code .jsonl.delta} sidecar
 * ({@code {"baseLineCount":N,"deletions":[...]}}) that is re-applied on load
 * until a full compaction rewrites the base. Crash safety: an interrupted
 * append leaves an unterminated trailing fragment that is discarded (or
 * repaired with its missing newline) on load. Auto-compaction triggers when
 * the delta ratio crosses {@code jsonl.compaction.threshold}.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlAppendModeTest {

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

    // ── Helpers ─────────────────────────────────────────────────────

    private static Map<String, Object> row(long id, String name, int age) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        return r;
    }

    private static JsonParserConfig appendConfig() {
        return appendConfig(0.3);
    }

    private static JsonParserConfig appendConfig(double threshold) {
        return new JsonParserConfig.Builder()
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .compactionThreshold(threshold)
                .build();
    }

    private JsonlRowStorage newStorage(String name) {
        return newStorage(name, appendConfig());
    }

    private JsonlRowStorage newStorage(String name, JsonParserConfig config) {
        JsonlRowStorage storage = new JsonlRowStorage(name, COLS, TYPES, config);
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private Path jsonlPath(String name) {
        return tempDir.resolve(name + ".jsonl");
    }

    private Path deltaPath(String name) {
        return tempDir.resolve(name + JsonlDeltaManager.DELTA_FILE_SUFFIX);
    }

    private long lineCount(String name) throws Exception {
        return Files.readAllLines(jsonlPath(name), StandardCharsets.UTF_8).size();
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue(haystack.contains(needle),
                "expected <" + needle + "> in <" + haystack + ">");
    }

    // ── Config plumbing ──────────────────────────────────────────────

    @Test
    void defaultsAreRewriteWithThirtyPercentThreshold() {
        JsonParserConfig defaults = JsonParserConfig.defaults();
        assertEquals(JsonParserConfig.WriteMode.REWRITE, defaults.writeMode());
        assertEquals(0.3, defaults.compactionThreshold(), 1e-9);
    }

    @Test
    void builderWriteModeAndThresholdAreApplied() {
        JsonParserConfig cfg = new JsonParserConfig.Builder()
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .compactionThreshold(0.15)
                .build();
        assertEquals(JsonParserConfig.WriteMode.APPEND, cfg.writeMode());
        assertEquals(0.15, cfg.compactionThreshold(), 1e-9);
        assertEquals(JsonParserConfig.WriteMode.REWRITE,
                new JsonParserConfig.Builder().writeMode(null).build().writeMode(),
                "null write mode falls back to REWRITE");
    }

    @Test
    void outOfRangeThresholdClampsToDefault() {
        JsonParserConfig cfg = new JsonParserConfig.Builder().compactionThreshold(-1).build();
        assertEquals(0.3, cfg.compactionThreshold(), 1e-9);
        assertEquals(0.3, new JsonParserConfig.Builder().compactionThreshold(7).build()
                .compactionThreshold(), 1e-9);
    }

    @Test
    void configReadsWriteModeAndThresholdFromSystemProperties() {
        String prevWrite = System.getProperty("jsonl.write.mode");
        String prevThreshold = System.getProperty("jsonl.compaction.threshold");
        try {
            System.setProperty("jsonl.write.mode", "append");
            System.setProperty("jsonl.compaction.threshold", "0.5");
            JsonParserConfig cfg = new JsonParserConfig.Builder().build();
            assertEquals(JsonParserConfig.WriteMode.APPEND, cfg.writeMode());
            assertEquals(0.5, cfg.compactionThreshold(), 1e-9);
        } finally {
            restoreProperty("jsonl.write.mode", prevWrite);
            restoreProperty("jsonl.compaction.threshold", prevThreshold);
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    @Test
    void appendModeCreatesDeltaManagerRewriteDoesNot() throws Exception {
        JsonlRowStorage append = newStorage("AMGR"); // appendConfig
        assertNotNull(append.getDeltaManager());
        assertEquals(JsonParserConfig.WriteMode.APPEND, append.getWriteMode());
        append.close();

        JsonlRowStorage rewrite = newStorage("RMGR", JsonParserConfig.defaults());
        assertNull(rewrite.getDeltaManager());
        assertEquals(JsonParserConfig.WriteMode.REWRITE, rewrite.getWriteMode());
        rewrite.close();
    }

    // ── Append behaviour ─────────────────────────────────────────────

    @Test
    void appendModeGrowsBaseFileWithoutDeltaFile() throws Exception {
        JsonlRowStorage storage = newStorage("APPEND1");
        storage.insert(row(1, "Alice", 30));
        storage.saveToFile("APPEND1");
        assertEquals(1, lineCount("APPEND1"), "first save appends the single row");
        assertFalse(Files.exists(deltaPath("APPEND1")), "no deletions - no delta sidecar");

        storage.insert(row(2, "Bob", 25));
        storage.insert(row(3, "Cid", 35));
        storage.saveToFile("APPEND1");
        assertEquals(3, lineCount("APPEND1"), "second save appends two more rows, nothing rewritten");
        assertFalse(Files.exists(deltaPath("APPEND1")));
        storage.close();
    }

    @Test
    void appendModeRoundTripPreservesAllRows() throws Exception {
        JsonlRowStorage storage = newStorage("ARTP");
        for (long i = 1; i <= 5; i++) {
            storage.insert(row(i, "name" + i, (int) (20 + i)));
        }
        storage.saveToFile("ARTP");

        JsonlRowStorage loaded = newStorage("ARTP");
        loaded.loadFromFile("ARTP");
        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(5, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("name5", rows.get(4).get("NAME"));
        assertEquals(25, rows.get(4).get("AGE"));
        loaded.close();
        storage.close();
    }

    @Test
    void newRowsDeletedBeforeFirstSaveNeverPersist() throws Exception {
        JsonlRowStorage storage = newStorage("ADELP");
        for (long i = 1; i <= 5; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.delete(2); // pending new row #2 removed, never written anywhere
        assertEquals(4, storage.scan().size());
        storage.saveToFile("ADELP");
        assertEquals(4, lineCount("ADELP"));
        assertFalse(Files.exists(deltaPath("ADELP")), "no deletions recorded for never-persisted rows");

        JsonlRowStorage loaded = newStorage("ADELP");
        loaded.loadFromFile("ADELP");
        assertEquals(4, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
        assertEquals(2L, loaded.scan().get(1).get("ID"));
        assertEquals(4L, loaded.scan().get(2).get("ID"));
        assertEquals(5L, loaded.scan().get(3).get("ID"), "row 2 deleted before save is gone");
        loaded.close();
        storage.close();
    }

    @Test
    void appendModeEmptyTableSaveProducesNothing() throws Exception {
        JsonlRowStorage storage = newStorage("AEMPTY");
        storage.saveToFile("AEMPTY");
        assertFalse(Files.exists(jsonlPath("AEMPTY")), "no rows, no deletions - nothing written");
        storage.loadFromFile("AEMPTY");
        assertTrue(storage.scan().isEmpty());
        storage.close();
    }

    // ── Delta: deletions and updates ────────────────────────────────

    @Test
    void deleteRecordsDeltaSidecarAndReloadAppliesIt() throws Exception {
        // High threshold keeps the tiny table from auto-compacting on delete.
        JsonParserConfig cfg = appendConfig(0.9);
        JsonlRowStorage storage = newStorage("ADEL", cfg);
        storage.insert(row(1, "Alice", 30));
        storage.insert(row(2, "Bob", 25));
        storage.insert(row(3, "Cid", 35));
        storage.saveToFile("ADEL");

        storage.delete(1); // drop "Bob"
        storage.saveToFile("ADEL");
        assertTrue(Files.exists(deltaPath("ADEL")), "deletion persisted in the delta sidecar");
        assertEquals(3, lineCount("ADEL"), "base file is untouched by a pure deletion");
        String deltaContent = Files.readString(deltaPath("ADEL"), StandardCharsets.UTF_8);
        assertContains(deltaContent, "\"baseLineCount\":3");
        assertContains(deltaContent, "1");

        JsonlRowStorage loaded = newStorage("ADEL", cfg);
        loaded.loadFromFile("ADEL");
        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
        assertEquals(3L, loaded.scan().get(1).get("ID"));
        loaded.close();
        storage.close();
    }

    @Test
    void updateWritesOldRowAsDeletionNewRowAsAppend() throws Exception {
        JsonParserConfig cfg = appendConfig(0.9);
        JsonlRowStorage storage = newStorage("AUPD", cfg);
        storage.insert(row(1, "Alice", 30));
        storage.insert(row(2, "Bob", 25));
        storage.insert(row(3, "Cid", 35));
        storage.saveToFile("AUPD");

        storage.update(1, row(2, "Robert", 26));
        storage.saveToFile("AUPD");
        assertTrue(Files.exists(deltaPath("AUPD")));
        String deltaContent = Files.readString(deltaPath("AUPD"), StandardCharsets.UTF_8);
        assertContains(deltaContent, "\"baseLineCount\":3");
        assertContains(deltaContent, "[1]");
        assertEquals(4, lineCount("AUPD"), "updated row appended as a new line");

        JsonlRowStorage loaded = newStorage("AUPD", cfg);
        loaded.loadFromFile("AUPD");
        assertEquals(3, loaded.scan().size());
        // Row 1 was deleted from the base and the new value appended at the end.
        assertEquals("Robert", loaded.scan().get(2).get("NAME"));
        assertEquals(26, loaded.scan().get(2).get("AGE"));
        assertEquals("Cid", loaded.scan().get(1).get("NAME"));
        loaded.close();
        storage.close();
    }

    @Test
    void insertThenUpdateBeforeSaveKeepsSingleRow() throws Exception {
        JsonlRowStorage storage = newStorage("AIU");
        for (long i = 1; i <= 3; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.update(1, row(2, "renamed", 31));
        storage.saveToFile("AIU");
        assertEquals(3, lineCount("AIU"), "no base row was deleted - only the new value appended");

        JsonlRowStorage loaded = newStorage("AIU");
        loaded.loadFromFile("AIU");
        assertEquals(3, loaded.scan().size());
        // The updated row is appended at the end of the batch, the deleted
        // pending slot is not re-persisted.
        assertEquals("name1", loaded.scan().get(0).get("NAME"));
        assertEquals("name3", loaded.scan().get(1).get("NAME"));
        assertEquals("renamed", loaded.scan().get(2).get("NAME"));
        assertEquals(31, loaded.scan().get(2).get("AGE"));
        loaded.close();
        storage.close();
    }

    @Test
    void deltaSurvivesMultipleLoadSaveCycles() throws Exception {
        JsonlRowStorage storage = newStorage("ACYC");
        for (long i = 1; i <= 4; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.saveToFile("ACYC");
        storage.delete(0);
        storage.saveToFile("ACYC");

        JsonlRowStorage loaded = newStorage("ACYC");
        loaded.loadFromFile("ACYC");
        assertEquals(3, loaded.scan().size());
        loaded.delete(0);
        loaded.saveToFile("ACYC");

        JsonlRowStorage again = newStorage("ACYC");
        again.loadFromFile("ACYC");
        assertEquals(2, again.scan().size());
        assertEquals(3L, again.scan().get(0).get("ID"));
        assertEquals(4L, again.scan().get(1).get("ID"));
        again.close();
        loaded.close();
        storage.close();
    }

    // ── Crash recovery ───────────────────────────────────────────────

    @Test
    void crashMidAppendDiscardsTruncatedLastLine() throws Exception {
        String base = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":30}\n"
                + "{\"ID\":2,\"NAME\":\"B\",\"AGE\":31}\n"
                + "{\"ID\":3,\"NAME\":\"C\",\"AGE\":32}\n";
        String partial = base + "{\"ID\":4,\"NAME\":\"D\",\"AGE\":\"int";
        Files.write(jsonlPath("ACRASH"), partial.getBytes(StandardCharsets.UTF_8));

        JsonlRowStorage storage = newStorage("ACRASH");
        storage.loadFromFile("ACRASH");
        assertEquals(3, storage.scan().size(), "truncated trailing fragment is discarded");
        assertEquals(3, lineCount("ACRASH"), "file truncated back to the last intact newline");

        // A second load is now clean.
        JsonlRowStorage again = newStorage("ACRASH");
        again.loadFromFile("ACRASH");
        assertEquals(3, again.scan().size());
        again.close();
        storage.close();
    }

    @Test
    void crashBeforeFinalNewlineRepairsTerminatorKeepsRow() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"A\",\"AGE\":30}\n"
                + "{\"ID\":2,\"NAME\":\"B\",\"AGE\":31}";
        Files.write(jsonlPath("ATERM"), content.getBytes(StandardCharsets.UTF_8));

        JsonlRowStorage storage = newStorage("ATERM");
        storage.loadFromFile("ATERM");
        assertEquals(2, storage.scan().size(), "complete row missing only its newline survives");
        byte[] bytes = Files.readAllBytes(jsonlPath("ATERM"));
        assertEquals('\n', bytes[bytes.length - 1], "missing terminator restored");

        // The row is preserved on the next load too.
        JsonlRowStorage again = newStorage("ATERM");
        again.loadFromFile("ATERM");
        assertEquals(2, again.scan().size());
        again.close();
        storage.close();
    }

    // ── Compaction ───────────────────────────────────────────────────

    @Test
    void autoCompactionRewritesBaseWhenThresholdExceeded() throws Exception {
        JsonlRowStorage storage = newStorage("ACOMP", appendConfig(0.5));
        for (long i = 1; i <= 10; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.saveToFile("ACOMP"); // base = 10 lines
        for (int d = 0; d < 7; d++) {
            storage.delete(0);
        }
        storage.saveToFile("ACOMP");

        // Had no compaction run, the base would still hold the 10 original
        // lines plus a 7-deletion delta. Auto-compaction rewrote it down to
        // the 4 lines that were live when the ratio last crossed 50%.
        assertEquals(4, lineCount("ACOMP"), "auto-compaction rewrote the base as deletion ratio grew");
        assertTrue(Files.exists(deltaPath("ACOMP")), "deletion recorded after the final compaction");
        String deltaContent = Files.readString(deltaPath("ACOMP"), StandardCharsets.UTF_8);
        assertContains(deltaContent, "\"baseLineCount\":4");

        JsonlRowStorage loaded = newStorage("ACOMP");
        loaded.loadFromFile("ACOMP");
        assertEquals(3, loaded.scan().size());
        assertEquals(8L, loaded.scan().get(0).get("ID"));
        assertEquals(9L, loaded.scan().get(1).get("ID"));
        assertEquals(10L, loaded.scan().get(2).get("ID"));
        loaded.close();
        storage.close();
    }

    @Test
    void manualCompactionRewritesBaseAndClearsDelta() throws Exception {
        JsonlRowStorage storage = newStorage("AMCPC");
        for (long i = 1; i <= 3; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.saveToFile("AMCPC"); // 3 lines
        storage.delete(1);
        storage.saveToFile("AMCPC"); // delta {1}, base 3 lines
        storage.delete(0);
        storage.saveToFile("AMCPC"); // delta {0,1}, base 3 lines

        storage.compactJsonl();
        assertEquals(1, lineCount("AMCPC"), "compaction rewrites only live rows");
        assertFalse(Files.exists(deltaPath("AMCPC")), "compaction clears the delta sidecar");
        assertEquals(0, storage.getDeltaManager().getDeletedBaseLines().size());
        assertEquals(1, storage.getDeltaManager().getBaseFileLineCount());

        JsonlRowStorage loaded = newStorage("AMCPC");
        loaded.loadFromFile("AMCPC");
        assertEquals(1, loaded.scan().size());
        assertEquals(3L, loaded.scan().get(0).get("ID"));
        loaded.close();
        storage.close();
    }

    // ── Rewrite mode stays a full rewrite ────────────────────────────

    @Test
    void rewriteModeRewritesWholeFileOnEverySave() throws Exception {
        JsonlRowStorage storage = newStorage("RWBL", JsonParserConfig.defaults());
        for (long i = 1; i <= 3; i++) {
            storage.insert(row(i, "name" + i, 30));
        }
        storage.saveToFile("RWBL");
        assertEquals(3, lineCount("RWBL"));
        storage.delete(1);
        storage.saveToFile("RWBL");

        assertEquals(2, lineCount("RWBL"), "rewrite mode physically drops the deleted row");
        assertNull(storage.getDeltaManager());
        JsonlRowStorage loaded = newStorage("RWBL", JsonParserConfig.defaults());
        loaded.loadFromFile("RWBL");
        assertEquals(2, loaded.scan().size());
        loaded.close();
        storage.close();
    }

    // ── Performance criterion ────────────────────────────────────────

    @Test
    void appendIncrementalSaveIsFasterThanRewrite() throws Exception {
        double rewrite = timeIncrementalSave(JsonParserConfig.WriteMode.REWRITE, 100_000, 10_000);
        double append = timeIncrementalSave(JsonParserConfig.WriteMode.APPEND, 100_000, 10_000);
        // 2x margin accounts for fixed I/O overhead (fsync, delta file, append seek).
        assertTrue(append < rewrite * 0.5,
                "append incremental save (" + append + " ms) should be faster than rewrite ("
                        + rewrite + " ms)");
    }

    private double timeIncrementalSave(JsonParserConfig.WriteMode mode, int base, int extra) throws Exception {
        warmUp(mode);
        JsonParserConfig cfg = new JsonParserConfig.Builder().writeMode(mode).build();
        String name = "PERF_" + mode;
        JsonlRowStorage storage = newStorage(name, cfg);
        for (int i = 0; i < base; i++) {
            storage.insert(row(i, "n" + i, i));
        }
        storage.saveToFile(name);
        for (int i = 0; i < extra; i++) {
            storage.insert(row(base + i, "n" + (base + i), base + i));
        }
        long t0 = System.nanoTime();
        storage.saveToFile(name);
        long t1 = System.nanoTime();
        storage.close();
        return (t1 - t0) / 1_000_000.0;
    }

    private void warmUp(JsonParserConfig.WriteMode mode) throws Exception {
        JsonParserConfig cfg = new JsonParserConfig.Builder().writeMode(mode).build();
        String name = "WARM_" + mode;
        JsonlRowStorage storage = newStorage(name, cfg);
        for (int i = 0; i < 2_000; i++) {
            storage.insert(row(i, "n" + i, i));
        }
        storage.saveToFile(name);
        storage.insert(row(9_999, "x", 9_999));
        storage.saveToFile(name);
        storage.close();
    }
}