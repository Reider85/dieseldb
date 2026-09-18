package diesel;

import diesel.storage.JsonlRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the per-format load modes for JSONL storage (Prompt 50):
 * {@code jsonl.load.mode} = {@code file} (always .jsonl) or
 * {@code auto_mtime} (fresher .table wins), with mandatory consistency
 * checks and automatic fallback from a broken .table to the .jsonl file.
 * Also verifies the optional {@code jsonl.table.mirror} toggle.
 */
@Tag("storage")
@StorageType({"jsonl", "csv"})
class JsonlLoadModeTest {

    @TempDir
    Path tempDir;

    private String prevJsonlLoadMode;
    private String prevCsvCompressionCodec;

    @BeforeEach
    void pinDefaults() {
        prevJsonlLoadMode = System.getProperty("jsonl.load.mode");
        System.setProperty("jsonl.load.mode", "file");
        prevCsvCompressionCodec = System.getProperty("csv.compression.codec");
        System.setProperty("csv.compression.codec", "none");
    }

    @AfterEach
    void clearProperties() {
        restoreOrClear("jsonl.load.mode", prevJsonlLoadMode);
        restoreOrClear("csv.compression.codec", prevCsvCompressionCodec);
        System.clearProperty("jsonl.table.mirror");
    }

    private static void restoreOrClear(String key, String prev) {
        if (prev == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, prev);
        }
    }

    private static List<String> schema() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static Map<String, Object> row(long id, String name, int age) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        return r;
    }

    private static void saveTwoRows(JsonlRowStorage storage) {
        if (System.getProperty("jsonl.table.mirror") == null) {
            System.setProperty("jsonl.table.mirror", "on");
        }
        storage.open();
        storage.insert(row(1L, "Alice", 25));
        storage.insert(row(2L, "Bob", 30));
        storage.saveToFile(storage.getTableName());
    }

    private static void rewriteJsonl(Path dir, String table, long id, String name, int age) throws Exception {
        String content = "{\"ID\":" + id + ",\"NAME\":\"" + name + "\",\"AGE\":" + age + "}\n";
        Files.writeString(dir.resolve(table + ".jsonl"), content);
    }

    private static void makeFresher(Path fresher, Path older) {
        long base = older.toFile().lastModified();
        assertTrue(fresher.toFile().setLastModified(base + 5000), "fresher mtime update failed");
    }

    // (a) file mode: .table is ignored even when it is fresher than the .jsonl file
    @Test
    void jsonlFileModeIgnoresNewerTableFile() throws Exception {
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_FILE", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteJsonl(tempDir, "LMJ_FILE", 2L, "Betty", 40);
        makeFresher(tempDir.resolve("LMJ_FILE.table"), tempDir.resolve("LMJ_FILE.jsonl"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_FILE", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_FILE");

        assertEquals(1, loaded.scan().size());
        assertEquals(2L, loaded.scan().get(0).get("ID"));
        assertEquals("Betty", loaded.scan().get(0).get("NAME"));
    }

    // (b) auto_mtime: a fresh .table loads through the fast path and wins over the .jsonl file
    @Test
    void jsonlAutoMtimeLoadsFresherTable() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_AMTIME", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteJsonl(tempDir, "LMJ_AMTIME", 9L, "Stale", 99);
        makeFresher(tempDir.resolve("LMJ_AMTIME.table"), tempDir.resolve("LMJ_AMTIME.jsonl"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_AMTIME", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_AMTIME");

        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
    }

    // (c) auto_mtime: a stale .table (.jsonl file newer) is ignored in favour of the .jsonl file
    @Test
    void jsonlAutoMtimePrefersNewerJsonl() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_STALE", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteJsonl(tempDir, "LMJ_STALE", 3L, "Carol", 35);
        makeFresher(tempDir.resolve("LMJ_STALE.jsonl"), tempDir.resolve("LMJ_STALE.table"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_STALE", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_STALE");

        assertEquals(1, loaded.scan().size());
        assertEquals(3L, loaded.scan().get(0).get("ID"));
    }

    // on equal mtimes the .jsonl file is preferred
    @Test
    void jsonlAutoMtimePrefersJsonlOnEqualMtime() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_EQUAL", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteJsonl(tempDir, "LMJ_EQUAL", 4L, "Dave", 44);
        File jsonl = tempDir.resolve("LMJ_EQUAL.jsonl").toFile();
        File table = tempDir.resolve("LMJ_EQUAL.table").toFile();
        table.setLastModified(jsonl.lastModified());

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_EQUAL", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_EQUAL");

        assertEquals(1, loaded.scan().size());
        assertEquals(4L, loaded.scan().get(0).get("ID"));
    }

    // (d) a corrupt .table triggers an automatic fallback to the .jsonl file
    @Test
    void jsonlAutoMtimeFallsBackOnBrokenTable() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_CORRUPT", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        Files.write(tempDir.resolve("LMJ_CORRUPT.table"), new byte[]{1, 2, 3, 4, 5});
        makeFresher(tempDir.resolve("LMJ_CORRUPT.table"), tempDir.resolve("LMJ_CORRUPT.jsonl"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_CORRUPT", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_CORRUPT");

        assertEquals(2, loaded.scan().size());
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
    }

    // (e) jsonl.load.mode is independent from csv.load.mode
    @Test
    void jsonlLoadModeIsIndependentFromCsv() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        System.setProperty("csv.load.mode", "file");

        // JSONL: fresh .table should be used (auto_mtime)
        JsonlRowStorage jsonl = new JsonlRowStorage("LMJ_INDEP_J", schema(), types());
        jsonl.setDataDir(tempDir.toString());
        saveTwoRows(jsonl);
        rewriteJsonl(tempDir, "LMJ_INDEP_J", 7L, "Seven", 70);
        makeFresher(tempDir.resolve("LMJ_INDEP_J.table"), tempDir.resolve("LMJ_INDEP_J.jsonl"));

        // CSV: fresh .table should be ignored (file mode)
        diesel.storage.CsvRowStorage csv = new diesel.storage.CsvRowStorage("LMJ_INDEP_C", schema(), types());
        csv.setDataDir(tempDir.toString());
        csv.open();
        csv.insert(row(1L, "Alice", 25));
        csv.insert(row(2L, "Bob", 30));
        System.setProperty("csv.table.mirror", "on");
        csv.saveToFile(csv.getTableName());
        Files.writeString(tempDir.resolve("LMJ_INDEP_C.csv"), "ID,NAME,AGE\n7,Seven,70\n");
        makeFresher(tempDir.resolve("LMJ_INDEP_C.table"), tempDir.resolve("LMJ_INDEP_C.csv"));

        JsonlRowStorage loadedJsonl = new JsonlRowStorage("LMJ_INDEP_J", schema(), types());
        loadedJsonl.setDataDir(tempDir.toString());
        loadedJsonl.loadFromFile("LMJ_INDEP_J");
        assertEquals(2, loadedJsonl.scan().size(), "jsonl load.mode=auto_mtime should use the fresh .table");

        diesel.storage.CsvRowStorage loadedCsv = new diesel.storage.CsvRowStorage("LMJ_INDEP_C", schema(), types());
        loadedCsv.setDataDir(tempDir.toString());
        loadedCsv.loadFromFile("LMJ_INDEP_C");
        assertEquals(1, loadedCsv.scan().size(), "csv load.mode=file should keep using the delimited file");
        assertEquals(7L, loadedCsv.scan().get(0).get("ID"));
    }

    // auto_mtime with only the .table available (.jsonl file disappeared)
    @Test
    void autoMtimeLoadsWhenJsonlMissing() throws Exception {
        System.setProperty("jsonl.load.mode", "auto_mtime");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_ONLY_TBL", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        Files.deleteIfExists(tempDir.resolve("LMJ_ONLY_TBL.jsonl"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_ONLY_TBL", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_ONLY_TBL");

        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
    }

    // an unknown load mode falls back to file behaviour
    @Test
    void unknownLoadModeFallsBackToFile() throws Exception {
        System.setProperty("jsonl.load.mode", "bogus");
        JsonlRowStorage storage = new JsonlRowStorage("LMJ_BOGUS", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteJsonl(tempDir, "LMJ_BOGUS", 5L, "Eve", 55);
        makeFresher(tempDir.resolve("LMJ_BOGUS.table"), tempDir.resolve("LMJ_BOGUS.jsonl"));

        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_BOGUS", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_BOGUS");

        assertEquals(1, loaded.scan().size());
        assertEquals(5L, loaded.scan().get(0).get("ID"));
    }

    // mirror toggle: .table is not written when mirror = off
    @Test
    void mirrorOffSkipsTableFile() throws Exception {
        System.setProperty("jsonl.table.mirror", "off");

        JsonlRowStorage storage = new JsonlRowStorage("LMJ_MIR", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        assertTrue(Files.exists(tempDir.resolve("LMJ_MIR.jsonl")));
        assertTrue(!Files.exists(tempDir.resolve("LMJ_MIR.table")),
                ".table must not be written when jsonl.table.mirror=off");

        // Verify we can load from .jsonl
        JsonlRowStorage loaded = new JsonlRowStorage("LMJ_MIR", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMJ_MIR");
        assertEquals(2, loaded.scan().size());
    }
}
