package diesel;

import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the per-format load modes for CSV/TSV storages (Prompt 32):
 * {@code csv.load.mode} / {@code tsv.load.mode} = {@code file} (always the
 * delimited file) or {@code auto_mtime} (fresher .table wins by mtime), with
 * mandatory consistency checks and automatic fallback from a broken .table to
 * the delimited file. The optional {@code csv.table.mirror} / {@code tsv.table.mirror}
 * settings disable the secondary .table write on save.
 */
class StorageLoadModeTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty("csv.load.mode");
        System.clearProperty("tsv.load.mode");
        System.clearProperty("csv.table.mirror");
        System.clearProperty("tsv.table.mirror");
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

    /** Saves a storage holding two rows plus its .table mirror (enabled unless explicitly off). */
    private static void saveTwoRows(CsvRowStorage storage) {
        if (System.getProperty("csv.table.mirror") == null) {
            System.setProperty("csv.table.mirror", "on");
        }
        storage.open();
        storage.insert(row(1L, "Alice", 25));
        storage.insert(row(2L, "Bob", 30));
        storage.saveToFile(storage.getTableName());
    }

    private static void saveTwoRows(TsvRowStorage storage) {
        if (System.getProperty("tsv.table.mirror") == null) {
            System.setProperty("tsv.table.mirror", "on");
        }
        storage.open();
        storage.insert(row(1L, "Alice", 25));
        storage.insert(row(2L, "Bob", 30));
        storage.saveToFile(storage.getTableName());
    }

    private static void rewriteDelimited(Path dir, String table, String ext, long id, String name, int age)
            throws Exception {
        String content = "ID,NAME,AGE\n" + id + "," + name + "," + age + "\n";
        if (".tsv".equals(ext)) {
            content = "ID\tNAME\tAGE\n" + id + "\t" + name + "\t" + age + "\n";
        }
        Files.writeString(dir.resolve(table + ext), content, StandardCharsets.UTF_8);
    }

    private static void makeFresher(Path fresher, Path older) {
        long base = older.toFile().lastModified();
        assertTrue(fresher.toFile().setLastModified(base + 5000), "fresher mtime update failed");
    }

    // (a) file mode: .table is ignored even when it is fresher than the delimited file
    @Test
    void csvFileModeIgnoresNewerTableFile() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("LMA_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA_CSV", ".csv", 2L, "Betty", 40);
        makeFresher(tempDir.resolve("LMA_CSV.table"), tempDir.resolve("LMA_CSV.csv"));

        CsvRowStorage loaded = new CsvRowStorage("LMA_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA_CSV");

        assertEquals(1, loaded.scan().size());
        assertEquals(2L, loaded.scan().get(0).get("ID"));
        assertEquals("Betty", loaded.scan().get(0).get("NAME"));
    }

    @Test
    void tsvFileModeIgnoresNewerTableFile() throws Exception {
        TsvRowStorage storage = new TsvRowStorage("LMA_TSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA_TSV", ".tsv", 2L, "Betty", 40);
        makeFresher(tempDir.resolve("LMA_TSV.table"), tempDir.resolve("LMA_TSV.tsv"));

        TsvRowStorage loaded = new TsvRowStorage("LMA_TSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA_TSV");

        assertEquals(1, loaded.scan().size());
        assertEquals(2L, loaded.scan().get(0).get("ID"));
    }

    // (b) auto_mtime: a fresh .table loads through the fast path and wins over the delimited file
    @Test
    void csvAutoMtimeLoadsFresherTable() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        CsvRowStorage storage = new CsvRowStorage("LMA2_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA2_CSV", ".csv", 9L, "Stale", 99);
        makeFresher(tempDir.resolve("LMA2_CSV.table"), tempDir.resolve("LMA2_CSV.csv"));

        CsvRowStorage loaded = new CsvRowStorage("LMA2_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA2_CSV");

        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
    }

    @Test
    void tsvAutoMtimeLoadsFresherTable() throws Exception {
        System.setProperty("tsv.load.mode", "auto_mtime");
        TsvRowStorage storage = new TsvRowStorage("LMA2_TSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA2_TSV", ".tsv", 9L, "Stale", 99);
        makeFresher(tempDir.resolve("LMA2_TSV.table"), tempDir.resolve("LMA2_TSV.tsv"));

        TsvRowStorage loaded = new TsvRowStorage("LMA2_TSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA2_TSV");

        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
    }

    // (c) auto_mtime: a stale .table (delimited file newer) is ignored in favour of the delimited file
    @Test
    void csvAutoMtimePrefersNewerDelimited() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        CsvRowStorage storage = new CsvRowStorage("LMA3_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA3_CSV", ".csv", 3L, "Carol", 35);
        makeFresher(tempDir.resolve("LMA3_CSV.csv"), tempDir.resolve("LMA3_CSV.table"));

        CsvRowStorage loaded = new CsvRowStorage("LMA3_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA3_CSV");

        assertEquals(1, loaded.scan().size());
        assertEquals(3L, loaded.scan().get(0).get("ID"));
    }

    // on equal mtimes the delimited file is preferred
    @Test
    void csvAutoMtimePrefersDelimitedOnEqualMtime() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        CsvRowStorage storage = new CsvRowStorage("LMA4_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA4_CSV", ".csv", 4L, "Dave", 44);
        File csv = tempDir.resolve("LMA4_CSV.csv").toFile();
        File table = tempDir.resolve("LMA4_CSV.table").toFile();
        table.setLastModified(csv.lastModified());

        CsvRowStorage loaded = new CsvRowStorage("LMA4_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA4_CSV");

        assertEquals(1, loaded.scan().size());
        assertEquals(4L, loaded.scan().get(0).get("ID"));
    }

    // (d) a corrupt .table triggers an automatic fallback to the delimited file
    @Test
    void csvAutoMtimeFallsBackOnBrokenTable() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        CsvRowStorage storage = new CsvRowStorage("LMA5_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        Files.write(tempDir.resolve("LMA5_CSV.table"), new byte[]{1, 2, 3, 4, 5});
        makeFresher(tempDir.resolve("LMA5_CSV.table"), tempDir.resolve("LMA5_CSV.csv"));

        CsvRowStorage loaded = new CsvRowStorage("LMA5_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA5_CSV");

        assertEquals(2, loaded.scan().size());
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
    }

    @Test
    void tsvAutoMtimeFallsBackOnBrokenTable() throws Exception {
        System.setProperty("tsv.load.mode", "auto_mtime");
        TsvRowStorage storage = new TsvRowStorage("LMA5_TSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        Files.write(tempDir.resolve("LMA5_TSV.table"), new byte[]{1, 2, 3, 4, 5});
        makeFresher(tempDir.resolve("LMA5_TSV.table"), tempDir.resolve("LMA5_TSV.tsv"));

        TsvRowStorage loaded = new TsvRowStorage("LMA5_TSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA5_TSV");

        assertEquals(2, loaded.scan().size());
    }

    // (e) csv.load.mode and tsv.load.mode are configured independently
    @Test
    void csvAndTsvModesAreIndependent() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        System.setProperty("tsv.load.mode", "file");

        CsvRowStorage csv = new CsvRowStorage("IND_CSV", schema(), types());
        csv.setDataDir(tempDir.toString());
        saveTwoRows(csv);
        rewriteDelimited(tempDir, "IND_CSV", ".csv", 7L, "Seven", 70);
        makeFresher(tempDir.resolve("IND_CSV.table"), tempDir.resolve("IND_CSV.csv"));

        TsvRowStorage tsv = new TsvRowStorage("IND_TSV", schema(), types());
        tsv.setDataDir(tempDir.toString());
        saveTwoRows(tsv);
        rewriteDelimited(tempDir, "IND_TSV", ".tsv", 8L, "Eight", 80);
        makeFresher(tempDir.resolve("IND_TSV.table"), tempDir.resolve("IND_TSV.tsv"));

        CsvRowStorage loadedCsv = new CsvRowStorage("IND_CSV", schema(), types());
        loadedCsv.setDataDir(tempDir.toString());
        loadedCsv.loadFromFile("IND_CSV");
        assertEquals(2, loadedCsv.scan().size(), "csv load.mode=auto_mtime should use the fresh .table");

        TsvRowStorage loadedTsv = new TsvRowStorage("IND_TSV", schema(), types());
        loadedTsv.setDataDir(tempDir.toString());
        loadedTsv.loadFromFile("IND_TSV");
        assertEquals(1, loadedTsv.scan().size(), "tsv load.mode=file should keep using the delimited file");
        assertEquals(8L, loadedTsv.scan().get(0).get("ID"));
    }

    // optional mirror toggle: .table is not written when mirror = off
    @Test
    void mirrorOffSkipsTableFile() throws Exception {
        System.setProperty("csv.table.mirror", "off");
        System.setProperty("tsv.table.mirror", "off");

        CsvRowStorage csv = new CsvRowStorage("MIR_CSV", schema(), types());
        csv.setDataDir(tempDir.toString());
        saveTwoRows(csv);
        assertTrue(Files.exists(tempDir.resolve("MIR_CSV.csv")));
        assertFalse(Files.exists(tempDir.resolve("MIR_CSV.table")), ".table must not be written when mirror=off");

        CsvRowStorage loadedCsv = new CsvRowStorage("MIR_CSV", schema(), types());
        loadedCsv.setDataDir(tempDir.toString());
        loadedCsv.loadFromFile("MIR_CSV");
        assertEquals(2, loadedCsv.scan().size());

        TsvRowStorage tsv = new TsvRowStorage("MIR_TSV", schema(), types());
        tsv.setDataDir(tempDir.toString());
        saveTwoRows(tsv);
        assertTrue(Files.exists(tempDir.resolve("MIR_TSV.tsv")));
        assertFalse(Files.exists(tempDir.resolve("MIR_TSV.table")), ".table must not be written when mirror=off");

        TsvRowStorage loadedTsv = new TsvRowStorage("MIR_TSV", schema(), types());
        loadedTsv.setDataDir(tempDir.toString());
        loadedTsv.loadFromFile("MIR_TSV");
        assertEquals(2, loadedTsv.scan().size());
    }

    // auto_mtime with only the .table available (delimited file disappeared)
    @Test
    void autoMtimeLoadsWhenDelimitedMissing() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        CsvRowStorage storage = new CsvRowStorage("ONLY_TBL", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        Files.deleteIfExists(tempDir.resolve("ONLY_TBL.csv"));

        CsvRowStorage loaded = new CsvRowStorage("ONLY_TBL", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("ONLY_TBL");

        assertEquals(2, loaded.scan().size());
        assertEquals(1L, loaded.scan().get(0).get("ID"));
    }

    // an unknown load mode falls back to file behaviour
    @Test
    void unknownLoadModeFallsBackToFile() throws Exception {
        System.setProperty("csv.load.mode", "bogus");
        CsvRowStorage storage = new CsvRowStorage("LMA_BOGUS", schema(), types());
        storage.setDataDir(tempDir.toString());
        saveTwoRows(storage);
        rewriteDelimited(tempDir, "LMA_BOGUS", ".csv", 5L, "Eve", 55);
        makeFresher(tempDir.resolve("LMA_BOGUS.table"), tempDir.resolve("LMA_BOGUS.csv"));

        CsvRowStorage loaded = new CsvRowStorage("LMA_BOGUS", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("LMA_BOGUS");

        assertEquals(1, loaded.scan().size());
        assertEquals(5L, loaded.scan().get(0).get("ID"));
    }
}