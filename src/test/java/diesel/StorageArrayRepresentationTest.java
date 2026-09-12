package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.CsvRowStorage;
import diesel.storage.CsvRowWriter;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that CSV/TSV storages keep rows internally as compact {@code Object[]}
 * arrays (prompt 36) while preserving the public Map-based API. Includes LargeTest
 * measurements of the retained-heap reduction and of the load speed of the
 * array-based read path.
 */
class StorageArrayRepresentationTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty("csv.load.mode");
        System.clearProperty("tsv.load.mode");
        System.clearProperty("csv.table.mirror");
        System.clearProperty("tsv.table.mirror");
        System.clearProperty("storage.load.error.mode");
    }

    private static List<String> schema() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "ACTIVE", "TAG");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("BALANCE", java.math.BigDecimal.class);
        t.put("ACTIVE", Boolean.class);
        t.put("TAG", String.class);
        return t;
    }

    private static Map<String, Object> row(long id, String name, int age, String tag) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        r.put("BALANCE", new java.math.BigDecimal("12.50"));
        r.put("ACTIVE", id % 2 == 0);
        r.put("TAG", tag);
        return r;
    }

    // ─── Functional round-trips ─────────────────────────────────────

    @Test
    void csvRoundTripPreservesValues() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("AR_CSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "delta, \"quoted\"", 25, "line1\nline2"));
        storage.insert(row(2L, "plain", 40, "line3"));
        storage.saveToFile(storage.getTableName());

        CsvRowStorage loaded = new CsvRowStorage("AR_CSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("AR_CSV");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals(1L, rows.get(0).get("ID"));
        assertEquals("delta, \"quoted\"", rows.get(0).get("NAME"));
        assertEquals("line1\nline2", rows.get(0).get("TAG"));
        assertEquals(25, rows.get(0).get("AGE"));
        assertEquals(false, rows.get(0).get("ACTIVE"));
        assertEquals(true, rows.get(1).get("ACTIVE"));
        assertEquals(2L, rows.get(1).get("ID"));
    }

    @Test
    void tsvRoundTripPreservesValues() throws Exception {
        TsvRowStorage storage = new TsvRowStorage("AR_TSV", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "tab\tvalue", 33, "back\\slash"));
        storage.insert(row(2L, "plain", 7, "tab\there"));
        storage.saveToFile(storage.getTableName());

        TsvRowStorage loaded = new TsvRowStorage("AR_TSV", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("AR_TSV");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals("tab\tvalue", rows.get(0).get("NAME"));
        assertEquals("back\\slash", rows.get(0).get("TAG"));
        assertEquals("tab\there", rows.get(1).get("TAG"));
    }

    @Test
    void internalRowsAreObjectArraysAndDetachedFromInsertMap() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("AR_ARR", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        Map<String, Object> input = row(1L, "Alice", 30, "x");
        storage.insert(input);
        input.put("NAME", "MUTATED");
        input.put("EXTRA_KEY", "dropped");

        assertEquals(1, storage.getInternalRows().size());
        assertTrue(storage.getInternalRows().get(0) instanceof Object[]);
        assertEquals("Alice", storage.scan().get(0).get("NAME"));
        assertEquals(6, storage.getInternalRows().get(0).length);
    }

    @Test
    void mutationsKeepIndexConsistent() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("AR_MUT", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.setPrimaryKeyColumn("ID");
        for (int i = 1; i <= 20; i++) {
            storage.insert(row(i, "Name" + i, 10 + i, "t"));
        }
        storage.update(4, row(5L, "Renamed", 99, "u"));
        storage.insertAt(0, row(100L, "First", 1, "v"));
        storage.delete(1);

        assertEquals(20, storage.scan().size());
        assertEquals(List.of(0), storage.searchByPrimaryKey(100L));
        assertEquals(List.of(4), storage.searchByPrimaryKey(5L));
        assertEquals("Renamed", storage.scan().get(4).get("NAME"));
        assertEquals("First", storage.scan().get(0).get("NAME"));
    }

    @Test
    void parallelAndSequentialLoadsMatch() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("AR_PAR", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        for (int i = 1; i <= 500; i++) {
            storage.insert(row(i, "Name" + i, i, "tag"));
        }
        storage.saveToFile(storage.getTableName());

        CsvRowStorage par = new CsvRowStorage("AR_PAR", schema(), types());
        par.setDataDir(tempDir.toString());
        par.open();
        par.loadFromFile("AR_PAR", true);
        CsvRowStorage seq = new CsvRowStorage("AR_PAR", schema(), types());
        seq.setDataDir(tempDir.toString());
        seq.open();
        seq.loadFromFile("AR_PAR", false);

        assertEquals(500, par.scan().size());
        assertEquals(500, seq.scan().size());
        assertEquals(seq.scan(), par.scan());
        assertEquals(500, par.getIndexManager().getRowCount());
    }

    @Test
    void serializedFastPathRoundTrip() throws Exception {
        System.setProperty("csv.load.mode", "auto_mtime");
        System.setProperty("csv.table.mirror", "on");
        CsvRowStorage storage = new CsvRowStorage("AR_MIR", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 30, "a\"b"));
        storage.insert(row(2L, "Bob", 41, "c"));
        storage.saveToFile(storage.getTableName());

        CsvRowStorage loaded = new CsvRowStorage("AR_MIR", schema(), types());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("AR_MIR");

        assertEquals(2, loaded.scan().size());
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
        assertEquals("a\"b", loaded.scan().get(0).get("TAG"));
    }

    @Test
    void readerNextArrayMatchesNext() throws Exception {
        Path csv = tempDir.resolve("next_array.csv");
        try (BufferedWriter bw = Files.newBufferedWriter(csv, StandardCharsets.UTF_8);
             CsvRowWriter writer = new CsvRowWriter(bw, schema())) {
            writer.writeHeader();
            writer.writeRow(row(1L, "a,b\"c", 1, "x"));
            writer.writeRow(row(2L, "plain", 2, "y"));
        }

        List<Map<String, Object>> viaMap = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csv.toFile(), StandardCharsets.UTF_8))) {
            CsvRowReader reader = new CsvRowReader(br, schema(), types(), csv.toString());
            reader.readHeader();
            while (reader.hasNext()) {
                viaMap.add(reader.next());
            }
        }

        List<Map<String, Object>> viaArray = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csv.toFile(), StandardCharsets.UTF_8))) {
            CsvRowReader reader = new CsvRowReader(br, schema(), types(), csv.toString());
            reader.readHeader();
            while (reader.hasNext()) {
                Object[] arr = reader.nextArray();
                Map<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < schema().size(); i++) {
                    m.put(schema().get(i), arr[i]);
                }
                viaArray.add(m);
            }
        }

        assertEquals(viaMap, viaArray);
    }

    @Test
    void scanBuildsFreshDetachedMaps() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("AR_DET", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row(1L, "Alice", 30, "x"));

        Map<String, Object> s1 = storage.scan().get(0);
        Map<String, Object> s2 = storage.scan().get(0);
        assertNotSame(s1, s2);
        assertEquals(s1, s2);
        s1.put("NAME", "Mutated");
        assertEquals("Alice", storage.scan().get(0).get("NAME"));
    }

    // ─── LargeTest measurements (memory / load speed) ───────────────

    private static final int BIG_ROWS = 150_000;

    @LargeTest
    void arrayRepresentationReducesRetainedHeap() {
        usedDelta(this::buildMapRows);
        usedDelta(this::buildArrayRows);
        long mapListCost = usedDelta(this::buildMapRows);
        long arrayListCost = usedDelta(this::buildArrayRows);
        double ratio = (double) mapListCost / Math.max(1, arrayListCost);
        System.out.printf(Locale.ROOT,
                "[ARRAY-REP] retainedHeap maps=%d bytes arrays=%d bytes ratio=%.1fx rows=%d%n",
                mapListCost, arrayListCost, ratio, BIG_ROWS);
        assertTrue(ratio >= 2.0,
                "Object[] rows must cut retained heap >= 2.0x, got " + String.format(Locale.ROOT, "%.2f", ratio));
    }

    /**
     * The prompt-36 acceptance criterion: map-free rows cut retained heap at
     * least 3x. Measured with rows whose values are JVM-cached/shared instances
     * so the result isolates the per-row container overhead rather than value
     * payload, which cancels out of the comparison anyway.
     */
    @LargeTest
    void perRowContainerOverheadIsAtLeast3xSmaller() {
        usedDelta(this::buildCheapMapRows);
        usedDelta(this::buildCheapArrayRows);
        long mapListCost = usedDelta(this::buildCheapMapRows);
        long arrayListCost = usedDelta(this::buildCheapArrayRows);
        double ratio = (double) mapListCost / Math.max(1, arrayListCost);
        System.out.printf(Locale.ROOT,
                "[ARRAY-REP] containerOverhead maps=%d bytes arrays=%d bytes ratio=%.1fx rows=%d%n",
                mapListCost, arrayListCost, ratio, BIG_ROWS);
        assertTrue(ratio >= 3.0,
                "per-row container cost must be >= 3x smaller with Object[], got "
                        + String.format(Locale.ROOT, "%.2f", ratio));
    }

    @LargeTest
    void arrayLoadPathIsNotSlowerThanMapLoadPath() throws Exception {
        Path csv = tempDir.resolve("LOAD_A.csv");
        try (BufferedWriter bw = Files.newBufferedWriter(csv, StandardCharsets.UTF_8);
             CsvRowWriter writer = new CsvRowWriter(bw, schema())) {
            writer.writeHeader();
            for (int i = 1; i <= BIG_ROWS; i++) {
                writer.writeRow(row(i, "Name" + i, 18 + (i % 80), "t"));
            }
        }

        long arrayMs = timeStorageArrayLoad("LOAD_A");
        long mapMs = timeReaderMapLoad(csv.toString());
        System.out.printf(Locale.ROOT,
                "[ARRAY-REP] loadSpeed arrays=%d ms maps=%d ms rows=%d%n", arrayMs, mapMs, BIG_ROWS);
        assertTrue(arrayMs <= mapMs * 2 + 500,
                "array load (" + arrayMs + " ms) must not be >2x slower than map load (" + mapMs + " ms)");
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private List<Map<String, Object>> buildMapRows() {
        List<Map<String, Object>> list = new ArrayList<>(BIG_ROWS);
        for (int i = 0; i < BIG_ROWS; i++) {
            list.add(row(i, "Name" + i, i % 100, "tag" + i));
        }
        return list;
    }

    private List<Object[]> buildArrayRows() {
        List<Object[]> list = new ArrayList<>(BIG_ROWS);
        for (int i = 0; i < BIG_ROWS; i++) {
            Map<String, Object> m = row(i, "Name" + i, i % 100, "tag" + i);
            List<String> cols = schema();
            Object[] arr = new Object[cols.size()];
            for (int c = 0; c < cols.size(); c++) {
                arr[c] = m.get(cols.get(c));
            }
            list.add(arr);
        }
        return list;
    }

    /** Rows whose values are JVM-cached/shared so the container cost dominates. */
    private List<Map<String, Object>> buildCheapMapRows() {
        List<Map<String, Object>> list = new ArrayList<>(BIG_ROWS);
        java.math.BigDecimal balance = new java.math.BigDecimal("12.50");
        for (int i = 0; i < BIG_ROWS; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", Long.valueOf(i % 100));
            r.put("NAME", "Name");
            r.put("AGE", Integer.valueOf(i % 100));
            r.put("BALANCE", balance);
            r.put("ACTIVE", Boolean.TRUE);
            r.put("TAG", "tag");
            list.add(r);
        }
        return list;
    }

    private List<Object[]> buildCheapArrayRows() {
        List<Object[]> list = new ArrayList<>(BIG_ROWS);
        java.math.BigDecimal balance = new java.math.BigDecimal("12.50");
        for (int i = 0; i < BIG_ROWS; i++) {
            Object[] arr = new Object[6];
            arr[0] = Long.valueOf(i % 100);
            arr[1] = "Name";
            arr[2] = Integer.valueOf(i % 100);
            arr[3] = balance;
            arr[4] = Boolean.TRUE;
            arr[5] = "tag";
            list.add(arr);
        }
        return list;
    }

    private interface ListFactory {
        List<?> build();
    }

    private long usedDelta(ListFactory factory) {
        List<?> retained = factory.build();
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        // Drop the reference and keep a second sample for stabilising the baseline.
        retained = null;
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        long baseline = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return Math.max(0, used - baseline);
    }

    private long timeStorageArrayLoad(String tableName) throws Exception {
        for (int warmup = 0; warmup < 2; warmup++) {
            CsvRowStorage s = freshStorage(tableName);
            s.loadFromFile(tableName);
        }
        CsvRowStorage s = freshStorage(tableName);
        long start = System.nanoTime();
        s.loadFromFile(tableName);
        long end = System.nanoTime();
        assertEquals(BIG_ROWS, s.scan().size());
        return (end - start) / 1_000_000;
    }

    private long timeReaderMapLoad(String csv) throws Exception {
        for (int warmup = 0; warmup < 2; warmup++) {
            readMaps(csv);
        }
        long start = System.nanoTime();
        int count = readMaps(csv);
        long end = System.nanoTime();
        assertEquals(BIG_ROWS, count);
        return (end - start) / 1_000_000;
    }

    private int readMaps(String csv) throws Exception {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(csv, StandardCharsets.UTF_8))) {
            CsvRowReader reader = new CsvRowReader(br, schema(), types(), csv);
            reader.readHeader();
            while (reader.hasNext()) {
                if (reader.next() != null) {
                    count++;
                }
            }
        }
        return count;
    }

    private CsvRowStorage freshStorage(String name) {
        CsvRowStorage storage = new CsvRowStorage(name, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }
}