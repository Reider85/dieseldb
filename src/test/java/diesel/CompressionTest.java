package diesel;

import diesel.storage.CompressionFactory;
import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 39 (§1a) CSV/TSV compression tests: codec/level resolution
 * ({@code csv|tsv.compression.codec|level}), round-trips for zstd/lz4/snappy,
 * the byte-identical {@code none} plain format, transparent reading of
 * compressed and uncompressed files regardless of the setting, independent
 * csv/tsv codecs, special characters and null sentinels through compression,
 * the sequential-read fallback for compressed files, level clamping and the
 * ≥3x size reduction requirement on repetitive data.
 */
class CompressionTest {

    @TempDir
    Path tempDir;

    private static final String[] PROP_KEYS = {
            "csv.compression.codec",
            "csv.compression.level",
            "tsv.compression.codec",
            "tsv.compression.level",
            "storage.null.representation",
            "csv.parallel.read.threshold"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
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

    private static List<String> schema() {
        return cols("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        return typed("ID", "Long", "NAME", "String", "AGE", "Integer");
    }

    private CsvRowStorage csvStorage(String table) {
        CsvRowStorage storage = new CsvRowStorage(table, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private TsvRowStorage tsvStorage(String table) {
        TsvRowStorage storage = new TsvRowStorage(table, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private static void assertRows(List<Map<String, Object>> expected, List<Map<String, Object>> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i));
        }
    }

    private static List<Map<String, Object>> simpleRows() {
        return List.of(
                map("ID", 1L, "NAME", "Alice", "AGE", 25),
                map("ID", 2L, "NAME", "Bob", "AGE", 30),
                map("ID", 3L, "NAME", "Carol", "AGE", null));
    }

    // ─── round-trips per codec ──────────────────────────────────────

    @Test
    void csvRoundTripAllCodecs() throws Exception {
        for (String[] codecSuffix : new String[][]{{"zstd", ".zst"}, {"lz4", ".lz4"}, {"snappy", ".snappy"}}) {
            String codec = codecSuffix[0];
            String suffix = codecSuffix[1];
            String table = "RT_CSV_" + codec.toUpperCase();
            System.setProperty("csv.compression.codec", codec);
            CsvRowStorage storage = csvStorage(table);
            for (Map<String, Object> row : simpleRows()) {
                storage.insert(row);
            }
            storage.saveToFile(table);
            assertTrue(Files.exists(tempDir.resolve(table + ".csv" + suffix)),
                    "expected " + table + ".csv" + suffix + " for codec " + codec);
            assertFalse(Files.exists(tempDir.resolve(table + ".csv")), "plain .csv must not be written for " + codec);

            CsvRowStorage loaded = csvStorage(table);
            loaded.loadFromFile(table);
            assertRows(simpleRows(), loaded.scan());
        }
    }

    @Test
    void tsvRoundTripAllCodecs() throws Exception {
        for (String[] codecSuffix : new String[][]{{"zstd", ".zst"}, {"lz4", ".lz4"}, {"snappy", ".snappy"}}) {
            String codec = codecSuffix[0];
            String suffix = codecSuffix[1];
            String table = "RT_TSV_" + codec.toUpperCase();
            System.setProperty("tsv.compression.codec", codec);
            TsvRowStorage storage = tsvStorage(table);
            for (Map<String, Object> row : simpleRows()) {
                storage.insert(row);
            }
            storage.saveToFile(table);
            assertTrue(Files.exists(tempDir.resolve(table + ".tsv" + suffix)),
                    "expected " + table + ".tsv" + suffix + " for codec " + codec);
            assertFalse(Files.exists(tempDir.resolve(table + ".tsv")), "plain .tsv must not be written for " + codec);

            TsvRowStorage loaded = tsvStorage(table);
            loaded.loadFromFile(table);
            assertRows(simpleRows(), loaded.scan());
        }
    }

    // ─── none keeps the byte-identical plain format ────────────────

    @Test
    void noneUsesPlainFormatExactly() throws Exception {
        String table = "PLAIN_CSV";
        System.setProperty("csv.compression.codec", "none");
        CsvRowStorage storage = csvStorage(table);
        for (Map<String, Object> row : simpleRows()) {
            storage.insert(row);
        }
        storage.saveToFile(table);

        String content = Files.readString(tempDir.resolve(table + ".csv"), StandardCharsets.UTF_8);
        assertEquals("ID,NAME,AGE\n1,Alice,25\n2,Bob,30\n3,Carol,\n", content);
        assertFalse(Files.exists(tempDir.resolve(table + ".csv.zst")));

        CsvRowStorage loaded = csvStorage(table);
        loaded.loadFromFile(table);
        assertRows(simpleRows(), loaded.scan());
    }

    // ─── transparent reads in both directions ──────────────────────

    @Test
    void compressedFileReadsWhenCodecIsNone() throws Exception {
        String table = "ZSTD_READ";
        System.setProperty("csv.compression.codec", "zstd");
        CsvRowStorage storage = csvStorage(table);
        for (Map<String, Object> row : simpleRows()) {
            storage.insert(row);
        }
        storage.saveToFile(table);
        assertTrue(Files.exists(tempDir.resolve(table + ".csv.zst")));

        System.setProperty("csv.compression.codec", "none");
        CsvRowStorage loaded = csvStorage(table);
        loaded.loadFromFile(table);
        assertRows(simpleRows(), loaded.scan());
    }

    @Test
    void plainFileReadsWhenCompressionEnabled() throws Exception {
        String table = "PLAIN_READ";
        Files.writeString(tempDir.resolve(table + ".csv"),
                "ID,NAME,AGE\n7,Seven,70\n8,Eight,80\n", StandardCharsets.UTF_8);

        System.setProperty("csv.compression.codec", "zstd");
        CsvRowStorage loaded = csvStorage(table);
        loaded.loadFromFile(table);
        assertRows(List.of(
                map("ID", 7L, "NAME", "Seven", "AGE", 70),
                map("ID", 8L, "NAME", "Eight", "AGE", 80)), loaded.scan());
    }

    @Test
    void configuredCodecSuffixWinsOverPlain() throws Exception {
        String table = "SUFFIX_WIN";
        Files.writeString(tempDir.resolve(table + ".csv"), "ID,NAME,AGE\n9,Stale,99\n", StandardCharsets.UTF_8);

        System.setProperty("csv.compression.codec", "zstd");
        CsvRowStorage storage = csvStorage(table);
        for (Map<String, Object> row : simpleRows()) {
            storage.insert(row);
        }
        storage.saveToFile(table);
        assertTrue(Files.exists(tempDir.resolve(table + ".csv.zst")));

        CsvRowStorage loaded = csvStorage(table);
        loaded.loadFromFile(table);
        assertRows(simpleRows(), loaded.scan());
    }

    @Test
    void codecChangeLeavesEarlierFileReadable() throws Exception {
        String table = "CODEC_CHANGE";
        List<Map<String, Object>> first = List.of(map("ID", 1L, "NAME", "One", "AGE", 11));
        List<Map<String, Object>> second = List.of(map("ID", 2L, "NAME", "Two", "AGE", 22));

        System.setProperty("csv.compression.codec", "zstd");
        CsvRowStorage a = csvStorage(table);
        for (Map<String, Object> row : first) {
            a.insert(row);
        }
        a.saveToFile(table);
        assertTrue(Files.exists(tempDir.resolve(table + ".csv.zst")));

        System.setProperty("csv.compression.codec", "lz4");
        CsvRowStorage b = csvStorage(table);
        for (Map<String, Object> row : second) {
            b.insert(row);
        }
        b.saveToFile(table);
        assertTrue(Files.exists(tempDir.resolve(table + ".csv.lz4")), "new codec must write its own segment");
        assertTrue(Files.exists(tempDir.resolve(table + ".csv.zst")), "old segment must not be removed");

        CsvRowStorage loadedLz4 = csvStorage(table);
        loadedLz4.loadFromFile(table);
        assertRows(second, loadedLz4.scan(), "load must follow the configured codec");

        System.setProperty("csv.compression.codec", "none");
        Files.deleteIfExists(tempDir.resolve(table + ".csv"));
        Files.deleteIfExists(tempDir.resolve(table + ".csv.lz4"));
        CsvRowStorage loadedOld = csvStorage(table);
        loadedOld.loadFromFile(table);
        assertRows(first, loadedOld.scan(), "old zstd segment stays readable after a codec change");
    }

    // ─── csv/tsv independence ───────────────────────────────────────

    @Test
    void csvAndTsvCodecsAreIndependent() throws Exception {
        String csvTable = "IND_CSV";
        String tsvTable = "IND_TSV";
        System.setProperty("csv.compression.codec", "zstd");
        System.setProperty("tsv.compression.codec", "snappy");

        CsvRowStorage csv = csvStorage(csvTable);
        for (Map<String, Object> row : simpleRows()) {
            csv.insert(row);
        }
        csv.saveToFile(csvTable);
        assertTrue(Files.exists(tempDir.resolve(csvTable + ".csv.zst")));
        assertFalse(Files.exists(tempDir.resolve(csvTable + ".csv")));

        TsvRowStorage tsv = tsvStorage(tsvTable);
        for (Map<String, Object> row : simpleRows()) {
            tsv.insert(row);
        }
        tsv.saveToFile(tsvTable);
        assertTrue(Files.exists(tempDir.resolve(tsvTable + ".tsv.snappy")));
        assertFalse(Files.exists(tempDir.resolve(tsvTable + ".tsv")));

        CsvRowStorage loadedCsv = csvStorage(csvTable);
        loadedCsv.loadFromFile(csvTable);
        assertRows(simpleRows(), loadedCsv.scan());

        TsvRowStorage loadedTsv = tsvStorage(tsvTable);
        loadedTsv.loadFromFile(tsvTable);
        assertRows(simpleRows(), loadedTsv.scan());
    }

    // ─── special characters and null sentinels ─────────────────────

    @Test
    void specialCharsAndNullSentinelsSurviveCompression() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<Map<String, Object>> complex = List.of(
                map("ID", 1L, "NAME", "multi\nline \"quoted\"", "AGE", 25),
                map("ID", 2L, "NAME", "", "AGE", null),
                map("ID", 3L, "NAME", "tab\tand backslash\\", "AGE", 30));

        for (String[] format : new String[][]{
                {"csv", "zstd"}, {"csv", "snappy"}, {"tsv", "lz4"}, {"tsv", "zstd"}}) {
            String kind = format[0];
            String codec = format[1];
            String table = "SPECIAL_" + kind.toUpperCase() + "_" + codec.toUpperCase();
            System.setProperty(kind + ".compression.codec", codec);
            if (kind.equals("csv")) {
                CsvRowStorage storage = csvStorage(table);
                for (Map<String, Object> row : complex) {
                    storage.insert(row);
                }
                storage.saveToFile(table);
                CsvRowStorage loaded = csvStorage(table);
                loaded.loadFromFile(table);
                assertRows(complex, loaded.scan());
            } else {
                TsvRowStorage storage = tsvStorage(table);
                for (Map<String, Object> row : complex) {
                    storage.insert(row);
                }
                storage.saveToFile(table);
                TsvRowStorage loaded = tsvStorage(table);
                loaded.loadFromFile(table);
                assertRows(complex, loaded.scan());
            }
        }
    }

    // ─── parallel flag falls back to sequential for compressed ─────

    @Test
    void parallelLoadOfCompressedFileFallsBackToSequential() throws Exception {
        String table = "PAR_COMPRESSED";
        System.setProperty("csv.compression.codec", "zstd");
        System.setProperty("csv.parallel.read.threshold", "10");
        List<Map<String, Object>> expected = new ArrayList<>();
        for (long i = 0; i < 200; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", i);
            row.put("NAME", "name" + i);
            row.put("AGE", (int) (i % 100));
            expected.add(row);
        }
        CsvRowStorage storage = csvStorage(table);
        for (Map<String, Object> row : expected) {
            storage.insert(row);
        }
        storage.saveToFile(table);

        CsvRowStorage parallel = csvStorage(table);
        parallel.setPrimaryKeyColumn("ID");
        parallel.loadFromFile(table, true);
        assertRows(expected, parallel.scan());

        CsvRowStorage sequential = csvStorage(table);
        sequential.setPrimaryKeyColumn("ID");
        sequential.loadFromFile(table, false);
        assertRows(expected, sequential.scan());
    }

    // ─── codec/level validation ─────────────────────────────────────

    @Test
    void unknownCodecNameIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> CompressionFactory.forName("bogus"));
    }

    @Test
    void zstdLevelsAreClampedAndTolerated() throws Exception {
        String table = "LVL";
        System.setProperty("csv.compression.codec", "zstd");
        for (String level : new String[]{"0", "99", "abc"}) {
            System.setProperty("csv.compression.level", level);
            CsvRowStorage storage = csvStorage(table + "_" + level.replaceAll("\\W", ""));
            storage.insert(map("ID", 1L, "NAME", "lev", "AGE", 1));
            storage.saveToFile(storage.getTableName());
            CsvRowStorage loaded = csvStorage(storage.getTableName());
            loaded.loadFromFile(storage.getTableName());
            assertEquals(1, loaded.scan().size());
        }
    }

    @Test
    void systemPropertyOverridesConfigProperties() throws Exception {
        String table = "SYS_OVERRIDE";
        CsvRowStorage base = csvStorage(table + "_BASE");
        base.insert(map("ID", 1L, "NAME", "base", "AGE", 1));
        base.saveToFile(base.getTableName());
        assertTrue(Files.exists(tempDir.resolve(base.getTableName() + ".csv")),
                "config.properties csv.compression.codec=none must produce a plain .csv");

        String overridden = table + "_LZ4";
        System.setProperty("csv.compression.codec", "lz4");
        CsvRowStorage storage = csvStorage(overridden);
        storage.insert(map("ID", 1L, "NAME", "lz4", "AGE", 1));
        storage.saveToFile(overridden);
        assertTrue(Files.exists(tempDir.resolve(overridden + ".csv.lz4")),
                "a system property override must beat the config.properties none");
    }

    // ─── size reduction ─────────────────────────────────────────────

    @Test
    void compressedSizesMeetThreeTimesReduction() throws Exception {
        String table = "SIZE";
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "repeated descriptive text token value number " + (i % 50));
            row.put("AGE", i % 120);
            rows.add(row);
        }

        for (String[] codecSuffix : new String[][]{{"zstd", ".zst"}, {"lz4", ".lz4"}, {"snappy", ".snappy"}}) {
            String codec = codecSuffix[0];
            String suffix = codecSuffix[1];
            String plainTable = table + "_PLAIN_" + codec.toUpperCase();
            String compressedTable = table + "_" + codec.toUpperCase();
            System.setProperty("csv.compression.codec", "none");
            CsvRowStorage plain = csvStorage(plainTable);
            for (Map<String, Object> row : rows) {
                plain.insert(row);
            }
            plain.saveToFile(plainTable);
            long plainBytes = Files.size(tempDir.resolve(plainTable + ".csv"));

            System.setProperty("csv.compression.codec", codec);
            CsvRowStorage compressed = csvStorage(compressedTable);
            for (Map<String, Object> row : rows) {
                compressed.insert(row);
            }
            compressed.saveToFile(compressedTable);
            long compressedBytes = Files.size(tempDir.resolve(compressedTable + ".csv" + suffix));
            System.out.printf("Compression(size): codec=%s plain=%d compressed=%d ratio=%.2fx%n",
                    codec, plainBytes, compressedBytes, (double) plainBytes / compressedBytes);
            assertTrue(compressedBytes * 3 < plainBytes,
                    "codec " + codec + " must reach a ≥3x size reduction");
        }
    }

    // ─── read/write speed measurement (recorded, not asserted) ─────

    @Test
    void saveAndLoadTimingMeasurement() throws Exception {
        String table = "TIMING";
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "timing measurement row text number " + (i % 500));
            row.put("AGE", i % 1000);
            rows.add(row);
        }
        measure("none", table, rows, ".csv", false);
        measure("zstd", table, rows, ".csv.zst", true);
    }

    private void measure(String codec, String table, List<Map<String, Object>> rows,
                         String suffix, boolean isCompressed) throws Exception {
        String t = table + "_" + codec.toUpperCase();
        System.setProperty("csv.compression.codec", codec);
        CsvRowStorage storage = csvStorage(t);
        long writeStart = System.nanoTime();
        for (Map<String, Object> row : rows) {
            storage.insert(row);
        }
        storage.saveToFile(t);
        long writeNanos = System.nanoTime() - writeStart;
        long bytes = Files.size(tempDir.resolve(t + suffix));

        CsvRowStorage loaded = csvStorage(t);
        long readStart = System.nanoTime();
        loaded.loadFromFile(t);
        long readNanos = System.nanoTime() - readStart;
        assertRows(rows, loaded.scan());

        System.out.printf("Compression(timing): codec=%s rows=%d size=%d write=%.1fms read=%.1fms%s%n",
                codec, rows.size(), bytes, writeNanos / 1e6, readNanos / 1e6,
                isCompressed ? " (compressed)" : " (plain)");
    }

    private static void assertRows(List<Map<String, Object>> expected, List<Map<String, Object>> actual, String msg) {
        assertEquals(expected.size(), actual.size(), msg);
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), msg);
        }
    }
}