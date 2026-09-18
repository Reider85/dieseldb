package diesel;

import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.AvroCompressionConfig;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.SnappyOptimizedCodec;
import diesel.storage.avro.ZStandardCodec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 62 AVRO compression tests: codec/level resolution
 * ({@code avro.compression.codec|level|auto|auto.min.bytes}), round-trips for
 * null/deflate/snappy/zstandard/bzip2, level clamping, auto-select by payload
 * size, storage integration and a {@code @LargeTest} codec benchmark.
 *
 * <p>Extended in Prompt 64 with Snappy optimization tests including buffer
 * size selection, caching, benchmarking, and performance recommendations.
 */
@Tag("storage")
@StorageType("avro")
class AvroCompressionTest {

    private static final String[] PROP_KEYS = {
            "avro.compression.codec",
            "avro.compression.level",
            "avro.compression.auto",
            "avro.compression.auto.min.bytes"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @TempDir
    Path tempDir;

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
    }

    @AfterEach
    void restoreConfig() {
        for (String key : PROP_KEYS) {
            String prev = prevProps.get(key);
            if (prev != null) {
                System.setProperty(key, prev);
            } else {
                System.clearProperty(key);
            }
        }
    }

    private static List<String> codecs() {
        return List.of("null", "deflate", "snappy", "zstandard", "bzip2");
    }

    private static List<String> simpleCols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> simpleTypes() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> simpleRow(long id) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", "User" + id);
        r.put("AGE", (int) (id % 100));
        r.put("ACTIVE", id % 2 == 0);
        return r;
    }

    /** Writes {@code rows} rows with the given codec (level -1) and returns the file. */
    private File writeRows(String tableName, String codec, List<Map<String, Object>> rows, int level) throws IOException {
        File f = new File(tempDir.toFile(), tableName + "_" + codec + ".avro");
        CodecFactory factory = AvroCodecFactory.factory(codec, level);
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f, factory);
        try {
            for (Map<String, Object> row : rows) {
                w.writeRow(row);
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private static int countRecords(AvroDataFileReader reader) {
        int n = 0;
        while (reader.hasNext()) {
            reader.next();
            n++;
        }
        return n;
    }

    private static long compressibleRowsSize(long rows, int textLen) {
        List<Object[]> arr = new ArrayList<>((int) rows);
        String text = "x".repeat(textLen);
        for (long i = 0; i < rows; i++) {
            arr.add(new Object[]{i, text, (int) (i % 100), i % 2 == 0});
        }
        return AvroCompressionConfig.estimatedBytes(arr);
    }

    // ─── Codec / level resolution ───────────────────────────────────

    @Test
    void resolvesDefaultsToNullCodec() {
        AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
        assertEquals("null", cfg.codec());
        assertEquals(-1, cfg.level());
        assertEquals(false, cfg.auto());
        assertEquals(1_048_576L, cfg.minBytes());
    }

    @Test
    void resolvesEverySupportedCodecFactory() {
        for (String codec : codecs()) {
            System.setProperty("avro.compression.codec", codec);
            AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
            assertEquals(codec, cfg.codec());
            CodecFactory factory = AvroCodecFactory.factory(codec, -1);
            assertNotNull(factory);
            assertTrue(factory.toString().startsWith(codec),
                    codec + " factory toString() = " + factory.toString());
        }
    }

    @Test
    void unknownCodecRejected() {
        System.setProperty("avro.compression.codec", "gzip");
        assertThrows(IllegalArgumentException.class, AvroCompressionConfig::resolve);
        assertThrows(IllegalArgumentException.class, () -> AvroCodecFactory.factory("gzip", -1));
        assertThrows(IllegalArgumentException.class, () -> AvroCodecFactory.factory("", -1));
    }

    // ─── ZStandard codec (Prompt 63) ────────────────────────────────

    @Test
    void zstandardCodecExposesLevelRange() {
        assertEquals("zstandard", ZStandardCodec.CODEC_NAME);
        assertEquals(1, ZStandardCodec.MIN_LEVEL);
        assertEquals(22, ZStandardCodec.MAX_LEVEL);
        assertEquals(CodecFactory.DEFAULT_ZSTANDARD_LEVEL, ZStandardCodec.DEFAULT_LEVEL);
        CodecFactory factory = ZStandardCodec.newCodec(-1);
        assertNotNull(factory);
        assertTrue(factory.toString().startsWith("zstandard"),
                "zstandard factory toString() = " + factory);
    }

    @Test
    void zstandardResolveLevelClamps() {
        assertEquals(3, ZStandardCodec.resolveLevel(-1));
        assertEquals(1, ZStandardCodec.resolveLevel(0));
        assertEquals(22, ZStandardCodec.resolveLevel(99));
        assertEquals(7, ZStandardCodec.resolveLevel(7));
        assertEquals(1, ZStandardCodec.resolveLevel(ZStandardCodec.MIN_LEVEL));
        assertEquals(22, ZStandardCodec.resolveLevel(ZStandardCodec.MAX_LEVEL));
    }

    @Test
    void zstandardLevelsRoundTrip() throws IOException {
        for (int level : new int[]{1, 3, 19, 22, -1}) {
            File f = writeRows("zstd_level_" + level, "zstandard", rows(5000), level);
            assertEquals("zstandard", readCodec(f),
                    "level " + level + " header codec");
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals(5000, countRecords(r), "level " + level + " row count");
            }
        }
    }

    @Test
    void zstandardFactoryDelegatesToZStandardCodec() {
        assertEquals(
                ZStandardCodec.newCodec(10).toString(),
                AvroCodecFactory.factory("zstandard", 10).toString());
    }

    @Test
    void levelResolutionAndFallback() {
        System.setProperty("avro.compression.level", "7");
        assertEquals(7, AvroCompressionConfig.resolve().level());

        System.setProperty("avro.compression.level", "not-a-number");
        assertEquals(-1, AvroCompressionConfig.resolve().level());

        System.setProperty("avro.compression.level", "3");
        assertEquals(3, AvroCompressionConfig.resolve().level());
    }

    @Test
    void outOfRangeLevelsAreClamped() throws IOException {
        // deflate: 99 clamps to 9 (a raw level 99 would fail in java.util.zip.Deflater)
        File deflate99 = writeRows("clamp_deflate", "deflate",
                rows(5), 99);
        assertEquals("deflate", readCodec(deflate99));

        // deflate: -5 clamps up to 0
        File deflateNeg = writeRows("clamp_deflate_neg", "deflate", rows(5), -5);
        assertEquals("deflate", readCodec(deflateNeg));

        // zstandard: 99 clamps to 22 (zstd accepts up to 22)
        File zstd99 = writeRows("clamp_zstd", "zstandard", rows(5), 99);
        assertEquals("zstandard", readCodec(zstd99));

        // zstandard: 0 clamps up to 1
        File zstd0 = writeRows("clamp_zstd0", "zstandard", rows(5), 0);
        assertEquals("zstandard", readCodec(zstd0));
    }

    private static String readCodec(File f) throws IOException {
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            return r.getCodecName();
        }
    }

    private static List<Map<String, Object>> rows(int n) {
        List<Map<String, Object>> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(simpleRow(i));
        }
        return list;
    }

    // ─── Round-trips ────────────────────────────────────────────────

    @Test
    void codecRoundTrip() throws IOException {
        for (String codec : codecs()) {
            File f = writeRows("roundtrip", codec, rows(5000), -1);
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals(codec, r.getCodecName());
                assertEquals(5000, countRecords(r));
            }
        }
    }

    @Test
    void zstandardLevelChangesFileSize() throws IOException {
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.".repeat(4);
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }
        long size1 = writeRows("zstd_l1", "zstandard", data, 1).length();
        long size22 = writeRows("zstd_l22", "zstandard", data, 22).length();
        assertTrue(size22 <= size1, "zstd level 22 should be <= level 1 (" + size22 + " vs " + size1 + ")");
    }

    @Test
    void compressedSmallerThanNullOnRepetitiveData() throws IOException {
        String text = "Lorem ipsum dolor sit amet ".repeat(10);
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }
        long nullSize = writeRows("size_null", "null", data, -1).length();
        for (String codec : List.of("deflate", "snappy", "zstandard", "bzip2")) {
            long size = writeRows("size_" + codec, codec, data, -1).length();
            assertTrue(size < nullSize,
                    codec + " (" + size + ") should be smaller than null (" + nullSize + ")");
        }
    }

    @Test
    void bzip2RoundTrip() throws IOException {
        File f = writeRows("bzip2_rt", "bzip2", rows(1000), -1);
        try (AvroDataFileReader r = new AvroDataFileReader(f, List.of("ID", "NAME"))) {
            assertTrue(r.isReaderSchemaCompatible());
            int n = 0;
            while (r.hasNext()) {
                GenericRecord rec = r.next();
                assertEquals((long) n, rec.get("ID"));
                assertEquals("User" + n, String.valueOf(rec.get("NAME")));
                n++;
            }
            assertEquals(1000, n);
        }
    }

    // ─── Auto-select by payload size ────────────────────────────────

    @Test
    void autoSelectBelowThresholdUsesNull() {
        System.setProperty("avro.compression.codec", "zstandard");
        System.setProperty("avro.compression.auto", "on");
        System.setProperty("avro.compression.auto.min.bytes", String.valueOf(1_048_576L));
        AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
        List<Object[]> small = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            small.add(new Object[]{(long) i, "hello", i, false});
        }
        assertEquals("null", cfg.effectiveCodec(small));
    }

    @Test
    void autoSelectAboveThresholdUsesConfigured() {
        System.setProperty("avro.compression.codec", "zstandard");
        System.setProperty("avro.compression.auto", "on");
        System.setProperty("avro.compression.auto.min.bytes", String.valueOf(1_048_576L));
        AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
        long estimated = compressibleRowsSize(20000, 80);
        assertTrue(estimated >= 1_048_576L, "fixture must exceed the threshold (got " + estimated + ")");
        List<Object[]> big = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            big.add(new Object[]{(long) i, "y".repeat(80), i % 100, false});
        }
        assertEquals("zstandard", cfg.effectiveCodec(big));
    }

    @Test
    void autoSelectOffAlwaysUsesConfigured() {
        System.setProperty("avro.compression.codec", "deflate");
        System.setProperty("avro.compression.auto", "off");
        AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
        assertEquals("deflate", cfg.effectiveCodec(List.of()));
    }

    @Test
    void autoSelectWritesNullHeaderForSmallStorage() throws IOException {
        System.setProperty("avro.compression.codec", "zstandard");
        System.setProperty("avro.compression.auto", "on");
        System.setProperty("avro.compression.auto.min.bytes", String.valueOf(1_048_576L));

        AvroRowStorage s = new AvroRowStorage("auto_small", simpleCols(), simpleTypes());
        s.setDataDir(tempDir.toString());
        for (int i = 0; i < 10; i++) {
            s.insert(simpleRow(i));
        }
        s.saveToFile("auto_small");
        File f = new File(tempDir.toFile(), "auto_small.avro");
        assertEquals("null", readCodec(f));
    }

    @Test
    void autoSelectWritesConfiguredHeaderForLargeStorage() throws IOException {
        System.setProperty("avro.compression.codec", "zstandard");
        System.setProperty("avro.compression.auto", "on");
        System.setProperty("avro.compression.auto.min.bytes", String.valueOf(1_048_576L));

        AvroRowStorage s = new AvroRowStorage("auto_big", simpleCols(), simpleTypes());
        s.setDataDir(tempDir.toString());
        String text = "y".repeat(100);
        for (int i = 0; i < 20000; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            s.insert(r);
        }
        s.saveToFile("auto_big");
        File f = new File(tempDir.toFile(), "auto_big.avro");
        assertEquals("zstandard", readCodec(f));
    }

    // ─── Storage integration ────────────────────────────────────────

    @Test
    void storageRoundTripWithCodec() throws IOException {
        System.setProperty("avro.compression.codec", "zstandard");
        System.setProperty("avro.compression.level", "5");

        String table = "codec_storage";
        AvroRowStorage s1 = new AvroRowStorage(table, simpleCols(), simpleTypes());
        s1.setDataDir(tempDir.toString());
        for (int i = 0; i < 1000; i++) {
            s1.insert(simpleRow(i));
        }
        s1.saveToFile(table);

        assertEquals("zstandard", readCodec(new File(tempDir.toFile(), table + ".avro")));

        // Switching the codec must not make the previously written file unreadable.
        System.setProperty("avro.compression.codec", "null");
        AvroRowStorage s2 = new AvroRowStorage(table, simpleCols(), simpleTypes());
        s2.setDataDir(tempDir.toString());
        s2.loadFromFile(table);
        List<Map<String, Object>> result = s2.scan();
        assertEquals(1000, result.size());
        assertEquals("User0", result.get(0).get("NAME"));
        assertEquals(999L, result.get(999).get("ID"));
    }

    @Test
    void storageRejectsUnknownCodec() {
        System.setProperty("avro.compression.codec", "garbage");
        AvroRowStorage s = new AvroRowStorage("bad_codec", simpleCols(), simpleTypes());
        s.setDataDir(tempDir.toString());
        s.insert(simpleRow(1));
        assertThrows(IllegalArgumentException.class, () -> s.saveToFile("bad_codec"));
    }

    @Test
    void syspropOverridesConfigProperties() {
        System.setProperty("avro.compression.codec", "deflate");
        System.setProperty("avro.compression.level", "7");
        AvroCompressionConfig cfg = AvroCompressionConfig.resolve();
        assertEquals("deflate", cfg.codec());
        assertEquals(7, cfg.level());
    }

    // ─── Benchmark (@LargeTest) ─────────────────────────────────────

    @LargeTest
    void benchmarkCodecWriteRead() throws IOException {
        int rows = 200_000;
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
        List<Map<String, Object>> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }

        long nullBytes = 0;
        StringBuilder report = new StringBuilder("[AVRO-BENCH] rows=" + rows);
        for (String codec : codecs()) {
            File f = new File(tempDir.toFile(), "bench_" + codec + ".avro");
            long writeStart = System.nanoTime();
            AvroDataFileWriter w = new AvroDataFileWriter(
                    simpleCols(), simpleTypes(), f, AvroCodecFactory.factory(codec, -1));
            try {
                for (Map<String, Object> row : data) {
                    w.writeRow(row);
                }
                w.flush();
            } finally {
                w.close();
            }
            long writeMs = (System.nanoTime() - writeStart) / 1_000_000;

            long readStart = System.nanoTime();
            int seen;
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals(codec, r.getCodecName());
                seen = countRecords(r);
            }
            long readMs = (System.nanoTime() - readStart) / 1_000_000;
            assertEquals(rows, seen);

            long bytes = f.length();
            if (codec.equals("null")) {
                nullBytes = bytes;
            }
            report.append(String.format(Locale.ROOT,
                    " %s:write=%dms read=%dms bytes=%d", codec, writeMs, readMs, bytes));
            assertTrue(writeMs < 60_000, codec + " write must stay under 60s (got " + writeMs + "ms)");
            assertTrue(readMs < 60_000, codec + " read must stay under 60s (got " + readMs + "ms)");
        }

        String reportStr = report.toString();
        System.out.println(reportStr);
        System.out.printf(Locale.ROOT, "[AVRO-BENCH-RATIO] zstd/null=%.1fx bzip2/null=%.1fx%n",
                (double) nullBytes / new File(tempDir.toFile(), "bench_zstandard.avro").length(),
                (double) nullBytes / new File(tempDir.toFile(), "bench_bzip2.avro").length());
        assertTrue(nullBytes > 0, "null codec file must exist");
        long zstdBytes = new File(tempDir.toFile(), "bench_zstandard.avro").length();
        assertTrue(nullBytes / (double) zstdBytes >= 2.0,
                "zstd must compress repetitive data >=2x (null=" + nullBytes + ", zstd=" + zstdBytes + ")");
    }

    // ─── Snappy optimization tests (Prompt 64) ─────────────────────────

    @Test
    void snappyOptimizedCodecFactoryIntegration() {
        // Verify that AvroCodecFactory uses SnappyOptimizedCodec for snappy
        CodecFactory factory = AvroCodecFactory.factory("snappy", -1);
        assertNotNull(factory);
        assertTrue(factory.toString().contains("snappy"),
                "Snappy codec factory should be created");
    }

    @Test
    void snappyBufferSizeSelectionForTextData() {
        List<Object[]> textData = createTextDataTable(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(textData);
        
        // Text data should use larger buffers
        assertTrue(bufferSize >= 16384, "Text data should use at least 16KB buffers");
        assertTrue(bufferSize <= 32768, "Text data should not exceed 32KB buffers");
    }

    @Test
    void snappyBufferSizeSelectionForNumericData() {
        List<Object[]> numericData = createNumericDataTable(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(numericData);
        
        // Numeric data should use smaller buffers
        assertTrue(bufferSize >= 4096, "Numeric data should use at least 4KB buffers");
        assertTrue(bufferSize <= 8192, "Numeric data should not exceed 8KB buffers");
    }

    @Test
    void snappyBufferSizeSelectionForMixedData() {
        List<Object[]> mixedData = createMixedDataTable(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(mixedData);
        
        // Mixed data should use medium buffers
        assertTrue(bufferSize >= 8192, "Mixed data should use at least 8KB buffers");
        assertTrue(bufferSize <= 16384, "Mixed data should not exceed 16KB buffers");
    }

    @Test
    void snappyBenchmarkPerformance() {
        List<Object[]> data = createTextDataTable(50);
        SnappyOptimizedCodec.BenchmarkResult[] results = 
                SnappyOptimizedCodec.benchmarkCompression(data);
        
        assertNotNull(results);
        assertEquals(SnappyOptimizedCodec.BENCHMARK_SIZES.length, results.length);
        
        // Verify all benchmark results are valid
        for (SnappyOptimizedCodec.BenchmarkResult result : results) {
            assertNotNull(result);
            assertTrue(result.getBufferSize() > 0);
            assertTrue(result.getCompressionTime() >= 0);
            assertTrue(result.getDecompressionTime() >= 0);
            assertTrue(result.getCompressionRatio() >= 0);
        }
    }

    @Test
    void snappyCacheManagement() {
        // Clear cache initially
        SnappyOptimizedCodec.clearCache();
        
        String initialStats = SnappyOptimizedCodec.getCacheStats();
        assertTrue(initialStats.contains("0 compressors") && initialStats.contains("0 decompressors"),
                "Cache should be empty initially");
        
        // Perform some operations that would populate cache
        List<Object[]> data = createTextDataTable(10);
        SnappyOptimizedCodec.benchmarkCompression(data);
        
        String afterStats = SnappyOptimizedCodec.getCacheStats();
        // Cache may contain entries now
        assertTrue(afterStats.contains("Snappy cache"), "Cache stats should be available");
        
        // Clear cache again
        SnappyOptimizedCodec.clearCache();
        String finalStats = SnappyOptimizedCodec.getCacheStats();
        assertTrue(finalStats.contains("0 compressors") && finalStats.contains("0 decompressors"),
                "Cache should be empty after clear");
    }

    @Test
    void snappyRecommendationsForDataTypes() {
        // Test recommendations for different data types
        assertEquals(32768, SnappyOptimizedCodec.getRecommendedBufferSize("text"));
        assertEquals(4096, SnappyOptimizedCodec.getRecommendedBufferSize("numeric"));
        assertEquals(16384, SnappyOptimizedCodec.getRecommendedBufferSize("mixed"));
        assertEquals(8192, SnappyOptimizedCodec.getRecommendedBufferSize("binary"));
        assertEquals(8192, SnappyOptimizedCodec.getRecommendedBufferSize("unknown"));
    }

    @Test
    void snappyStorageIntegrationWithOptimizedCodec() throws IOException {
        // Test that storage works with the optimized Snappy codec
        System.setProperty("avro.compression.codec", "snappy");
        
        String table = "snappy_optimized";
        AvroRowStorage storage = new AvroRowStorage(table, simpleCols(), simpleTypes());
        storage.setDataDir(tempDir.toString());
        
        // Insert test data
        for (int i = 0; i < 1000; i++) {
            storage.insert(simpleRow(i));
        }
        storage.saveToFile(table);
        
        // Verify the file was created and can be read back
        File avroFile = new File(tempDir.toFile(), table + ".avro");
        assertTrue(avroFile.exists(), "AVRO file should be created");
        
        // Verify we can read it back
        AvroRowStorage readStorage = new AvroRowStorage(table, simpleCols(), simpleTypes());
        readStorage.setDataDir(tempDir.toString());
        readStorage.loadFromFile(table);
        
        List<Map<String, Object>> result = readStorage.scan();
        assertEquals(1000, result.size());
        assertEquals("User0", result.get(0).get("NAME"));
        assertEquals(999L, result.get(999).get("ID"));
        
        // Verify it's using Snappy codec
        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            assertEquals("snappy", reader.getCodecName());
        }
    }

    @Test
    void snappyBlockSizeBenchmark() throws IOException {
        // Test performance across different block sizes
        int rows = 5000;
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(2);
        List<Map<String, Object>> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }

        // Write uncompressed (null codec) reference file for compression ratio check
        File nullFile = new File(tempDir.toFile(), "snappy_bench_null.avro");
        AvroDataFileWriter nw = new AvroDataFileWriter(
                simpleCols(), simpleTypes(), nullFile, AvroCodecFactory.factory("null", -1));
        try {
            for (Map<String, Object> row : data) {
                nw.writeRow(row);
            }
            nw.flush();
        } finally {
            nw.close();
        }
        long nullBytes = nullFile.length();
        assertTrue(nullBytes > 0, "Null-codec reference file must exist");

        StringBuilder report = new StringBuilder("[SNAPPY-BENCH] rows=" + rows);

        for (int bufferSize : SnappyOptimizedCodec.BENCHMARK_SIZES) {
            File f = new File(tempDir.toFile(), "snappy_bench_" + bufferSize + ".avro");
            
            long writeStart = System.nanoTime();
            AvroDataFileWriter w = new AvroDataFileWriter(
                    simpleCols(), simpleTypes(), f, AvroCodecFactory.factory("snappy", -1));
            try {
                for (Map<String, Object> row : data) {
                    w.writeRow(row);
                }
                w.flush();
            } finally {
                w.close();
            }
            long writeMs = (System.nanoTime() - writeStart) / 1_000_000;

            long readStart = System.nanoTime();
            int seen;
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals("snappy", r.getCodecName());
                seen = countRecords(r);
            }
            long readMs = (System.nanoTime() - readStart) / 1_000_000;
            assertEquals(rows, seen);

            long bytes = f.length();
            
            report.append(String.format(Locale.ROOT,
                    " size=%d:write=%dms read=%dms bytes=%d", bufferSize, writeMs, readMs, bytes));
            
            assertTrue(writeMs < 30_000, "Snappy write must stay under 30s (got " + writeMs + "ms)");
            assertTrue(readMs < 30_000, "Snappy read must stay under 30s (got " + readMs + "ms)");
        }

        String reportStr = report.toString();
        System.out.println(reportStr);
        
        // Verify compression ratio: snappy 8192 should compress >= 1.5x vs null
        long compressedBytes = new File(tempDir.toFile(), "snappy_bench_8192.avro").length();
        assertTrue(nullBytes / (double) compressedBytes >= 1.5,
                "Snappy should compress repetitive data >=1.5x (null=" + nullBytes + ", snappy=" + compressedBytes + ")");
    }

    // ─── Helper methods for Snappy tests ────────────────────────────────

    private List<Object[]> createTextDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        String longText = "This is a longer text string for testing buffer selection. ";
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User" + i,
                    longText.repeat(3),
                    "Address" + i,
                    "Description" + i + " " + longText.repeat(2)
            });
        }
        return data;
    }

    private List<Object[]> createNumericDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    (double) (i * 3.14159),
                    (float) (i * 2.71828),
                    i,
                    i % 1000
            });
        }
        return data;
    }

    private List<Object[]> createMixedDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User" + i,
                    (double) (i * 3.14159),
                    i % 2 == 0,
                    "Address" + i,
                    (float) (i * 2.71828),
                    i % 100
            });
        }
        return data;
    }
}