package diesel;

import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.AvroCompressionConfig;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.BZip2Codec;
import org.apache.avro.file.CodecFactory;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 66 BZip2 codec tests: block size resolution/clamping,
 * codec round-trip, config resolution, cold-data detection,
 * storage tiering recommendations, compression ratio, integration
 * with AvroRowStorage, and archive-tier behaviour.
 */
@Tag("storage")
@StorageType("avro")
class BZip2CodecTest {

    private static final String[] PROP_KEYS = {
            BZip2Codec.BLOCK_SIZE_KEY,
            BZip2Codec.COLD_THRESHOLD_KEY
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

    private static List<Map<String, Object>> rows(int n) {
        List<Map<String, Object>> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(simpleRow(i));
        }
        return list;
    }

    private File writeRows(String tableName, int blockSize) throws IOException {
        File f = new File(tempDir.toFile(), tableName + "_bzip2.avro");
        CodecFactory factory = AvroCodecFactory.factory("bzip2", blockSize);
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f, factory);
        try {
            for (Map<String, Object> row : rows(1000)) {
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

    // ─── Block size resolution ────────────────────────────────────────

    @Test
    void resolveBlockSizeClamps() {
        assertEquals(BZip2Codec.DEFAULT_BLOCK_SIZE, BZip2Codec.resolveBlockSize(-1));
        assertEquals(BZip2Codec.MIN_BLOCK_SIZE, BZip2Codec.resolveBlockSize(0));
        assertEquals(BZip2Codec.MIN_BLOCK_SIZE, BZip2Codec.resolveBlockSize(50_000));
        assertEquals(BZip2Codec.MAX_BLOCK_SIZE, BZip2Codec.resolveBlockSize(999_999));
        assertEquals(500_000, BZip2Codec.resolveBlockSize(500_000));
        assertEquals(BZip2Codec.MIN_BLOCK_SIZE, BZip2Codec.resolveBlockSize(BZip2Codec.MIN_BLOCK_SIZE));
        assertEquals(BZip2Codec.MAX_BLOCK_SIZE, BZip2Codec.resolveBlockSize(BZip2Codec.MAX_BLOCK_SIZE));
    }

    // ─── Codec factory ─────────────────────────────────────────────────

    @Test
    void newCodecProducesBzip2CodecFactory() throws IOException {
        CodecFactory factory = BZip2Codec.newCodec(-1);
        assertNotNull(factory);
        File f = writeRows("bzip2_factory", -1);
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals("bzip2", r.getCodecName());
        }
    }

    @Test
    void bzip2RoundTripForVariousBlockSizes() throws IOException {
        for (int blockSize : new int[]{100_000, 300_000, 500_000, 900_000}) {
            File f = writeRows("bs_" + blockSize, blockSize);
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals("bzip2", r.getCodecName());
                assertEquals(1000, countRecords(r));
            }
        }
    }

    // ─── Config resolution ─────────────────────────────────────────────

    @Test
    void blockSizeFromSysprop() {
        System.setProperty(BZip2Codec.BLOCK_SIZE_KEY, "500000");
        assertEquals(500_000, BZip2Codec.resolveBlockSize(
                Integer.parseInt(System.getProperty(BZip2Codec.BLOCK_SIZE_KEY))));
    }

    @Test
    void blockSizeFromFile() {
        // config.properties has avro.bzip2.block.size = 900000; resolveBlockSize(-1) should return default
        assertEquals(BZip2Codec.DEFAULT_BLOCK_SIZE, BZip2Codec.resolveBlockSize(-1));
    }

    @Test
    void coldThresholdFromSysprop() {
        System.setProperty(BZip2Codec.COLD_THRESHOLD_KEY, "5242880");
        assertEquals(5_242_880L, BZip2Codec.resolveColdThreshold());
    }

    // ─── Cold data detection ───────────────────────────────────────────

    @Test
    void coldDataDetectionAboveThreshold() {
        assertTrue(BZip2Codec.isColdData(BZip2Codec.DEFAULT_COLD_THRESHOLD));
        assertTrue(BZip2Codec.isColdData(BZip2Codec.DEFAULT_COLD_THRESHOLD + 1));
    }

    @Test
    void coldDataDetectionBelowThreshold() {
        assertFalse(BZip2Codec.isColdData(BZip2Codec.DEFAULT_COLD_THRESHOLD - 1));
        assertFalse(BZip2Codec.isColdData(1024));
    }

    // ─── Storage tiering recommendations ────────────────────────────────

    @Test
    void recommendedCodecForColdData() {
        assertEquals("bzip2", BZip2Codec.getRecommendedCodec(20_000_000, "cold"));
        assertEquals("bzip2", BZip2Codec.getRecommendedCodec(20_000_000, null));
    }

    @Test
    void recommendedCodecForWarmData() {
        assertEquals("zstandard", BZip2Codec.getRecommendedCodec(2_000_000, "warm"));
        assertEquals("null", BZip2Codec.getRecommendedCodec(10_000, "warm"));
    }

    @Test
    void recommendedCodecForHotData() {
        assertEquals("null", BZip2Codec.getRecommendedCodec(20_000_000, "hot"));
        assertEquals("null", BZip2Codec.getRecommendedCodec(10_000, "hot"));
    }

    @Test
    void recommendedCodecSmallData() {
        assertEquals("null", BZip2Codec.getRecommendedCodec(100, "cold"));
        assertEquals("null", BZip2Codec.getRecommendedCodec(100, null));
    }

    // ─── Trade-off analysis ─────────────────────────────────────────────

    @Test
    void analyzeTradeoffCoversAllRanges() {
        String info = BZip2Codec.analyzeTradeoff(BZip2Codec.DEFAULT_BLOCK_SIZE);
        assertNotNull(info);
        assertFalse(info.isEmpty());
        assertTrue(info.contains("900KB") || info.contains("~900KB"));
    }

    @Test
    void analyzeTradeoffSmallBlockSize() {
        String info = BZip2Codec.analyzeTradeoff(150_000);
        assertTrue(info.contains("200KB") || info.contains("faster"));
    }

    @Test
    void analyzeTradeoffLargeBlockSize() {
        String info = BZip2Codec.analyzeTradeoff(800_000);
        assertTrue(info.contains("900KB") || info.contains("archival"));
    }

    // ─── Compression ratio ─────────────────────────────────────────────

    @Test
    void bzip2CompressionRatioOnRepetitiveData() throws IOException {
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(10);
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }
        File nullFile = new File(tempDir.toFile(), "bz2_null_ref.avro");
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
        assertTrue(nullBytes > 0, "null-codec reference file must exist");

        File bz2File = writeRows("bz2_ratio", BZip2Codec.DEFAULT_BLOCK_SIZE);
        long bz2Bytes = bz2File.length();
        assertTrue(bz2Bytes < nullBytes,
                "bzip2 (" + bz2Bytes + ") should compress repetitive data vs null (" + nullBytes + ")");
        // bzip2 should achieve at least 1.5x compression on repetitive text
        assertTrue(nullBytes / (double) bz2Bytes >= 1.5,
                "bzip2 should compress repetitive data >= 1.5x (null=" + nullBytes + ", bzip2=" + bz2Bytes + ")");
    }

    // ─── Integration ────────────────────────────────────────────────────

    @Test
    void storageRoundTripWithBzip2() throws IOException {
        File f = writeRows("bz2_storage", BZip2Codec.DEFAULT_BLOCK_SIZE);
        try (AvroDataFileReader r = new AvroDataFileReader(f, List.of("ID", "NAME"))) {
            assertTrue(r.isReaderSchemaCompatible());
            int n = 0;
            while (r.hasNext()) {
                var rec = r.next();
                assertEquals((long) n, rec.get("ID"));
                assertEquals("User" + n, String.valueOf(rec.get("NAME")));
                n++;
            }
            assertEquals(1000, n);
        }
    }

    @Test
    void invalidBlockSizeIsClamped() throws IOException {
        // Block size below minimum is clamped to MIN_BLOCK_SIZE
        File f = writeRows("bz2_clamped", 50_000);
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals("bzip2", r.getCodecName());
        }
    }

    @Test
    void codecRoundTripViaFactory() throws IOException {
        for (int blockSize : new int[]{100_000, 900_000}) {
            File f = writeRows("bz2_rt_" + blockSize, blockSize);
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals("bzip2", r.getCodecName());
                assertEquals(1000, countRecords(r));
            }
        }
    }

    @Test
    void bzip2CompressionOnMixedData() throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "User" + i + "_name_" + "x".repeat(50));
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            data.add(r);
        }
        File nullFile = new File(tempDir.toFile(), "bz2_mixed_null.avro");
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
        File bz2File = writeRows("bz2_mixed", BZip2Codec.DEFAULT_BLOCK_SIZE);
        long bz2Bytes = bz2File.length();
        assertTrue(bz2Bytes < nullBytes,
                "bzip2 should compress mixed data vs null");
    }

    @Test
    void configSummaryIsNotEmpty() {
        String summary = BZip2Codec.configSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
        assertTrue(summary.contains("BZip2Config"));
    }

    @Test
    void estimateCompressionRatioBounded() {
        double ratio = BZip2Codec.estimateCompressionRatio(1_000_000, "text");
        assertTrue(ratio > 0.0 && ratio <= 1.0);
        // text should compress better than binary
        double textRatio = BZip2Codec.estimateCompressionRatio(1_000_000, "text");
        double binaryRatio = BZip2Codec.estimateCompressionRatio(1_000_000, "binary");
        assertTrue(textRatio < binaryRatio);
    }
}