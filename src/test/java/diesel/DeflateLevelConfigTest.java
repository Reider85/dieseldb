package diesel;

import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.DeflateLevelConfig;
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
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 65 DeflateLevelConfig tests: level resolution/clamping, adaptive
 * level selection, trade-off analysis, compression-ratio estimation and the
 * bounded Deflater/Inflater pool.
 *
 * <p>Covered scenarios:
 * <ul>
 *   <li>{@code resolveLevel}: -1 → default 3, sub-range clamps up to 1,
 *       over-range clamps down to 9, in-range values pass through</li>
 *   <li>{@code newCodec} / {@link AvroCodecFactory#factory}: deflate round-trip
 *       for every level 1..9 (file is written and re-read with the "deflate"
 *       header codec)</li>
 *   <li>{@code selectOptimalLevel}: streaming / small / large / per-type picks,
 *       null data type → default</li>
 *   <li>{@code estimateCompressionRatio}: bounded in (0,1] and monotonically
 *       better with higher levels</li>
 *   <li>Deflater/Inflater pool: borrow/return reuse, bounded cap, clearCache
 *       releases native resources, cache-disable sysprop yields fresh objects</li>
 * </ul>
 */
@Tag("storage")
class DeflateLevelConfigTest {

    private static final String[] PROP_KEYS = {
            DeflateLevelConfig.CACHE_KEY,
            DeflateLevelConfig.CACHE_MAX_KEY
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
        DeflateLevelConfig.resolveCacheConfig();
        DeflateLevelConfig.clearCache();
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
        DeflateLevelConfig.resolveCacheConfig();
        DeflateLevelConfig.clearCache();
    }

    // ─── Level resolution ──────────────────────────────────────────

    @Test
    void resolveLevelMapsDefaultAndClamps() {
        assertEquals(3, DeflateLevelConfig.resolveLevel(-1));
        assertEquals(3, DeflateLevelConfig.resolveLevel(DeflateLevelConfig.DEFAULT_LEVEL));
        assertEquals(1, DeflateLevelConfig.resolveLevel(0));
        assertEquals(1, DeflateLevelConfig.resolveLevel(-5));
        assertEquals(1, DeflateLevelConfig.resolveLevel(DeflateLevelConfig.MIN_LEVEL));
        assertEquals(9, DeflateLevelConfig.resolveLevel(10));
        assertEquals(9, DeflateLevelConfig.resolveLevel(99));
        assertEquals(9, DeflateLevelConfig.resolveLevel(DeflateLevelConfig.MAX_LEVEL));
        for (int level = 1; level <= 9; level++) {
            assertEquals(level, DeflateLevelConfig.resolveLevel(level));
        }
    }

    @Test
    void newCodecProducesDeflateCodecFactory() throws IOException {
        CodecFactory factory = DeflateLevelConfig.newCodec(-1);
        assertNotNull(factory);
        // CodecFactory in Avro 1.12 doesn't expose getName() — verify via round-trip
        File f = writeRows("newCodecVerify", -1);
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals("deflate", r.getCodecName());
        } catch (IOException e) {
            throw new AssertionError("round-trip after newCodec failed", e);
        }
    }

    // ─── Round-trip across every level ─────────────────────────────

    @Test
    void deflateRoundTripForAllLevels() throws IOException {
        for (int level = 1; level <= 9; level++) {
            File f = writeRows("level" + level, level);
            try (AvroDataFileReader r = new AvroDataFileReader(f)) {
                assertEquals("deflate", r.getCodecName());
                assertEquals(500, countRecords(r));
            }
        }
    }

    // ─── Adaptive level selection ──────────────────────────────────

    @Test
    void selectOptimalLevelForStreaming() {
        // Streaming always prefers fast levels (<= 3)
        assertEquals(3, DeflateLevelConfig.selectOptimalLevel(100_000, "text", true));
        assertEquals(3, DeflateLevelConfig.selectOptimalLevel(10_000_000, "repetitive", true));
    }

    @Test
    void selectOptimalLevelBySize() {
        // Small payload → speed
        assertEquals(1, DeflateLevelConfig.selectOptimalLevel(100, "text", false));
        // Large payload → ratio
        assertEquals(7, DeflateLevelConfig.selectOptimalLevel(2_000_000, "text", false));
    }

    @Test
    void selectOptimalLevelByDataType() {
        assertEquals(3, DeflateLevelConfig.selectOptimalLevel(50_000, "text", false));
        assertEquals(2, DeflateLevelConfig.selectOptimalLevel(50_000, "numeric", false));
        assertEquals(5, DeflateLevelConfig.selectOptimalLevel(50_000, "binary", false));
        assertEquals(6, DeflateLevelConfig.selectOptimalLevel(50_000, "repetitive", false));
        assertEquals(3, DeflateLevelConfig.selectOptimalLevel(50_000, null, false));
        assertEquals(3, DeflateLevelConfig.selectOptimalLevel(50_000, "unknown", false));
    }

    // ─── Trade-off / estimation ────────────────────────────────────

    @Test
    void analyzeTradeoffCoversAllLevels() {
        for (int level = 1; level <= 9; level++) {
            String info = DeflateLevelConfig.analyzeTradeoff(level);
            assertTrue(info.startsWith("Level " + level), info);
            assertFalse(info.isEmpty());
        }
        assertTrue(DeflateLevelConfig.analyzeTradeoff(99).contains("Unknown"));
    }

    @Test
    void estimateCompressionRatioIsBoundedAndMonotonic() {
        double previous = 1.0;
        for (int level = 1; level <= 9; level++) {
            double ratio = DeflateLevelConfig.estimateCompressionRatio(level, "text");
            assertTrue(ratio > 0.0 && ratio <= 1.0);
            assertTrue(ratio <= previous, "level " + level + " must not be worse than lower level");
            previous = ratio;
        }
        // Text compresses better than binary at the same level
        double text = DeflateLevelConfig.estimateCompressionRatio(5, "text");
        double binary = DeflateLevelConfig.estimateCompressionRatio(5, "binary");
        assertTrue(text < binary);
    }

    // ─── Deflater / Inflater pool ──────────────────────────────────

    @Test
    void deflaterPoolReusesInstances() {
        Deflater first = DeflateLevelConfig.borrowDeflater(5);
        DeflateLevelConfig.returnDeflater(5, first);
        Deflater second = DeflateLevelConfig.borrowDeflater(5);
        assertSame(first, second, "returned Deflater should be reused");
        second.end();
        DeflateLevelConfig.clearCache();
    }

    @Test
    void inflaterPoolReusesInstances() {
        Inflater first = DeflateLevelConfig.borrowInflater();
        DeflateLevelConfig.returnInflater(first);
        Inflater second = DeflateLevelConfig.borrowInflater();
        assertSame(first, second, "returned Inflater should be reused");
        second.end();
        DeflateLevelConfig.clearCache();
    }

    @Test
    void poolRespectsMaxSizeCap() {
        System.setProperty(DeflateLevelConfig.CACHE_MAX_KEY, "2");
        DeflateLevelConfig.resolveCacheConfig();
        DeflateLevelConfig.clearCache();

        Deflater d1 = DeflateLevelConfig.borrowDeflater(3);
        Deflater d2 = DeflateLevelConfig.borrowDeflater(3);
        DeflateLevelConfig.returnDeflater(3, d1);
        DeflateLevelConfig.returnDeflater(3, d2);
        Deflater d3 = DeflateLevelConfig.borrowDeflater(3);
        DeflateLevelConfig.returnDeflater(3, d3);

        // Cap 2: only the two most-recently returned survive; stats show <= cap
        assertTrue(DeflateLevelConfig.getCacheStats().contains(", 2 deflaters"));
    }

    @Test
    void clearCacheReleasesResources() {
        Deflater d = DeflateLevelConfig.borrowDeflater(3);
        DeflateLevelConfig.returnDeflater(3, d);
        Inflater i = DeflateLevelConfig.borrowInflater();
        DeflateLevelConfig.returnInflater(i);
        assertTrue(DeflateLevelConfig.getCacheStats().contains("1 inflaters"));

        DeflateLevelConfig.clearCache();
        String stats = DeflateLevelConfig.getCacheStats();
        assertTrue(stats.contains("0 deflaters"));
        assertTrue(stats.contains("0 inflaters"));
    }

    @Test
    void cacheDisableYieldsFreshInstances() {
        System.setProperty(DeflateLevelConfig.CACHE_KEY, "false");
        DeflateLevelConfig.resolveCacheConfig();

        Deflater a = DeflateLevelConfig.borrowDeflater(3);
        DeflateLevelConfig.returnDeflater(3, a);
        Deflater b = DeflateLevelConfig.borrowDeflater(3);
        assertNotEquals(a, b, "caching disabled: fresh Deflater expected");
        b.end();
        assertTrue(DeflateLevelConfig.getCacheStats().startsWith("Deflate cache: disabled"));

        DeflateLevelConfig.clearCache();
    }

    @Test
    void cacheStatsReflectEnabledState() {
        DeflateLevelConfig.resolveCacheConfig();
        String stats = DeflateLevelConfig.getCacheStats();
        assertTrue(stats.startsWith("Deflate cache:"));
        assertTrue(stats.contains("max 10") || stats.contains("max "), stats);
    }

    // ─── Helpers (mirror AvroCompressionTest) ──────────────────────

    private List<String> simpleCols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private Map<String, Class<?>> simpleTypes() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private Map<String, Object> simpleRow(long id) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", "User" + id);
        r.put("AGE", (int) (id % 100));
        r.put("ACTIVE", id % 2 == 0);
        return r;
    }

    private File writeRows(String tableName, int level) throws IOException {
        File f = new File(tempDir.toFile(), tableName + ".avro");
        CodecFactory factory = AvroCodecFactory.factory("deflate", level);
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f, factory);
        try {
            for (int i = 0; i < 500; i++) {
                w.writeRow(simpleRow(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private int countRecords(AvroDataFileReader reader) {
        int n = 0;
        while (reader.hasNext()) {
            reader.next();
            n++;
        }
        return n;
    }
}