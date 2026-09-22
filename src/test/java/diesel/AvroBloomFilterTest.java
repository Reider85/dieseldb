package diesel;

import diesel.storage.avro.AvroBloomFilter;
import diesel.storage.avro.AvroBloomFilterConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 87 AVRO per-block bloom filter tests: config resolution,
 * per-block build/query, false-positive behaviour, multi-block isolation
 * and sidecar persistence.
 */
@Tag("storage")
@StorageType("avro")
class AvroBloomFilterTest {

    private static final String[] PROP_KEYS = {
            "avro.bloom.enabled",
            "avro.bloom.bits.per.key",
            "avro.bloom.num.hashes",
            "avro.bloom.fpp",
            "avro.bloom.config.file"
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

    // ─── Config resolution ────────────────────────────────────────────

    @Test
    void defaultResolution() {
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertTrue(cfg.enabled());
        assertEquals(AvroBloomFilterConfig.DEFAULT_BITS_PER_KEY, cfg.bitsPerKey());
        assertEquals(AvroBloomFilterConfig.DEFAULT_NUM_HASHES, cfg.numHashes());
        assertEquals(AvroBloomFilterConfig.DEFAULT_FPP, cfg.fpp());
    }

    @Test
    void syspropOverrideEnabled() {
        System.setProperty("avro.bloom.enabled", "false");
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertFalse(cfg.enabled());
    }

    @Test
    void syspropOverrideBitsPerKey() {
        System.setProperty("avro.bloom.bits.per.key", "20");
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertEquals(20, cfg.bitsPerKey());
    }

    @Test
    void syspropOverrideNumHashes() {
        System.setProperty("avro.bloom.num.hashes", "10");
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertEquals(10, cfg.numHashes());
    }

    @Test
    void syspropOverrideFpp() {
        System.setProperty("avro.bloom.fpp", "0.001");
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertEquals(0.001, cfg.fpp());
    }

    @Test
    void invalidBitsPerKeyClamps() {
        System.setProperty("avro.bloom.bits.per.key", "-5");
        assertEquals(1, AvroBloomFilterConfig.resolve().bitsPerKey());
        System.setProperty("avro.bloom.bits.per.key", "200");
        assertEquals(64, AvroBloomFilterConfig.resolve().bitsPerKey());
    }

    @Test
    void invalidNumHashesClamps() {
        System.setProperty("avro.bloom.num.hashes", "0");
        assertEquals(1, AvroBloomFilterConfig.resolve().numHashes());
        System.setProperty("avro.bloom.num.hashes", "100");
        assertEquals(32, AvroBloomFilterConfig.resolve().numHashes());
    }

    @Test
    void invalidFppFallsBackToDefault() {
        System.setProperty("avro.bloom.fpp", "0");
        assertEquals(AvroBloomFilterConfig.DEFAULT_FPP, AvroBloomFilterConfig.resolve().fpp());
        System.setProperty("avro.bloom.fpp", "1.5");
        assertEquals(AvroBloomFilterConfig.DEFAULT_FPP, AvroBloomFilterConfig.resolve().fpp());
    }

    @Test
    void configKeyConstants() {
        assertEquals("avro.bloom.enabled", AvroBloomFilterConfig.ENABLED_KEY);
        assertEquals("avro.bloom.bits.per.key", AvroBloomFilterConfig.BITS_PER_KEY_KEY);
        assertEquals("avro.bloom.num.hashes", AvroBloomFilterConfig.NUM_HASHES_KEY);
        assertEquals("avro.bloom.fpp", AvroBloomFilterConfig.FPP_KEY);
    }

    @Test
    void configResolutionPriority_syspropOverConfigFile() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.bloom.enabled", "false");
            props.setProperty("avro.bloom.bits.per.key", "5");
            props.setProperty("avro.bloom.num.hashes", "3");
            props.setProperty("avro.bloom.fpp", "0.5");
            props.store(out, "test");
        }
        System.setProperty("avro.bloom.config.file", configFile.getAbsolutePath());
        System.setProperty("avro.bloom.bits.per.key", "30");

        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertFalse(cfg.enabled());
        assertEquals(30, cfg.bitsPerKey());
        assertEquals(3, cfg.numHashes());
        assertEquals(0.5, cfg.fpp());
    }

    @Test
    void configResolutionPriority_configFileOverDefault() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.bloom.bits.per.key", "25");
            props.setProperty("avro.bloom.num.hashes", "9");
            props.setProperty("avro.bloom.fpp", "0.05");
            props.store(out, "test");
        }
        System.setProperty("avro.bloom.config.file", configFile.getAbsolutePath());

        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        assertTrue(cfg.enabled());
        assertEquals(25, cfg.bitsPerKey());
        assertEquals(9, cfg.numHashes());
        assertEquals(0.05, cfg.fpp());
    }

    @Test
    void toStringContainsAllFields() {
        String str = AvroBloomFilterConfig.resolve().toString();
        assertTrue(str.contains("AvroBloomFilterConfig{"));
        assertTrue(str.contains("enabled="));
        assertTrue(str.contains("bitsPerKey="));
        assertTrue(str.contains("numHashes="));
        assertTrue(str.contains("fpp="));
    }

    @Test
    void bitSizeForScalesWithExpectedInsertions() {
        AvroBloomFilterConfig cfg = AvroBloomFilterConfig.resolve();
        int small = cfg.bitSizeFor(10);
        int large = cfg.bitSizeFor(1000);
        assertTrue(small >= 8);
        assertTrue(large > small);
        assertEquals(1000 * cfg.bitsPerKey(), cfg.bitSizeFor(1000));
    }

    // ─── Configuration-driven behaviour ───────────────────────────────

    @Test
    void createFromConfigFile() throws IOException {
        File configFile = tempDir.resolve("bf.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.bloom.enabled", "true");
            props.setProperty("avro.bloom.bits.per.key", "16");
            props.setProperty("avro.bloom.num.hashes", "5");
            props.store(out, "test");
        }
        AvroBloomFilter filter = AvroBloomFilter.create(configFile.getAbsolutePath());
        assertTrue(filter.isEnabled());
        assertEquals(16, filter.getBitsPerKey());
        assertEquals(5, filter.getNumHashes());
    }

    @Test
    void disabledFilterIgnoresWritesAndQueries() {
        System.setProperty("avro.bloom.enabled", "false");
        AvroBloomFilter filter = AvroBloomFilter.create();
        assertFalse(filter.isEnabled());
        filter.buildValues(0, List.of("a", "b", "c"));
        filter.put(1, "x");
        assertEquals(0, filter.getBlockCount());
        assertFalse(filter.mightContain(0, "a"));
        assertFalse(filter.hasBlock(0));
    }

    // ─── Per-block build and query ────────────────────────────────────

    @Test
    void buildValuesContainsAllInsertedValues() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        List<Object> values = List.of("alpha", "beta", "gamma", 42, 3.14, true);
        filter.buildValues(0, values);
        for (Object v : values) {
            assertTrue(filter.mightContain(0, v), "must contain inserted value: " + v);
        }
    }

    @Test
    void buildValuesRejectsAbsentValue() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("alpha", "beta", "gamma"));
        assertFalse(filter.mightContain(0, "zeta"));
    }

    @Test
    void noFalseNegativesAcrossLargeSet() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add("key-" + i);
        }
        filter.buildValues(0, values);
        for (Object v : values) {
            assertTrue(filter.mightContain(0, v));
        }
    }

    @Test
    void putIncrementalBuild() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.put(0, "first");
        filter.put(0, "second");
        assertTrue(filter.mightContain(0, "first"));
        assertTrue(filter.mightContain(0, "second"));
        assertFalse(filter.mightContain(0, "absent"));
        assertEquals(2, filter.getKeyCount(0));
    }

    @Test
    void nullValuesSkipped() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        List<Object> values = new ArrayList<>();
        values.add("a");
        values.add(null);
        values.add("b");
        filter.buildValues(0, values);
        assertEquals(2, filter.getKeyCount(0));
        assertFalse(filter.mightContain(0, null));
    }

    @Test
    void unknownBlockReturnsFalse() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a"));
        assertFalse(filter.mightContain(99, "a"));
        assertFalse(filter.mightContain(1, "a"));
    }

    @Test
    void objectTypeDoesNotMatterForPresence() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("123", 123L));
        assertTrue(filter.mightContain(0, "123"));
        assertTrue(filter.mightContain(0, 123L));
    }

    // ─── Multi-block isolation ────────────────────────────────────────

    @Test
    void multiBlockIsolation() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a", "b", "c"));
        filter.buildValues(1, List.of("x", "y", "z"));
        filter.buildValues(2, List.of("m", "n"));
        assertTrue(filter.mightContain(0, "a"));
        assertFalse(filter.mightContain(0, "x"));
        assertFalse(filter.mightContain(0, "m"));
        assertTrue(filter.mightContain(1, "z"));
        assertFalse(filter.mightContain(1, "a"));
        assertTrue(filter.mightContain(2, "n"));
        assertFalse(filter.mightContain(2, "a"));
        assertEquals(3, filter.getBlockCount());
        assertEquals(List.of(0, 1, 2), filter.getBlockIndexes());
    }

    @Test
    void rebuildReplacesBlockFilter() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("old", "another"));
        assertEquals(2, filter.getKeyCount(0));
        filter.buildValues(0, List.of("new"));
        assertTrue(filter.mightContain(0, "new"));
        assertEquals(1, filter.getKeyCount(0));
        assertEquals(1, filter.getBlockCount());
    }

    @Test
    void removeBlockAndClear() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a"));
        filter.buildValues(1, List.of("b"));
        filter.removeBlock(0);
        assertFalse(filter.hasBlock(0));
        assertTrue(filter.hasBlock(1));
        filter.clear();
        assertEquals(0, filter.getBlockCount());
        assertFalse(filter.mightContain(1, "b"));
    }

    @Test
    void perBlockStats() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a", "b", "c", "d"));
        assertTrue(filter.hasBlock(0));
        assertEquals(4, filter.getKeyCount(0));
        assertTrue(filter.getBitSize(0) >= 4L * AvroBloomFilterConfig.DEFAULT_BITS_PER_KEY);
        assertTrue(filter.getEstimatedFpp(0) >= 0.0);
        assertTrue(filter.getEstimatedFpp(0) < 0.1);
    }

    @Test
    void staticConfigAccessors() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        assertEquals(AvroBloomFilterConfig.DEFAULT_BITS_PER_KEY, filter.getBitsPerKey());
        assertEquals(AvroBloomFilterConfig.DEFAULT_NUM_HASHES, filter.getNumHashes());
        assertEquals(AvroBloomFilterConfig.DEFAULT_FPP, filter.getFpp());
        assertTrue(filter.isEnabled());
    }

    @Test
    void moreBitsLowerFalsePositiveRate() {
        List<Object> inserted = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            inserted.add("k-" + i);
        }
        List<Object> probes = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            probes.add("p-" + i);
        }
        AvroBloomFilterConfig lowConfig = AvroBloomFilterConfig.resolveFor(4, 7, 0.01, true);
        AvroBloomFilterConfig highConfig = AvroBloomFilterConfig.resolveFor(24, 7, 0.01, true);
        AvroBloomFilter low = AvroBloomFilter.create(lowConfig);
        AvroBloomFilter high = AvroBloomFilter.create(highConfig);
        low.buildValues(0, inserted);
        high.buildValues(0, inserted);
        int lowFp = 0;
        int highFp = 0;
        for (Object p : probes) {
            if (low.mightContain(0, p)) lowFp++;
            if (high.mightContain(0, p)) highFp++;
        }
        assertTrue(highFp < lowFp, "expected higher bits-per-key to reduce false positives: " + highFp + " vs " + lowFp);
    }

    @Test
    void falsePositiveRateBoundedUnder10Percent() {
        List<Object> inserted = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            inserted.add("key-" + i);
        }
        List<Object> probes = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            probes.add("probe-" + i);
        }
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, inserted);
        int fp = 0;
        for (Object p : probes) {
            if (filter.mightContain(0, p)) fp++;
        }
        assertTrue(fp < 200, "false positives should stay well under 10%: " + fp + " / " + probes.size());
    }

    @Test
    void mixedValueTypesWithinBlock() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        Random rnd = new Random(87);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            switch (i % 4) {
                case 0 -> values.add("s" + i);
                case 1 -> values.add(i);
                case 2 -> values.add(i * 0.5);
                case 3 -> values.add(i % 2 == 0);
            }
        }
        if (rnd.nextBoolean()) values.add(null);
        filter.buildValues(0, values);
        for (Object v : values) {
            if (v != null) {
                assertTrue(filter.mightContain(0, v));
            }
        }
    }

    // ─── Sidecar persistence ──────────────────────────────────────────

    @Test
    void sidecarRoundTrip() throws IOException {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a", "b", "c"));
        filter.buildValues(1, List.of("x", "y"));
        Path sidecar = tempDir.resolve("t.avro.bf");
        filter.saveToSidecar(sidecar, 1000L, 5000L);

        AvroBloomFilter loaded = AvroBloomFilter.loadFromSidecar(sidecar, 1000L, 5000L);
        assertNotNull(loaded);
        assertTrue(loaded.isEnabled());
        assertEquals(2, loaded.getBlockCount());
        assertTrue(loaded.mightContain(0, "a"));
        assertTrue(loaded.mightContain(0, "b"));
        assertTrue(loaded.mightContain(0, "c"));
        assertFalse(loaded.mightContain(0, "zzz"));
        assertTrue(loaded.mightContain(1, "x"));
        assertEquals(3, loaded.getKeyCount(0));
        assertEquals(2, loaded.getKeyCount(1));
    }

    @Test
    void sidecarPathConvention() {
        Path avro = Path.of("data", "USERS.avro");
        Path sidecar = AvroBloomFilter.sidecarPath(avro);
        assertEquals("USERS.avro.bf", sidecar.getFileName().toString());
    }

    @Test
    void sidecarStaleSizeRejected() throws IOException {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a"));
        Path sidecar = tempDir.resolve("t.avro.bf");
        filter.saveToSidecar(sidecar, 1000L, 5000L);
        assertNull(AvroBloomFilter.loadFromSidecar(sidecar, 999L, 5000L));
    }

    @Test
    void sidecarStaleModifiedRejected() throws IOException {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a"));
        Path sidecar = tempDir.resolve("t.avro.bf");
        filter.saveToSidecar(sidecar, 1000L, 5000L);
        assertNull(AvroBloomFilter.loadFromSidecar(sidecar, 1000L, 5001L));
    }

    @Test
    void sidecarMissingReturnsNull() throws IOException {
        Path sidecar = tempDir.resolve("missing.avro.bf");
        assertNull(AvroBloomFilter.loadFromSidecar(sidecar, 1000L, 5000L));
    }

    @Test
    void sidecarCorruptReturnsNull() throws IOException {
        Path sidecar = tempDir.resolve("corrupt.avro.bf");
        Files.writeString(sidecar, "VERSION=99\n---\ngarbage\n");
        assertNull(AvroBloomFilter.loadFromSidecar(sidecar, 1000L, 5000L));
    }

    @Test
    void sidecarDisabledFlagPreserved() throws IOException {
        System.setProperty("avro.bloom.enabled", "false");
        AvroBloomFilter filter = AvroBloomFilter.create();
        assertFalse(filter.isEnabled());
        Path sidecar = tempDir.resolve("t.avro.bf");
        filter.saveToSidecar(sidecar, 100L, 200L);
        AvroBloomFilter loaded = AvroBloomFilter.loadFromSidecar(sidecar, 100L, 200L);
        assertNotNull(loaded);
        assertFalse(loaded.isEnabled());
    }

    @Test
    void sidecarEmptyFilterRoundTrip() throws IOException {
        AvroBloomFilter filter = AvroBloomFilter.create();
        Path sidecar = tempDir.resolve("empty.avro.bf");
        filter.saveToSidecar(sidecar, 0L, 0L);
        AvroBloomFilter loaded = AvroBloomFilter.loadFromSidecar(sidecar, 0L, 0L);
        assertNotNull(loaded);
        assertEquals(0, loaded.getBlockCount());
        assertTrue(loaded.isEnabled());
    }

    @Test
    void sidecarDoubleRoundTripPreservesBits() throws IOException {
        AvroBloomFilter filter = AvroBloomFilter.create();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add("v-" + i);
        }
        filter.buildValues(0, values);
        Path sidecar1 = tempDir.resolve("t1.avro.bf");
        filter.saveToSidecar(sidecar1, 123L, 456L);
        AvroBloomFilter loaded1 = AvroBloomFilter.loadFromSidecar(sidecar1, 123L, 456L);
        assertNotNull(loaded1);
        Path sidecar2 = tempDir.resolve("t2.avro.bf");
        loaded1.saveToSidecar(sidecar2, 123L, 456L);
        AvroBloomFilter loaded2 = AvroBloomFilter.loadFromSidecar(sidecar2, 123L, 456L);
        assertNotNull(loaded2);
        for (Object v : values) {
            assertTrue(loaded2.mightContain(0, v), "round-tripped filter must keep value: " + v);
        }
    }

    @Test
    void toStringIncludesState() {
        AvroBloomFilter filter = AvroBloomFilter.create();
        filter.buildValues(0, List.of("a"));
        String str = filter.toString();
        assertTrue(str.contains("AvroBloomFilter{"));
        assertTrue(str.contains("blocks="));
        assertTrue(str.contains("enabled="));
    }
}