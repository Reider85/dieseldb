package diesel;

import diesel.storage.avro.AvroBufferConfig;
import diesel.storage.avro.AvroBufferConfig.FlushStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 78 AVRO buffer configuration tests:
 * write/read buffer sizes, flush strategies, time interval, zero-copy flag,
 * sysprop → config.properties → default priority.
 */
@Tag("storage")
@StorageType("avro")
class AvroBufferConfigTest {

    private static final String[] PROP_KEYS = {
            "avro.buffer.write.size",
            "avro.buffer.read.size",
            "avro.buffer.flush.strategy",
            "avro.buffer.flush.interval.ms",
            "avro.buffer.zero.copy",
            "avro.buffer.config.file"
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

    @Test
    void defaultResolution() {
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(AvroBufferConfig.DEFAULT_WRITE_SIZE, cfg.writeBufferSize());
        assertEquals(AvroBufferConfig.DEFAULT_READ_SIZE, cfg.readBufferSize());
        assertEquals(FlushStrategy.SIZE, cfg.flushStrategy());
        assertEquals(AvroBufferConfig.DEFAULT_FLUSH_INTERVAL_MS, cfg.flushIntervalMs());
        assertTrue(cfg.zeroCopyEnabled());
    }

    @Test
    void configKeyConstants() {
        assertEquals("avro.buffer.write.size", AvroBufferConfig.WRITE_SIZE_KEY);
        assertEquals("avro.buffer.read.size", AvroBufferConfig.READ_SIZE_KEY);
        assertEquals("avro.buffer.flush.strategy", AvroBufferConfig.FLUSH_STRATEGY_KEY);
        assertEquals("avro.buffer.flush.interval.ms", AvroBufferConfig.FLUSH_INTERVAL_KEY);
        assertEquals("avro.buffer.zero.copy", AvroBufferConfig.ZERO_COPY_KEY);
    }

    @Test
    void syspropOverrideWriteSize() {
        System.setProperty("avro.buffer.write.size", "131072");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(131072, cfg.writeBufferSize());
        assertEquals(AvroBufferConfig.DEFAULT_READ_SIZE, cfg.readBufferSize());
    }

    @Test
    void syspropOverrideReadSize() {
        System.setProperty("avro.buffer.read.size", "16384");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(16384, cfg.readBufferSize());
        assertEquals(AvroBufferConfig.DEFAULT_WRITE_SIZE, cfg.writeBufferSize());
    }

    @Test
    void syspropOverrideTimeStrategy() {
        System.setProperty("avro.buffer.flush.strategy", "time");
        System.setProperty("avro.buffer.flush.interval.ms", "250");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(FlushStrategy.TIME, cfg.flushStrategy());
        assertEquals(250L, cfg.flushIntervalMs());
    }

    @Test
    void syspropOverrideForcedStrategy() {
        System.setProperty("avro.buffer.flush.strategy", "forced");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(FlushStrategy.FORCED, cfg.flushStrategy());
    }

    @Test
    void syspropOverrideZeroCopyOff() {
        System.setProperty("avro.buffer.zero.copy", "false");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertFalse(cfg.zeroCopyEnabled());
    }

    @Test
    void invalidWriteSizeFallsBackToDefault() {
        System.setProperty("avro.buffer.write.size", "-1");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(AvroBufferConfig.DEFAULT_WRITE_SIZE, cfg.writeBufferSize());
    }

    @Test
    void invalidWriteSizeZeroFallsBackToDefault() {
        System.setProperty("avro.buffer.write.size", "0");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(AvroBufferConfig.DEFAULT_WRITE_SIZE, cfg.writeBufferSize());
    }

    @Test
    void invalidReadSizeFallsBackToDefault() {
        System.setProperty("avro.buffer.read.size", "abc");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(AvroBufferConfig.DEFAULT_READ_SIZE, cfg.readBufferSize());
    }

    @Test
    void invalidStrategyFallsBackToSize() {
        System.setProperty("avro.buffer.flush.strategy", "34234234");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(FlushStrategy.SIZE, cfg.flushStrategy());
    }

    @Test
    void caseInsensitiveStrategyNames() {
        System.setProperty("avro.buffer.flush.strategy", "FORCED");
        assertEquals(FlushStrategy.FORCED, AvroBufferConfig.resolve().flushStrategy());
        System.setProperty("avro.buffer.flush.strategy", "Time");
        assertEquals(FlushStrategy.TIME, AvroBufferConfig.resolve().flushStrategy());
        System.setProperty("avro.buffer.flush.strategy", "SIZE");
        assertEquals(FlushStrategy.SIZE, AvroBufferConfig.resolve().flushStrategy());
    }

    @Test
    void invalidIntervalFallsBackToDefault() {
        System.setProperty("avro.buffer.flush.interval.ms", "0");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(AvroBufferConfig.DEFAULT_FLUSH_INTERVAL_MS, cfg.flushIntervalMs());
    }

    @Test
    void invalidZeroCopyBooleanFallsBackToDefault() {
        System.setProperty("avro.buffer.zero.copy", "banana");
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertTrue(cfg.zeroCopyEnabled());
    }

    @Test
    void configResolutionPriority_syspropOverConfigFile() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.buffer.write.size", "111");
            props.setProperty("avro.buffer.read.size", "222");
            props.setProperty("avro.buffer.flush.strategy", "time");
            props.setProperty("avro.buffer.flush.interval.ms", "333");
            props.setProperty("avro.buffer.zero.copy", "false");
            props.store(out, "test");
        }
        System.setProperty("avro.buffer.config.file", configFile.getAbsolutePath());
        System.setProperty("avro.buffer.write.size", "9999");

        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(9999, cfg.writeBufferSize());
        assertEquals(222, cfg.readBufferSize());
        assertEquals(FlushStrategy.TIME, cfg.flushStrategy());
        assertEquals(333L, cfg.flushIntervalMs());
        assertFalse(cfg.zeroCopyEnabled());
    }

    @Test
    void configResolutionPriority_configFileOverDefault() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.buffer.write.size", "555");
            props.setProperty("avro.buffer.read.size", "666");
            props.setProperty("avro.buffer.flush.strategy", "forced");
            props.setProperty("avro.buffer.flush.interval.ms", "777");
            props.setProperty("avro.buffer.zero.copy", "on");
            props.store(out, "test");
        }
        System.setProperty("avro.buffer.config.file", configFile.getAbsolutePath());

        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        assertEquals(555, cfg.writeBufferSize());
        assertEquals(666, cfg.readBufferSize());
        assertEquals(FlushStrategy.FORCED, cfg.flushStrategy());
        assertEquals(777L, cfg.flushIntervalMs());
        assertTrue(cfg.zeroCopyEnabled());
    }

    @Test
    void strategyEnumValues() {
        assertNotNull(FlushStrategy.valueOf("SIZE"));
        assertNotNull(FlushStrategy.valueOf("TIME"));
        assertNotNull(FlushStrategy.valueOf("FORCED"));
        assertEquals(3, FlushStrategy.values().length);
    }

    @Test
    void toStringContainsAllFields() {
        AvroBufferConfig cfg = AvroBufferConfig.resolve();
        String str = cfg.toString();
        assertTrue(str.contains("AvroBufferConfig{"));
        assertTrue(str.contains("writeBufferSize="));
        assertTrue(str.contains("readBufferSize="));
        assertTrue(str.contains("flushStrategy="));
        assertTrue(str.contains("flushIntervalMs="));
        assertTrue(str.contains("zeroCopyEnabled="));
    }

    @Test
    void resolveNeverNull() {
        assertNotNull(AvroBufferConfig.resolve());
    }

    @Test
    void negativeIntervalRejected() {
        System.setProperty("avro.buffer.flush.interval.ms", "-10");
        assertEquals(AvroBufferConfig.DEFAULT_FLUSH_INTERVAL_MS, AvroBufferConfig.resolve().flushIntervalMs());
    }
}