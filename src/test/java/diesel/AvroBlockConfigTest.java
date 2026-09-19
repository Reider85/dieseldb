package diesel;

import diesel.storage.avro.AvroBlockConfig;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 68 AVRO block configuration tests:
 * block size resolution, workload presets, sync interval,
 * sysprop → config.properties → default priority.
 */
@Tag("storage")
@StorageType("avro")
class AvroBlockConfigTest {

    private static final String[] PROP_KEYS = {
            "avro.block.size",
            "avro.block.workload",
            "avro.block.sync.interval",
            "avro.block.config.file"
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
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.blockSize());
        assertEquals(AvroBlockConfig.DEFAULT_WORKLOAD, cfg.workload());
        assertEquals(AvroBlockConfig.DEFAULT_SYNC_INTERVAL, cfg.syncInterval());
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void syspropOverrideBlockSize() {
        System.setProperty("avro.block.size", "33554432");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(33554432L, cfg.blockSize());
    }

    @Test
    void syspropOverrideWorkload() {
        System.setProperty("avro.block.workload", "streaming");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals("streaming", cfg.workload());
        assertEquals(AvroBlockConfig.STREAMING_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void syspropOverrideSyncInterval() {
        System.setProperty("avro.block.sync.interval", "33554432");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(33554432L, cfg.syncInterval());
    }

    @Test
    void workloadStreamingPreset() {
        System.setProperty("avro.block.workload", "streaming");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.WORKLOAD_STREAMING, cfg.workload());
        assertEquals(AvroBlockConfig.STREAMING_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void workloadRandomAccessPreset() {
        System.setProperty("avro.block.workload", "random_access");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.WORKLOAD_RANDOM_ACCESS, cfg.workload());
        assertEquals(AvroBlockConfig.RANDOM_ACCESS_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void workloadBatchPreset() {
        System.setProperty("avro.block.workload", "batch");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.WORKLOAD_BATCH, cfg.workload());
        assertEquals(AvroBlockConfig.BATCH_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void defaultWorkloadUsesExplicitBlockSize() {
        System.setProperty("avro.block.size", "12345678");
        System.setProperty("avro.block.workload", "default");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(12345678L, cfg.blockSize());
        assertEquals(12345678L, cfg.effectiveBlockSize());
    }

    @Test
    void invalidWorkloadFallsBackToDefault() {
        System.setProperty("avro.block.workload", "invalid_workload");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_WORKLOAD, cfg.workload());
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.effectiveBlockSize());
    }

    @Test
    void configKeyConstants() {
        assertEquals("avro.block.size", AvroBlockConfig.BLOCK_SIZE_KEY);
        assertEquals("avro.block.workload", AvroBlockConfig.WORKLOAD_KEY);
        assertEquals("avro.block.sync.interval", AvroBlockConfig.SYNC_INTERVAL_KEY);
    }

    @Test
    void configResolutionPriority_syspropOverConfigFile() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.block.size", "11111111");
            props.setProperty("avro.block.workload", "streaming");
            props.setProperty("avro.block.sync.interval", "22222222");
            props.store(out, "test");
        }
        System.setProperty("avro.block.config.file", configFile.getAbsolutePath());
        System.setProperty("avro.block.size", "99999999");

        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(99999999L, cfg.blockSize());
        assertEquals("streaming", cfg.workload());
        assertEquals(22222222L, cfg.syncInterval());
    }

    @Test
    void configResolutionPriority_configFileOverDefault() throws IOException {
        File configFile = tempDir.resolve("config.properties").toFile();
        try (OutputStream out = new FileOutputStream(configFile)) {
            Properties props = new Properties();
            props.setProperty("avro.block.size", "55555555");
            props.setProperty("avro.block.workload", "random_access");
            props.setProperty("avro.block.sync.interval", "66666666");
            props.store(out, "test");
        }
        System.setProperty("avro.block.config.file", configFile.getAbsolutePath());

        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(55555555L, cfg.blockSize());
        assertEquals("random_access", cfg.workload());
        assertEquals(66666666L, cfg.syncInterval());
    }

    @Test
    void invalidBlockSizeNegativeFallsBackToDefault() {
        System.setProperty("avro.block.size", "-1");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.blockSize());
    }

    @Test
    void invalidBlockSizeZeroFallsBackToDefault() {
        System.setProperty("avro.block.size", "0");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.blockSize());
    }

    @Test
    void invalidSyncIntervalNegativeFallsBackToDefault() {
        System.setProperty("avro.block.sync.interval", "-100");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_SYNC_INTERVAL, cfg.syncInterval());
    }

    @Test
    void toStringContainsAllFields() {
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        String str = cfg.toString();
        assertTrue(str.contains("AvroBlockConfig{"));
        assertTrue(str.contains("blockSize="));
        assertTrue(str.contains("workload="));
        assertTrue(str.contains("effectiveBlockSize="));
        assertTrue(str.contains("syncInterval="));
    }

    @Test
    void syncIntervalDefaultsToBlockSize() {
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.syncInterval());
    }

    @Test
    void syncIntervalCanBeSetIndependently() {
        System.setProperty("avro.block.sync.interval", "1048576");
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        assertEquals(1048576L, cfg.syncInterval());
        assertEquals(AvroBlockConfig.DEFAULT_BLOCK_SIZE, cfg.blockSize());
    }
}
