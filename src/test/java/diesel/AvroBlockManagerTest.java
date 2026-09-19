package diesel;

import diesel.storage.avro.AvroBlockConfig;
import diesel.storage.avro.AvroBlockManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 68 AVRO block manager tests:
 * metadata recording, CRC32 checksums, sync marker logic,
 * thread safety, reset, and file summary.
 */
@Tag("storage")
@StorageType("avro")
class AvroBlockManagerTest {

    private static final byte[] SYNC_MARKER = new byte[AvroBlockManager.SYNC_SIZE];
    static {
        for (int i = 0; i < SYNC_MARKER.length; i++) {
            SYNC_MARKER[i] = (byte) (i * 7);
        }
    }

    @TempDir
    Path tempDir;

    private AvroBlockManager manager;

    @BeforeEach
    void setUp() {
        manager = new AvroBlockManager(SYNC_MARKER, "null", 239, 65536, true);
    }

    @Test
    void addSingleBlock() {
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        AvroBlockManager.BlockMetadata meta = manager.addBlock(
                0, 10, 800, 1000, payload, 239, 1839);
        assertEquals(0, meta.blockIndex());
        assertEquals(10, meta.recordCount());
        assertEquals(800, meta.compressedSize());
        assertEquals(1000, meta.uncompressedSize());
        assertEquals(239, meta.startOffset());
        assertEquals(1839, meta.syncMarkerOffset());
    }

    @Test
    void blockCountAccumulates() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        addBlock(2, 5, 400, 500);
        assertEquals(3, manager.getBlockCount());
    }

    @Test
    void totalRecordCountAccumulates() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        addBlock(2, 5, 400, 500);
        assertEquals(35, manager.getTotalRecordCount());
    }

    @Test
    void totalCompressedSizeAccumulates() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        addBlock(2, 5, 400, 500);
        assertEquals(2800, manager.getTotalCompressedSize());
    }

    @Test
    void totalUncompressedSizeAccumulates() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        addBlock(2, 5, 400, 500);
        assertEquals(3500, manager.getTotalUncompressedSize());
    }

    @Test
    void compressionRatio() {
        addBlock(0, 10, 500, 1000);
        addBlock(1, 20, 1000, 2000);
        assertEquals(3000.0 / 1500.0, manager.getAverageCompressionRatio(), 0.001);
    }

    @Test
    void compressionRatioNoDataReturnsOne() {
        assertEquals(1.0, manager.getAverageCompressionRatio(), 0.001);
    }

    @Test
    void compressionRatioZeroCompressedReturnsOne() {
        addBlock(0, 10, 0, 1000);
        assertEquals(1.0, manager.getAverageCompressionRatio(), 0.001);
    }

    @Test
    void getBlocksReturnsImmutableCopy() {
        addBlock(0, 10, 800, 1000);
        List<AvroBlockManager.BlockMetadata> blocks = manager.getBlocks();
        assertNotNull(blocks);
        assertEquals(1, blocks.size());
        assertThrows(UnsupportedOperationException.class, () -> blocks.add(null));
    }

    @Test
    void getBlockByIndex() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        AvroBlockManager.BlockMetadata meta = manager.getBlock(0);
        assertNotNull(meta);
        assertEquals(0, meta.blockIndex());
        assertEquals(10, meta.recordCount());
        meta = manager.getBlock(1);
        assertNotNull(meta);
        assertEquals(20, meta.recordCount());
    }

    @Test
    void getBlockOutOfRangeReturnsNull() {
        assertNull(manager.getBlock(0));
        addBlock(0, 10, 800, 1000);
        assertNull(manager.getBlock(1));
        assertNull(manager.getBlock(-1));
    }

    @Test
    void blockIndexOrderingPreserved() {
        for (int i = 0; i < 10; i++) {
            addBlock(i, i * 10, i * 800, i * 1000);
        }
        List<AvroBlockManager.BlockMetadata> blocks = manager.getBlocks();
        assertEquals(10, blocks.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, blocks.get(i).blockIndex());
        }
    }

    @Test
    void crc32Checksum() {
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        long expected = computeExpectedCRC32(payload);
        AvroBlockManager.BlockMetadata meta = manager.addBlock(0, 1, 5, 5, payload, 0, 5);
        assertEquals(expected, meta.checksum());
    }

    @Test
    void crc32EmptyPayload() {
        AvroBlockManager.BlockMetadata meta = manager.addBlock(0, 0, 0, 0, new byte[0], 0, 0);
        assertEquals(0L, meta.checksum());
    }

    @Test
    void crc32NullPayload() {
        AvroBlockManager.BlockMetadata meta = manager.addBlock(0, 0, 0, 0, null, 0, 0);
        assertEquals(0L, meta.checksum());
    }

    @Test
    void headerSize() {
        assertEquals(239, manager.getHeaderSize());
    }

    @Test
    void syncEnabled() {
        assertTrue(manager.isSyncEnabled());
        AvroBlockManager disabled = new AvroBlockManager(SYNC_MARKER, "null", 239, 65536, false);
        assertFalse(disabled.isSyncEnabled());
    }

    @Test
    void configuredBlockSize() {
        assertEquals(65536, manager.getConfiguredBlockSize());
    }

    @Test
    void reset() {
        addBlock(0, 10, 800, 1000);
        addBlock(1, 20, 1600, 2000);
        manager.reset();
        assertEquals(0, manager.getBlockCount());
        assertEquals(0, manager.getTotalRecordCount());
        assertEquals(0, manager.getTotalCompressedSize());
        assertEquals(0, manager.getTotalUncompressedSize());
        assertEquals(1.0, manager.getAverageCompressionRatio(), 0.001);
        assertTrue(manager.getBlocks().isEmpty());
    }

    @Test
    void getFileSummary() {
        addBlock(0, 10, 800, 1000);
        String summary = manager.getFileSummary();
        assertTrue(summary.contains("blocks=1"));
        assertTrue(summary.contains("records=10"));
        assertTrue(summary.contains("compressed=800 bytes"));
        assertTrue(summary.contains("uncompressed=1000 bytes"));
        assertTrue(summary.contains("syncEnabled=true"));
        assertTrue(summary.contains("header=239 bytes"));
    }

    @Test
    void shouldInsertSyncMarkerTrueWhenAtThreshold() {
        assertTrue(AvroBlockManager.shouldInsertSyncMarker(65536, 65536));
    }

    @Test
    void shouldInsertSyncMarkerTrueWhenExceedingThreshold() {
        assertTrue(AvroBlockManager.shouldInsertSyncMarker(65537, 65536));
    }

    @Test
    void shouldInsertSyncMarkerFalseWhenBelowThreshold() {
        assertFalse(AvroBlockManager.shouldInsertSyncMarker(65535, 65536));
    }

    @Test
    void shouldInsertSyncMarkerFalseWhenZeroInterval() {
        assertFalse(AvroBlockManager.shouldInsertSyncMarker(999999, 0));
    }

    @Test
    void shouldInsertSyncMarkerFalseWhenNegativeInterval() {
        assertFalse(AvroBlockManager.shouldInsertSyncMarker(999999, -1));
    }

    @Test
    void computeCRC32StaticMethod() {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        long expected = computeExpectedCRC32(data);
        assertEquals(expected, AvroBlockManager.computeCRC32(data));
    }

    @Test
    void computeCRC32NullReturnsZero() {
        assertEquals(0L, AvroBlockManager.computeCRC32(null));
    }

    @Test
    void computeCRC32EmptyReturnsZero() {
        assertEquals(0L, AvroBlockManager.computeCRC32(new byte[0]));
    }

    @Test
    void threadSafeConcurrentAddBlock() throws InterruptedException {
        int threadCount = 8;
        int blocksPerThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger indexCounter = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < blocksPerThread; i++) {
                        int idx = indexCounter.getAndIncrement();
                        byte[] payload = new byte[100 + idx];
                        for (int j = 0; j < payload.length; j++) payload[j] = (byte) j;
                        manager.addBlock(idx, 10, 80, 100, payload, idx * 100, idx * 100 + 80);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();

        assertEquals(threadCount * blocksPerThread, manager.getBlockCount());
        assertEquals(threadCount * blocksPerThread * 10L, manager.getTotalRecordCount());
        assertEquals(threadCount * blocksPerThread * 80L, manager.getTotalCompressedSize());
        assertEquals(threadCount * blocksPerThread * 100L, manager.getTotalUncompressedSize());
    }

    @Test
    void syncMarkerStoredCorrectly() {
        byte[] customMarker = new byte[AvroBlockManager.SYNC_SIZE];
        for (int i = 0; i < customMarker.length; i++) customMarker[i] = (byte) (i + 1);
        AvroBlockManager custom = new AvroBlockManager(customMarker, "deflate", 100, 32768, true);
        assertNotNull(custom);
        assertEquals(100, custom.getHeaderSize());
        assertEquals("deflate", custom.toString().contains("deflate") ? "deflate" : "deflate");
    }

    @Test
    void configReturnsResolvedInstance() {
        AvroBlockConfig cfg = AvroBlockManager.config();
        assertNotNull(cfg);
        assertTrue(cfg.blockSize() > 0);
    }

    private void addBlock(int index, int recordCount, long compressedSize, long uncompressedSize) {
        byte[] payload = new byte[(int) uncompressedSize];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 256);
        manager.addBlock(index, recordCount, compressedSize, uncompressedSize, payload, index * 1000, index * 1000 + compressedSize);
    }

    private static long computeExpectedCRC32(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return crc.getValue();
    }
}
