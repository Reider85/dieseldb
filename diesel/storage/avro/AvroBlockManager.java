package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Manages block-level metadata for Avro data files (Prompt 68).
 *
 * <p>Tracks per-block metadata — record count, compressed/uncompressed
 * size, CRC32 checksum, byte offsets — and provides aggregate statistics
 * (total records, total size, average compression ratio). The sync
 * marker interval is controlled by {@link AvroBlockConfig#syncInterval()}.
 *
 * <p>This class is thread-safe: all mutation methods are synchronized
 * and the returned block lists are immutable copies.
 *
 * @since Prompt 68
 */
public final class AvroBlockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBlockManager.class);

    /** The fixed 16-byte Avro sync marker size. */
    public static final int SYNC_SIZE = 16;

    private final byte[] syncMarker;
    private final String codecName;
    private final long headerSize;
    private final int configuredBlockSize;
    private final boolean syncEnabled;

    private final List<BlockMetadata> blocks = new ArrayList<>();
    private final AtomicInteger blockCount = new AtomicInteger(0);
    private final AtomicLong totalRecords = new AtomicLong(0);
    private final AtomicLong totalCompressed = new AtomicLong(0);
    private final AtomicLong totalUncompressed = new AtomicLong(0);

    /**
     * Immutable per-block metadata record.
     *
     * @param blockIndex       zero-based block sequence number
     * @param recordCount      records stored in this block
     * @param compressedSize   compressed payload bytes (after codec)
     * @param uncompressedSize raw payload bytes before compression
     * @param checksum         CRC32 of the uncompressed payload
     * @param startOffset      file offset where this block's payload starts
     * @param syncMarkerOffset file offset of the trailing sync marker
     */
    public record BlockMetadata(
            int blockIndex,
            long recordCount,
            long compressedSize,
            long uncompressedSize,
            long checksum,
            long startOffset,
            long syncMarkerOffset) {
    }

    /**
     * Creates a new block manager.
     *
     * @param syncMarker       the 16-byte sync marker from the file header
     * @param codecName        the codec name declared in the header ({@code "null"} for none)
     * @param headerSize       byte offset where the first data block begins
     * @param configuredBlockSize the configured block size in bytes
     * @param syncEnabled      whether sync markers are written between blocks
     */
    public AvroBlockManager(byte[] syncMarker, String codecName, long headerSize,
                            int configuredBlockSize, boolean syncEnabled) {
        this.syncMarker = syncMarker != null ? syncMarker.clone() : new byte[SYNC_SIZE];
        this.codecName = codecName != null ? codecName : "null";
        this.headerSize = headerSize;
        this.configuredBlockSize = configuredBlockSize;
        this.syncEnabled = syncEnabled;
    }

    /**
     * Records a completed block and returns its metadata.
     *
     * @param index            block index (0-based)
     * @param recordCount      number of records in this block
     * @param compressedSize   compressed payload size in bytes
     * @param uncompressedSize raw payload size in bytes
     * @param payload          the uncompressed payload bytes (used for CRC32)
     * @param startOffset      file offset of the payload start
     * @param syncMarkerOffset file offset of the trailing sync marker, or -1
     * @return the recorded {@link BlockMetadata}
     */
    public synchronized BlockMetadata addBlock(int index, long recordCount,
                                                long compressedSize, long uncompressedSize,
                                                byte[] payload, long startOffset,
                                                long syncMarkerOffset) {
        long checksum = computeCRC32(payload);
        BlockMetadata meta = new BlockMetadata(
                index, recordCount, compressedSize, uncompressedSize,
                checksum, startOffset, syncMarkerOffset);
        blocks.add(meta);
        blockCount.incrementAndGet();
        totalRecords.addAndGet(recordCount);
        totalCompressed.addAndGet(compressedSize);
        totalUncompressed.addAndGet(uncompressedSize);
        return meta;
    }

    /**
     * Returns an immutable snapshot of all recorded block metadata.
     */
    public synchronized List<BlockMetadata> getBlocks() {
        return Collections.unmodifiableList(new ArrayList<>(blocks));
    }

    /**
     * Returns the metadata for a specific block index, or {@code null}
     * if the index is out of range.
     */
    public synchronized BlockMetadata getBlock(int index) {
        if (index < 0 || index >= blocks.size()) {
            return null;
        }
        return blocks.get(index);
    }

    /** Total number of blocks recorded. */
    public int getBlockCount() {
        return blockCount.get();
    }

    /** Total record count across all blocks. */
    public long getTotalRecordCount() {
        return totalRecords.get();
    }

    /** Total compressed size across all blocks (bytes). */
    public long getTotalCompressedSize() {
        return totalCompressed.get();
    }

    /** Total uncompressed size across all blocks (bytes). */
    public long getTotalUncompressedSize() {
        return totalUncompressed.get();
    }

    /**
     * Average compression ratio across all blocks:
     * total uncompressed / total compressed.
     * Returns 1.0 when no data has been recorded or total compressed is 0.
     */
    public double getAverageCompressionRatio() {
        long uncompressed = totalUncompressed.get();
        long compressed = totalCompressed.get();
        if (compressed == 0) {
            return 1.0;
        }
        return (double) uncompressed / compressed;
    }

    /** Returns the file header size in bytes (where the first data block begins). */
    public long getHeaderSize() {
        return headerSize;
    }

    /** Returns whether sync markers are enabled. */
    public boolean isSyncEnabled() {
        return syncEnabled;
    }

    /** Returns the configured block size in bytes. */
    public int getConfiguredBlockSize() {
        return configuredBlockSize;
    }

    /**
     * Returns a human-readable summary of the block metadata.
     */
    public synchronized String getFileSummary() {
        return String.format(
                "AvroBlockManager{blocks=%d, records=%d, compressed=%d bytes, "
                        + "uncompressed=%d bytes, ratio=%.2f, header=%d bytes, syncEnabled=%s}",
                blocks.size(), totalRecords.get(), totalCompressed.get(),
                totalUncompressed.get(), getAverageCompressionRatio(),
                headerSize, syncEnabled);
    }

    /**
     * Returns {@code true} if a sync marker should be inserted after
     * writing {@code bytesSinceLastSync} bytes of payload.
     *
     * @param bytesSinceLastSync bytes written since the last sync marker
     * @param syncInterval       the configured sync interval in bytes
     */
    public static boolean shouldInsertSyncMarker(long bytesSinceLastSync, long syncInterval) {
        return syncInterval > 0 && bytesSinceLastSync >= syncInterval;
    }

    /**
     * Computes the CRC32 checksum of the given byte array.
     *
     * @param data the payload bytes (may be {@code null} or empty)
     * @return CRC32 value (0 for null/empty input)
     */
    public static long computeCRC32(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    /**
     * Resets all accumulated block metadata to zero.
     */
    public synchronized void reset() {
        blocks.clear();
        blockCount.set(0);
        totalRecords.set(0);
        totalCompressed.set(0);
        totalUncompressed.set(0);
    }

    /** Returns a resolved config based on current system properties and config.properties. */
    public static AvroBlockConfig config() {
        return AvroBlockConfig.resolve();
    }
}
