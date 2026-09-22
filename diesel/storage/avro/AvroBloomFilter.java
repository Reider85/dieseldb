package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-block Bloom filters for AVRO data files (Prompt 87).
 *
 * <p>Each data block of an Avro file gets its own compact Bloom filter
 * over every (non-null) value found in the block's rows, so a point lookup
 * can skip an entire block — and its associated I/O — when the filter
 * reports the value is <em>definitely not</em> present. False positives
 * are possible (the block may still be read); false negatives are
 * impossible.
 *
 * <p>Filters are keyed by block index and stored in a thread-safe
 * {@link ConcurrentHashMap}. A filter is either built in one pass from a
 * known value collection ({@link #buildValues}) — sizing the bit array
 * exactly to the expected distinct keys — or grown incrementally via
 * {@link #put} with an initial size estimate.
 *
 * <p>The false-positive rate is configurable:
 * <ul>
 *   <li>{@code avro.bloom.enabled} — master switch (default {@code true})</li>
 *   <li>{@code avro.bloom.bits.per.key} — bits per distinct key
 *       (default {@code 10})</li>
 *   <li>{@code avro.bloom.num.hashes} — hash functions (default {@code 7})</li>
 *   <li>{@code avro.bloom.fpp} — target false-positive probability
 *       (default {@code 0.01}, informational; sizing uses bits-per-key)</li>
 * </ul>
 *
 * <p>Persistence uses a {@code .bf} sidecar file (text format with a
 * metadata header stamping the data file size/mtime, mirroring
 * {@link AvroPrimaryKeyIndex}), so stale sidecars are detected and
 * rejected on load.
 *
 * @since Prompt 87
 */
public final class AvroBloomFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBloomFilter.class);

    /** Sidecar file extension for bloom filter persistence. */
    public static final String SIDECAR_EXTENSION = ".bf";
    private static final int SIDECAR_VERSION = 1;
    private static final int DEFAULT_INITIAL_KEYS = 1024;

    // ─── State ──────────────────────────────────────────────────────
    private final boolean enabled;
    private final int bitsPerKey;
    private final int numHashes;
    private final double fpp;

    private final Map<Integer, BlockFilter> filters = new ConcurrentHashMap<>();

    private AvroBloomFilter(AvroBloomFilterConfig config) {
        this.enabled = config.enabled();
        this.bitsPerKey = config.bitsPerKey();
        this.numHashes = config.numHashes();
        this.fpp = config.fpp();
    }

    // ─── Factories ─────────────────────────────────────────────────

    /**
     * Creates a bloom filter configured from the current system properties
     * and {@code config.properties} (resolved per call).
     */
    public static AvroBloomFilter create() {
        return new AvroBloomFilter(AvroBloomFilterConfig.resolve());
    }

    /**
     * Creates a bloom filter from an explicit configuration record.
     *
     * @param config the resolved configuration (may be {@code null} to
     *               fall back to {@code AvroBloomFilterConfig.resolve()})
     */
    public static AvroBloomFilter create(AvroBloomFilterConfig config) {
        return new AvroBloomFilter(config != null ? config : AvroBloomFilterConfig.resolve());
    }

    /**
     * Creates a bloom filter with a test-config override file.
     *
     * @param configFilePath a path to a {@code config.properties} used to
     *                       resolve the bloom settings (or {@code null}
     *                       for the default file)
     */
    public static AvroBloomFilter create(String configFilePath) {
        String prev = System.getProperty(AvroBloomFilterConfig.CONFIG_FILE_KEY);
        try {
            if (configFilePath != null) {
                System.setProperty(AvroBloomFilterConfig.CONFIG_FILE_KEY, configFilePath);
            }
            return create();
        } finally {
            if (prev == null) {
                System.clearProperty(AvroBloomFilterConfig.CONFIG_FILE_KEY);
            } else {
                System.setProperty(AvroBloomFilterConfig.CONFIG_FILE_KEY, prev);
            }
        }
    }

    // ─── Config accessors ───────────────────────────────────────────

    /** Whether the bloom filter is enabled via configuration. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Bits allocated per distinct key. */
    public int getBitsPerKey() {
        return bitsPerKey;
    }

    /** Number of hash functions per filter. */
    public int getNumHashes() {
        return numHashes;
    }

    /** Target false-positive probability. */
    public double getFpp() {
        return fpp;
    }

    // ─── Build / insert ─────────────────────────────────────────────

    /**
     * Builds the filter for a block from every (non-null) value in the
     * given collection, sizing the bit array to the distinct value count.
     * Replaces any existing filter for that block.
     *
     * @param blockIndex the zero-based block index
     * @param values     the values contained in the block (null values are skipped)
     */
    public synchronized void buildValues(int blockIndex, Collection<Object> values) {
        if (!enabled) return;
        if (values == null) {
            filters.remove(blockIndex);
            return;
        }
        List<Object> nonNull = new ArrayList<>(values.size());
        for (Object v : values) {
            if (v != null) nonNull.add(v);
        }
        int expected = Math.max(1, nonNull.size());
        BlockFilter filter = BlockFilter.create(expected, bitsPerKey, numHashes);
        for (Object v : nonNull) {
            filter.put(v);
        }
        filter.keyCount.set(nonNull.size());
        filters.put(blockIndex, filter);
        LOGGER.debug("AvroBloomFilter built block {} from {} values ({} distinct estimated)",
                blockIndex, values.size(), nonNull.size());
    }

    /**
     * Incrementally adds a single non-null value to a block's filter,
     * creating the filter with an initial size estimate on first use.
     *
     * @param blockIndex the zero-based block index
     * @param value      the value to add (null values are ignored)
     */
    public void put(int blockIndex, Object value) {
        if (!enabled || value == null) return;
        BlockFilter filter = filters.computeIfAbsent(blockIndex,
                k -> BlockFilter.create(DEFAULT_INITIAL_KEYS, bitsPerKey, numHashes));
        filter.put(value);
        filter.keyCount.incrementAndGet();
    }

    /**
     * Returns {@code true} when the block <em>might</em> contain the value.
     * A false positive is possible; a false negative is impossible.
     * Returns {@code false} for disabled filters, null values and
     * unknown/absent blocks.
     *
     * @param blockIndex the zero-based block index
     * @param value      the looked-up value
     */
    public boolean mightContain(int blockIndex, Object value) {
        if (!enabled || value == null) return false;
        BlockFilter filter = filters.get(blockIndex);
        return filter != null && filter.mightContain(value);
    }

    // ─── Block management ───────────────────────────────────────────

    /** Whether a filter exists for the given block index. */
    public boolean hasBlock(int blockIndex) {
        return filters.containsKey(blockIndex);
    }

    /** Number of blocks that currently have a filter. */
    public int getBlockCount() {
        return filters.size();
    }

    /** Returns the sorted list of block indexes that have a filter. */
    public List<Integer> getBlockIndexes() {
        List<Integer> list = new ArrayList<>(filters.keySet());
        Collections.sort(list);
        return list;
    }

    /** Removes the filter for a single block. */
    public void removeBlock(int blockIndex) {
        filters.remove(blockIndex);
    }

    /** Removes all filters. */
    public void clear() {
        filters.clear();
    }

    // ─── Per-block stats ────────────────────────────────────────────

    /** Number of distinct values inserted for the block, or {@code 0}. */
    public long getKeyCount(int blockIndex) {
        BlockFilter filter = filters.get(blockIndex);
        return filter == null ? 0 : filter.keyCount.get();
    }

    /** Bit-array size (bits) for the block's filter, or {@code 0}. */
    public long getBitSize(int blockIndex) {
        BlockFilter filter = filters.get(blockIndex);
        return filter == null ? 0 : filter.bitSize;
    }

    /**
     * The estimated false-positive probability of the block's filter,
     * computed from the current distinct-key count. Returns {@code -1}
     * for absent blocks.
     *
     * @param blockIndex the zero-based block index
     */
    public double getEstimatedFpp(int blockIndex) {
        BlockFilter filter = filters.get(blockIndex);
        return filter == null ? -1.0 : filter.estimatedFpp();
    }

    // ─── Persistence ────────────────────────────────────────────────

    /**
     * Returns the sidecar file path for this bloom filter.
     *
     * @param avroFilePath the path to the .avro data file
     */
    public static Path sidecarPath(Path avroFilePath) {
        return avroFilePath.resolveSibling(avroFilePath.getFileName() + SIDECAR_EXTENSION);
    }

    /**
     * Saves all block filters to a sidecar file via a temp+rename atomic
     * write. The data file size and last-modified stamp are recorded so a
     * stale sidecar can be rejected on load.
     *
     * @param sidecarPath      the target sidecar path
     * @param dataFileSize     the .avro data file size in bytes (validation stamp)
     * @param dataFileModified the .avro data file last-modified millis (validation stamp)
     */
    public void saveToSidecar(Path sidecarPath, long dataFileSize, long dataFileModified) throws IOException {
        File parent = sidecarPath.getParent() != null ? sidecarPath.getParent().toFile() : null;
        if (parent != null) parent.mkdirs();
        Path tmp = sidecarPath.resolveSibling(sidecarPath.getFileName() + ".tmp");
        List<Integer> indexes = getBlockIndexes();
        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            w.write("VERSION=" + SIDECAR_VERSION);
            w.newLine();
            w.write("ENABLED=" + enabled);
            w.newLine();
            w.write("BITS_PER_KEY=" + bitsPerKey);
            w.newLine();
            w.write("NUM_HASHES=" + numHashes);
            w.newLine();
            w.write("FPP=" + fpp);
            w.newLine();
            w.write("DATA_FILE_SIZE=" + dataFileSize);
            w.newLine();
            w.write("DATA_FILE_MODIFIED=" + dataFileModified);
            w.newLine();
            w.write("BLOCK_COUNT=" + indexes.size());
            w.newLine();
            w.write("---");
            w.newLine();
            for (Integer index : indexes) {
                BlockFilter filter = filters.get(index);
                if (filter == null) continue;
                w.write("BLOCK=" + index);
                w.newLine();
                w.write("KEYS=" + filter.keyCount.get());
                w.newLine();
                w.write("BIT_SIZE=" + filter.bitSize);
                w.newLine();
                w.write("BIT_BYTES=" + filter.bits.toByteArray().length);
                w.newLine();
                w.write("BITS=" + Base64.getEncoder().encodeToString(filter.bits.toByteArray()));
                w.newLine();
            }
        }
        Files.move(tmp, sidecarPath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Loads a bloom filter from a sidecar file. Returns {@code null}
     * when the sidecar is missing, produced for a different data file
     * (size/mtime mismatch) or corrupt.
     *
     * @param sidecarPath      the sidecar file path
     * @param dataFileSize     the current .avro data file size
     * @param dataFileModified the current .avro data file last-modified millis
     * @return the loaded filter, or {@code null} on mismatch/corruption
     */
    public static AvroBloomFilter loadFromSidecar(Path sidecarPath,
                                                  long dataFileSize,
                                                  long dataFileModified) {
        if (!Files.exists(sidecarPath)) return null;
        try (BufferedReader r = Files.newBufferedReader(sidecarPath, StandardCharsets.UTF_8)) {
            String line;
            boolean enabled = true;
            int loadedBitsPerKey = AvroBloomFilterConfig.DEFAULT_BITS_PER_KEY;
            int loadedNumHashes = AvroBloomFilterConfig.DEFAULT_NUM_HASHES;
            double loadedFpp = AvroBloomFilterConfig.DEFAULT_FPP;
            long fileSize = -2;
            long fileModified = -2;
            int expectedBlocks = -1;
            boolean reachedData = false;
            List<LoadedBlock> loaded = new ArrayList<>();
            while ((line = r.readLine()) != null) {
                if (line.equals("---")) {
                    reachedData = true;
                    continue;
                }
                if (!reachedData) {
                    if (line.startsWith("VERSION=")) {
                        int version = Integer.parseInt(line.substring(8));
                        if (version != SIDECAR_VERSION) return null;
                    } else if (line.startsWith("ENABLED=")) {
                        enabled = Boolean.parseBoolean(line.substring(8));
                    } else if (line.startsWith("BITS_PER_KEY=")) {
                        loadedBitsPerKey = Integer.parseInt(line.substring(13));
                    } else if (line.startsWith("NUM_HASHES=")) {
                        loadedNumHashes = Integer.parseInt(line.substring(11));
                    } else if (line.startsWith("FPP=")) {
                        loadedFpp = Double.parseDouble(line.substring(4));
                    } else if (line.startsWith("DATA_FILE_SIZE=")) {
                        fileSize = Long.parseLong(line.substring(15));
                    } else if (line.startsWith("DATA_FILE_MODIFIED=")) {
                        fileModified = Long.parseLong(line.substring(19));
                    } else if (line.startsWith("BLOCK_COUNT=")) {
                        expectedBlocks = Integer.parseInt(line.substring(12));
                    }
                    continue;
                }
                if (line.startsWith("BLOCK=")) {
                    int key = Integer.parseInt(line.substring(6));
                    LoadedBlock block = new LoadedBlock();
                    block.index = key;
                    loaded.add(block);
                } else if (line.startsWith("KEYS=")) {
                    if (!loaded.isEmpty()) {
                        loaded.get(loaded.size() - 1).keys = Integer.parseInt(line.substring(5));
                    }
                } else if (line.startsWith("BIT_SIZE=")) {
                    if (!loaded.isEmpty()) {
                        loaded.get(loaded.size() - 1).bitSize = Integer.parseInt(line.substring(9));
                    }
                } else if (line.startsWith("BITS=")) {
                    if (!loaded.isEmpty()) {
                        loaded.get(loaded.size() - 1).bits =
                                Base64.getDecoder().decode(line.substring(5));
                    }
                }
            }
            if (fileSize != dataFileSize || fileModified != dataFileModified) {
                LOGGER.debug("AvroBloomFilter sidecar stamp mismatch: {}",
                        fileSize != dataFileSize ? "size" : "modified");
                return null;
            }
            if (expectedBlocks >= 0 && loaded.size() != expectedBlocks) {
                LOGGER.warn("AvroBloomFilter sidecar block count mismatch: expected {}, got {}",
                        expectedBlocks, loaded.size());
                return null;
            }
            AvroBloomFilter filter = new AvroBloomFilter(
                    AvroBloomFilterConfig.resolveFor(loadedBitsPerKey, loadedNumHashes, loadedFpp, enabled));
            for (LoadedBlock block : loaded) {
                if (block.bits == null) continue;
                BlockFilter bf = BlockFilter.fromBytes(block.bits, block.bitSize, loadedNumHashes);
                bf.keyCount.set(block.keys);
                filter.filters.put(block.index, bf);
            }
            return filter;
        } catch (Exception e) {
            LOGGER.debug("AvroBloomFilter sidecar load failed: {}", e.getMessage());
            return null;
        }
    }

    // ─── Internal helpers ───────────────────────────────────────────

    private static final class LoadedBlock {
        int index;
        int keys;
        int bitSize;
        byte[] bits;
    }

    private static final class BlockFilter {
        private final BitSet bits;
        private final int numHashes;
        private final int bitSize;
        private final AtomicInteger keyCount = new AtomicInteger();

        private BlockFilter(int bitSize, int numHashes) {
            this.bitSize = Math.max(8, bitSize);
            this.numHashes = Math.max(1, numHashes);
            this.bits = new BitSet(this.bitSize);
        }

        static BlockFilter create(int expectedKeys, int bitsPerKey, int numHashes) {
            long size = Math.max(8L, (long) Math.max(1, expectedKeys) * Math.max(1, bitsPerKey));
            return new BlockFilter((int) Math.min(size, Integer.MAX_VALUE - 64), numHashes);
        }

        static BlockFilter fromBytes(byte[] bytes, int bitSize, int numHashes) {
            BlockFilter filter = new BlockFilter(Math.max(8, bitSize), numHashes);
            filter.bits.or(BitSet.valueOf(bytes != null ? bytes : new byte[0]));
            return filter;
        }

        void put(Object key) {
            long h1 = hash1(key);
            long h2 = hash2(key);
            for (int i = 0; i < numHashes; i++) {
                int idx = (int) (Math.abs(h1 + (long) i * h2) % bitSize);
                bits.set(idx);
            }
        }

        boolean mightContain(Object key) {
            long h1 = hash1(key);
            long h2 = hash2(key);
            for (int i = 0; i < numHashes; i++) {
                int idx = (int) (Math.abs(h1 + (long) i * h2) % bitSize);
                if (!bits.get(idx)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Estimates the false-positive probability from the current key
         * count: (1 - e^{-k*n/m})^k. Returns 0.0 for an empty filter.
         */
        double estimatedFpp() {
            double n = keyCount.get();
            if (n == 0) return 0.0;
            double m = bitSize;
            double k = numHashes;
            double p = Math.pow(1 - Math.exp(-k * n / m), k);
            return Math.max(0.0, Math.min(1.0, p));
        }

        private static long hash1(Object key) {
            int h = key.hashCode();
            return (h ^ (h >>> 16)) & 0xFFFFFFFFL;
        }

        private static long hash2(Object key) {
            int h = key.hashCode();
            return ((h * 0x85ebca6b) ^ (h >>> 16)) & 0xFFFFFFFFL;
        }
    }

    @Override
    public String toString() {
        return "AvroBloomFilter{enabled=" + enabled
                + ", bitsPerKey=" + bitsPerKey
                + ", numHashes=" + numHashes
                + ", fpp=" + fpp
                + ", blocks=" + filters.size() + '}';
    }
}