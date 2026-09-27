package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import diesel.ConfigKeys;

/**
 * Parallel reader for Avro object-container files (Prompt 69).
 *
 * <p>Reads the data blocks of an Avro file in parallel and returns the rows in
 * exact file order, regardless of worker count:
 * <ul>
 *   <li><b>Block index scan</b> - the file header is probed and every data block
 *       is walked in a cheap pass ({@code [count, size, payload, sync]}), recording
 *       each block's header offset, record count and payload size without decoding
 *       records.</li>
 *   <li><b>Load balancing</b> - blocks are cut into <em>contiguous</em> partitions
 *       by accumulated <em>payload size</em> (weighted, not by block count), so
 *       partitions are balanced even when block sizes differ sharply.</li>
 *   <li><b>Parallel reading</b> - each worker opens its own
 *       {@link AvroDataFileReader} (with the same projection, if any), seeks to
 *       the sync marker preceding its first block via
 *       {@link AvroDataFileReader#seekToSyncMarker(long)} and decodes exactly the
 *       blocks in its partition.</li>
 *   <li><b>Deterministic merge</b> - partition results are concatenated in
 *       partition order, which equals file order. Any worker failure is rethrown
 *       as an {@link IOException}.</li>
 *   <li><b>Sequential fallback</b> - below
 *       {@code avro.parallel.read.threshold} rows (system property, then
 *       {@code config.properties}, default 10000) or for a single-block file the
 *       reader falls back to a plain sequential read.</li>
 * </ul>
 *
 * @since Prompt 69
 */
public final class AvroParallelReader implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroParallelReader.class);

    /** Config key: minimum row count before parallel reading is attempted. */
    public static final String THRESHOLD_KEY = "avro.parallel.read.threshold";
    /** Default threshold when neither the system property nor config.properties sets it. */
    public static final int DEFAULT_THRESHOLD = 10000;

    private final File file;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final int threshold;
    private final int maxThreads;
    private final boolean parallelEnabled;
    private final List<BlockEntry> blocks;
    private final List<int[]> partitions;
    private final List<Long> partitionLoads;
    private volatile boolean closed;

    /**
     * Lightweight per-block information gathered by the index scan.
     *
     * @param headerPos   file offset of the block header (the count varint)
     * @param recordCount records declared by this block
     * @param payloadSize compressed payload size in bytes (used as partition weight)
     */
    public static final class BlockEntry {
        public final long headerPos;
        public final long recordCount;
        public final long payloadSize;

        public BlockEntry(long headerPos, long recordCount, long payloadSize) {
            this.headerPos = headerPos;
            this.recordCount = recordCount;
            this.payloadSize = payloadSize;
        }
    }

    /**
     * Opens the file and decides parallelism from the configured threshold and
     * the number of available processors.
     *
     * @param avroFile the Avro object-container file
     * @throws IOException if the file is missing, corrupt, or cannot be read
     */
    public AvroParallelReader(File avroFile) throws IOException {
        this(avroFile, 0);
    }

    /**
     * Opens the file with an explicit cap on worker threads.
     *
     * @param avroFile   the Avro object-container file
     * @param maxThreads maximum worker threads; {@code 0} means auto (available
     *                   processors, capped by the number of blocks)
     */
    public AvroParallelReader(File avroFile, int maxThreads) throws IOException {
        this.file = avroFile;
        if (avroFile == null || !avroFile.isFile()) {
            throw new IOException(AvroFileConstants.MSG_FILE_NOT_FOUND + avroFile);
        }
        this.maxThreads = Math.max(0, maxThreads);
        this.threshold = resolveThreshold();

        try (AvroDataFileReader probe = new AvroDataFileReader(avroFile)) {
            this.columns = buildColumns(probe.getSchema());
            this.columnTypes = buildColumnTypes(probe.getSchema());
            this.blocks = scanBlocks(avroFile, probe.getSyncMarker(), probe.getPosition());
        }

        long totalPayload = 0;
        long totalRows = 0;
        for (BlockEntry b : blocks) {
            totalPayload += b.payloadSize;
            totalRows += b.recordCount;
        }
        boolean multiBlock = blocks.size() > 1;
        boolean enoughRows = totalRows >= threshold;
        boolean enoughThreads = maxThreads == 0
                ? Runtime.getRuntime().availableProcessors() > 1
                : maxThreads > 1;
        boolean sequential = !multiBlock || !enoughRows || !enoughThreads;

        if (sequential) {
            this.parallelEnabled = false;
            this.partitions = blocks.isEmpty() ? List.of() : List.of(new int[]{0, blocks.size()});
            this.partitionLoads = blocks.isEmpty() ? List.of() : List.of(totalPayload);
        } else {
            int workers = maxThreads == 0
                    ? Math.min(Runtime.getRuntime().availableProcessors(), blocks.size())
                    : Math.min(maxThreads, blocks.size());
            this.parallelEnabled = true;
            this.partitions = buildPartitions(blocks, workers);
            this.partitionLoads = new ArrayList<>(partitions.size());
            for (int[] p : partitions) {
                long w = 0;
                for (int i = p[0]; i < p[1]; i++) {
                    w += blocks.get(i).payloadSize;
                }
                partitionLoads.add(w);
            }
        }
        LOGGER.debug("AvroParallelReader opened {} ({} blocks, {} rows, parallel={}, {} partitions)",
                avroFile, blocks.size(), totalRows, parallelEnabled, partitions.size());
    }

    // ─── Index scan (byte-level walk, no record decoding) ─────────────

    private static List<BlockEntry> scanBlocks(File avroFile, byte[] sync, long headerEnd) throws IOException {
        List<BlockEntry> result = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "r");
             FileChannel ch = raf.getChannel()) {
            long fileLen = raf.length();
            long pos = headerEnd;
            while (pos < fileLen) {
                if (fileLen - pos == AvroDataFileReader.SYNC_SIZE) {
                    break; // trailing sync-only region (final FLUSH marker)
                }
                long blockHeaderStart = pos;
                long[] cv = readZigzagVlq(ch, pos);
                long count = cv[0];
                pos = cv[1];
                long[] sv = readZigzagVlq(ch, pos);
                long size = sv[0];
                pos = sv[1];
                if (size < 0 || size > Integer.MAX_VALUE) {
                    throw new IOException("Invalid Avro block size " + size + " in " + avroFile);
                }
                pos += size;
                long syncPos = pos;
                byte[] actual = new byte[AvroDataFileReader.SYNC_SIZE];
                readFully(ch, syncPos, actual, fileLen, avroFile);
                if (!Arrays.equals(actual, sync)) {
                    throw new IOException(AvroFileConstants.MSG_SYNC_MARKER_MISMATCH + syncPos
                            + ": corrupt file or interrupted write");
                }
                result.add(new BlockEntry(blockHeaderStart, count, size));
                pos = syncPos + AvroDataFileReader.SYNC_SIZE;
            }
        }
        return result;
    }

    // ─── Load-balanced partition building ─────────────────────────────

    /**
     * Cuts the block range into contiguous partitions by accumulated payload
     * weight, aiming for {@code totalWeight / workers} per partition. A block
     * heavier than the target claims its own partition; partitions never overlap
     * and are non-empty.
     *
     * @param blocks  the block index (must be in file order)
     * @param workers requested partition count
     * @return partition ranges {@code [from, to)} in file order
     */
    public static List<int[]> buildPartitions(List<BlockEntry> blocks, int workers) {
        if (blocks.isEmpty()) {
            return List.of();
        }
        int effective = Math.min(Math.max(1, workers), blocks.size());
        long total = 0;
        for (BlockEntry b : blocks) {
            total += b.payloadSize;
        }
        long target = Math.max(1, total / effective);
        List<int[]> parts = new ArrayList<>(effective);
        long acc = 0;
        int start = 0;
        for (int i = 0; i < blocks.size(); i++) {
            acc += blocks.get(i).payloadSize;
            int remainingSlots = (effective - 1) - parts.size();
            int blocksLeftIncludingThis = blocks.size() - i;
            if (acc >= target && remainingSlots > 0 && blocksLeftIncludingThis > remainingSlots) {
                parts.add(new int[]{start, i + 1});
                start = i + 1;
                acc = 0;
            }
        }
        parts.add(new int[]{start, blocks.size()});
        return parts;
    }

    // ─── Reading ───────────────────────────────────────────────────────

    /** Reads every row of the file as {@code Object[]}, one per record. */
    public List<Object[]> readAll() throws IOException {
        return readProjected(null);
    }

    /**
     * Reads the file and returns rows containing exactly the requested columns
     * (in the requested order); non-requested values are {@code null}.
     *
     * @param projection requested field names; {@code null} or empty means a full read
     */
    public List<Object[]> readProjected(Collection<String> projection) throws IOException {
        if (closed) {
            throw new IOException("AvroParallelReader is closed: " + file);
        }
        List<String> cols = (projection == null || projection.isEmpty())
                ? columns
                : new ArrayList<>(projection);
        Class<?>[] targetTypes = AvroRowStorage.resolveColumnTypes(cols, columnTypes);
        if (blocks.isEmpty()) {
            return new ArrayList<>();
        }
        if (!parallelEnabled) {
            return readSequential(projection, cols, targetTypes);
        }
        return readParallel(projection, cols, targetTypes);
    }

    private List<Object[]> readSequential(Collection<String> projection, List<String> cols,
                                           Class<?>[] targetTypes) throws IOException {
        List<Object[]> rows = new ArrayList<>((int) Math.min(estimatedRows(), Integer.MAX_VALUE / 2));
        try (AvroDataFileReader reader = newReader(projection)) {
            for (BlockEntry b : blocks) {
                for (long i = 0; i < b.recordCount; i++) {
                    rows.add(AvroRowStorage.fromRecord(reader.nextRecord(), cols, targetTypes));
                }
            }
        }
        return rows;
    }

    private List<Object[]> readParallel(Collection<String> projection, List<String> cols,
                                        Class<?>[] targetTypes) throws IOException {
        try (ExecutorService pool = Executors.newFixedThreadPool(partitions.size(), r -> {
            Thread t = new Thread(r, "avro-parallel");
            t.setDaemon(true);
            return t;
        })) {
            try {
                List<Future<List<Object[]>>> futures = new ArrayList<>(partitions.size());
                for (int[] p : partitions) {
                    futures.add(pool.submit(readPartition(projection, cols, targetTypes, p)));
                }
                List<Object[]> result = new ArrayList<>((int) Math.min(estimatedRows(), Integer.MAX_VALUE / 2));
                for (Future<List<Object[]>> f : futures) {
                    result.addAll(f.get());
                }
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Avro parallel read interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                throw new IOException("Avro parallel read failed", cause);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    private Callable<List<Object[]>> readPartition(Collection<String> projection, List<String> cols,
                                                     Class<?>[] targetTypes, int[] range) {
        return () -> {
            try (AvroDataFileReader reader = newReader(projection)) {
                long estimate = 0;
                for (int i = range[0]; i < range[1]; i++) {
                    estimate += blocks.get(i).recordCount;
                }
                List<Object[]> rows = new ArrayList<>((int) Math.min(estimate, Integer.MAX_VALUE / 2));
                for (int i = range[0]; i < range[1]; i++) {
                    BlockEntry b = blocks.get(i);
                    reader.seekToSyncMarker(b.headerPos - AvroDataFileReader.SYNC_SIZE);
                    for (long j = 0; j < b.recordCount; j++) {
                        rows.add(AvroRowStorage.fromRecord(reader.nextRecord(), cols, targetTypes));
                    }
                }
                return rows;
            }
        };
    }

    private AvroDataFileReader newReader(Collection<String> projection) throws IOException {
        return (projection == null || projection.isEmpty())
                ? new AvroDataFileReader(file)
                : new AvroDataFileReader(file, projection);
    }

    private long estimatedRows() {
        long n = 0;
        for (BlockEntry b : blocks) {
            n += b.recordCount;
        }
        return n;
    }

    // ─── Header-derived metadata ───────────────────────────────────────

    private static List<String> buildColumns(org.apache.avro.Schema schema) {
        List<String> cols = new ArrayList<>();
        if (schema.getType() == org.apache.avro.Schema.Type.RECORD) {
            for (org.apache.avro.Schema.Field f : schema.getFields()) {
                cols.add(f.name());
            }
        }
        return cols;
    }

    private static Map<String, Class<?>> buildColumnTypes(org.apache.avro.Schema schema) {
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (schema.getType() == org.apache.avro.Schema.Type.RECORD) {
            for (org.apache.avro.Schema.Field f : schema.getFields()) {
                types.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
            }
        }
        return types;
    }

    // ─── Diagnostics ───────────────────────────────────────────────────

    /** Returns how many data blocks the file contains. */
    public long blockCount() {
        return blocks.size();
    }

    /** Returns how many records the file contains (exact, from the index scan). */
    public long rowCount() {
        return estimatedRows();
    }

    /** Returns whether the read uses parallel worker threads. */
    public boolean isParallel() {
        return parallelEnabled;
    }

    /** Returns the cap on worker threads ({@code 0} = auto). */
    public int getMaxThreads() {
        return maxThreads;
    }

    /** Returns the number of partitions the file is split into. */
    public int getPartitionCount() {
        return partitions.size();
    }

    /**
     * Returns the payload weight (bytes) of each partition, in partition order.
     * With a single partition (sequential or single-block file) this is the total
     * payload size.
     */
    public List<Long> getPartitionLoads() {
        return new ArrayList<>(partitionLoads);
    }

    /** Returns the resolved parallel-read threshold (rows). */
    public int getThreshold() {
        return threshold;
    }

    /** Returns the total compressed payload size in bytes, summed over all blocks. */
    public long getTotalPayloadBytes() {
        long total = 0;
        for (BlockEntry b : blocks) {
            total += b.payloadSize;
        }
        return total;
    }

    // ─── Threshold resolution ──────────────────────────────────────────

    private static int resolveThreshold() {
        String sys = System.getProperty(THRESHOLD_KEY);
        if (sys != null) {
            try {
                int v = Integer.parseInt(sys.trim());
                if (v >= 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        Properties props = new Properties();
        File configFile = new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
            }
        }
        String raw = props.getProperty(THRESHOLD_KEY);
        if (raw != null) {
            try {
                int v = Integer.parseInt(raw.trim());
                if (v >= 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return DEFAULT_THRESHOLD;
    }

    // ─── Raw I/O helpers ───────────────────────────────────────────────

    private static long[] readZigzagVlq(FileChannel ch, long pos) throws IOException {
        long value = 0;
        int shift = 0;
        long p = pos;
        while (true) {
            ByteBuffer one = ByteBuffer.allocate(1);
            int got = ch.read(one, p);
            if (got < 0) {
                throw new IOException("Truncated Avro data file: expected varint at offset " + p);
            }
            byte b = one.array()[0];
            p++;
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
            if (shift > 63) {
                throw new IOException("Malformed varint in Avro block header");
            }
        }
        return new long[]{(value >>> 1) ^ -(value & 1L), p};
    }

    private static void readFully(FileChannel ch, long position, byte[] out, long fileLen, File avroFile)
            throws IOException {
        if (position < 0 || position + out.length > fileLen) {
            throw new IOException(AvroFileConstants.MSG_TRUNCATED_FILE + avroFile + AvroFileConstants.MSG_EXPECTED + out.length
                    + AvroFileConstants.MSG_BYTES_AT_OFFSET + position + AvroFileConstants.MSG_FILE_ENDS_AT + fileLen);
        }
        int off = 0;
        while (off < out.length) {
            int got = ch.read(ByteBuffer.wrap(out, off, out.length - off), position + off);
            if (got < 0) {
                throw new IOException(AvroFileConstants.MSG_TRUNCATED_FILE + avroFile + " at offset " + position);
            }
            off += got;
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}