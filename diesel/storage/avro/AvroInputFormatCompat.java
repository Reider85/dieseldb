package diesel.storage.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import diesel.ConfigKeys;

/**
 * Avro map-reduce compatibility layer: splits an Avro object-container file
 * into independent, self-contained byte ranges aligned on <em>sync markers</em>
 * (Prompt 70).
 *
 * <p>An Avro data file is a sequence of {@code [count, size, payload, sync]}
 * blocks. Every block is delimited by the 16-byte sync marker written to the
 * file header, so any block can be decoded without reading its neighbours. This
 * class turns that property into <em>splits</em> — contiguous, non-overlapping
 * byte ranges that together cover the whole file. Each split starts exactly at
 * a block boundary (the block header that follows a sync marker), which is the
 * precondition Hadoop/MapReduce and Spark-oriented readers need in order to
 * schedule one mapper per split and have each mapper read its range
 * independently.
 *
 * <p>Splits are computed by a cheap byte-level pass that never decodes records
 * (mirroring {@link AvroParallelReader}) and are described by
 * {@link SplitDescriptor}; {@link #readSplit} re-opens the file per split,
 * seeks to the boundary sync marker via
 * {@link AvroDataFileReader#seekToSyncMarker(long)} and decodes exactly the
 * split's records. This makes the split set a drop-in feed for a map phase:
 *
 * <pre>{@code
 * // MapReduce-style mapper: one invocation per split, run in parallel.
 * for (AvroInputFormatCompat.SplitDescriptor split :
 *         AvroInputFormatCompat.computeSplits(avroFile)) {
 *     List<Object[]> rows = AvroInputFormatCompat.readSplit(avroFile, split);
 *     for (Object[] row : rows) {
 *         emit(context, row);            // per-mapper row processing
 *     }
 * }
 *
 * // Spark DataFrame-style projection pushdown: read only two columns per split.
 * List<Object[]> projected = AvroInputFormatCompat.readSplit(
 *         avroFile, split, List.of("NAME", "AGE"));
 * }</pre>
 *
 * <p>Configuration is resolved from a system-property override first, then the
 * root {@code config.properties}, then the code-level defaults:
 * <ul>
 *   <li>{@code avro.split.size} — target split size in bytes (default 67108864
 *       = 64 MB). Blocks accumulate into a split until this is reached.</li>
 *   <li>{@code avro.split.min.size} — a trailing split smaller than this is
 *       merged into the previous one (default 1048576 = 1 MiB).</li>
 * </ul>
 *
 * <p>The class is stateless and thread-safe: all methods are static and every
 * split read opens its own reader, so mappers may process different splits in
 * parallel without shared mutable state.
 *
 * @since Prompt 70
 */
public final class AvroInputFormatCompat {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroInputFormatCompat.class);

    /** Config key: target split size in bytes. */
    public static final String SPLIT_SIZE_KEY = "avro.split.size";
    /** Config key: minimum size of a trailing split before it is merged. */
    public static final String SPLIT_MIN_SIZE_KEY = "avro.split.min.size";

    /** Code-level default target split size: 64 MB. */
    public static final long DEFAULT_SPLIT_SIZE = 67108864L;
    /** Code-level default minimum split size: 1 MiB. */
    public static final long DEFAULT_MIN_SPLIT_SIZE = 1048576L;

    private AvroInputFormatCompat() {
        throw new AssertionError("No instances");
    }

    /**
     * A single map-reduce-readable byte range of an Avro file.
     *
     * <p>{@code start} is the file offset of the first block's header (the
     * {@code count} varint). The split covers the blocks
     * {@code [firstBlock, endBlock)} and therefore starts exactly on a sync
     * marker boundary; {@link #readSplit} seeks back by
     * {@link AvroDataFileReader#SYNC_SIZE} to land on that marker and then
     * decodes {@code recordCount} records sequentially.
     *
     * @param start       byte offset of the first block header in the split
     * @param length      byte span from {@code start} to the end of the last
     *                    block's trailing sync marker
     * @param firstBlock  zero-based index of the first block (file order)
     * @param endBlock    exclusive index of the last block (file order)
     * @param blockCount  number of blocks the split covers
     * @param recordCount total records declared by the split's blocks (exact)
     * @param payloadSize compressed payload bytes summed over the split's blocks
     */
    public record SplitDescriptor(long start, long length, int firstBlock, int endBlock,
                                  int blockCount, long recordCount, long payloadSize) {

        /** End offset (exclusive) of the split in the file: {@code start + length}. */
        public long end() {
            return start + length;
        }

        /** Whether the split covers no blocks at all. */
        public boolean isEmpty() {
            return blockCount == 0;
        }
    }

    /**
     * Byte layout of a single data block discovered by the byte-level scan.
     *
     * @param headerPos   file offset of the block header ({@code count} varint)
     * @param blockEnd    file offset just past the block's trailing sync marker
     * @param recordCount records declared by the block
     * @param payloadSize compressed payload size in bytes (used as the weight
     *                    when grouping blocks into splits)
     */
    public record BlockRange(long headerPos, long blockEnd, long recordCount, long payloadSize) {
    }

    /**
     * Structural summary of an Avro object-container file.
     *
     * @param schema       writer schema parsed from the header metadata
     * @param codecName    codec name declared in the header ({@code "null"},
     *                     {@code "deflate"}, {@code "snappy"}, ...)
     * @param syncMarker   the 16-byte sync marker repeated after every block
     * @param headerSize   number of bytes the header (magic + meta + sync) occupies
     * @param blocks       every data block, in file order (never decoded)
     * @param fileSize     total file length in bytes
     */
    public record FileLayout(Schema schema, String codecName, byte[] syncMarker, long headerSize,
                             List<BlockRange> blocks, long fileSize) {

        /** Total records across all blocks (exact, from the block scan). */
        public long totalRecords() {
            long n = 0;
            for (BlockRange b : blocks) {
                n += b.recordCount();
            }
            return n;
        }

        /** Total compressed payload bytes across all blocks. */
        public long totalPayloadBytes() {
            long n = 0;
            for (BlockRange b : blocks) {
                n += b.payloadSize();
            }
            return n;
        }
    }

    // ─── Split computation ────────────────────────────────────────────

    /**
     * Computes map-reduce splits for the given Avro file using the configured
     * {@code avro.split.size} / {@code avro.split.min.size} (system property,
     * then {@code config.properties}, then the defaults).
     *
     * @param avroFile the Avro object-container file
     * @return non-overlapping splits in file order covering the whole data
     *         section; empty if the file has no data blocks
     * @throws IOException if the file is missing, not Avro data, or corrupt
     */
    public static List<SplitDescriptor> computeSplits(File avroFile) throws IOException {
        return computeSplits(avroFile, resolveSplitSize(), resolveMinSplitSize());
    }

    /**
     * Computes map-reduce splits with explicit size limits.
     *
     * <p>Contiguous blocks are accumulated by payload weight until
     * {@code targetSize} is reached; a single block larger than the target
     * claims its own split. A trailing split whose byte length is below
     * {@code minSize} is merged into the previous split to avoid tiny map tasks.
     *
     * @param avroFile   the Avro object-container file
     * @param targetSize target split size in bytes ({@code <= 0} uses the default)
     * @param minSize    minimum trailing split length in bytes ({@code <= 0} uses
     *                   the default); ignored for the first split
     * @return non-overlapping splits in file order
     * @throws IOException if the file cannot be read or is corrupt
     */
    public static List<SplitDescriptor> computeSplits(File avroFile, long targetSize, long minSize)
            throws IOException {
        FileLayout layout = analyzeFile(avroFile);
        List<BlockRange> blocks = layout.blocks();
        if (blocks.isEmpty()) {
            return List.of();
        }

        long target = Math.max(1, targetSize > 0 ? targetSize : DEFAULT_SPLIT_SIZE);
        long min = Math.max(1, minSize > 0 ? minSize : DEFAULT_MIN_SPLIT_SIZE);

        List<SplitDescriptor> splits = new ArrayList<>();
        long acc = 0;
        int start = 0;
        for (int i = 0; i < blocks.size(); i++) {
            acc += blocks.get(i).payloadSize();
            if (acc >= target && i + 1 < blocks.size()) {
                splits.add(buildSplit(blocks, start, i + 1));
                start = i + 1;
                acc = 0;
            }
        }
        splits.add(buildSplit(blocks, start, blocks.size()));

        while (splits.size() > 1 && splits.get(splits.size() - 1).length() < min) {
            SplitDescriptor last = splits.remove(splits.size() - 1);
            SplitDescriptor prev = splits.get(splits.size() - 1);
            splits.set(splits.size() - 1, mergeSplits(prev, last));
        }

        LOGGER.debug("Computed {} split(s) for {} ({} blocks, {} rows, target={} bytes)",
                splits.size(), avroFile, blocks.size(), layout.totalRecords(), target);
        return splits;
    }

    private static SplitDescriptor buildSplit(List<BlockRange> blocks, int from, int to) {
        BlockRange first = blocks.get(from);
        BlockRange last = blocks.get(to - 1);
        long records = 0;
        long payload = 0;
        for (int i = from; i < to; i++) {
            records += blocks.get(i).recordCount();
            payload += blocks.get(i).payloadSize();
        }
        return new SplitDescriptor(first.headerPos(), last.blockEnd() - first.headerPos(),
                from, to, to - from, records, payload);
    }

    private static SplitDescriptor mergeSplits(SplitDescriptor a, SplitDescriptor b) {
        return new SplitDescriptor(a.start(), b.end() - a.start(), a.firstBlock(), b.endBlock(),
                a.blockCount() + b.blockCount(), a.recordCount() + b.recordCount(),
                a.payloadSize() + b.payloadSize());
    }

    // ─── Reading a split ──────────────────────────────────────────────

    /**
     * Reads every record of the given split as {@code Object[]}, one per record,
     * in file order. Each column of the row corresponds to a field of the writer
     * schema in header order.
     *
     * @param avroFile the Avro object-container file
     * @param split    a split previously returned by {@link #computeSplits}
     * @return the split's rows
     * @throws IOException if the split does not align with a sync boundary or
     *                     the file is corrupt
     */
    public static List<Object[]> readSplit(File avroFile, SplitDescriptor split) throws IOException {
        return readSplit(avroFile, split, null);
    }

    /**
     * Reads a split and returns rows containing exactly the requested columns
     * (in the requested order); non-requested values are {@code null} — the
     * equivalent of a Spark {@code DataFrame} projection. A {@code null} or empty
     * projection performs a full read.
     *
     * @param avroFile   the Avro object-container file
     * @param split      a split previously returned by {@link #computeSplits}
     * @param projection requested field names; {@code null} or empty means a full read
     * @return the split's rows restricted to the projection
     * @throws IOException if the split does not align with a sync boundary or
     *                     the file is corrupt
     */
    public static List<Object[]> readSplit(File avroFile, SplitDescriptor split,
                                           Collection<String> projection) throws IOException {
        List<Object[]> rows;
        try (AvroDataFileReader reader = (projection == null || projection.isEmpty())
                ? new AvroDataFileReader(avroFile)
                : new AvroDataFileReader(avroFile, projection)) {
            if (!reader.seekToSyncMarker(split.start() - AvroDataFileReader.SYNC_SIZE)) {
                throw new IOException("No sync marker before split start " + split.start()
                        + " in " + avroFile + ": split is not aligned on a block boundary");
            }
            Schema schema = reader.getSchema();
            List<String> cols = (projection == null || projection.isEmpty())
                    ? buildColumns(schema)
                    : new ArrayList<>(projection);
            Map<String, Class<?>> types = buildColumnTypes(schema);
            Class<?>[] targetTypes = AvroRowStorage.resolveColumnTypes(cols, types);
            rows = new ArrayList<>((int) Math.min(split.recordCount(), Integer.MAX_VALUE / 2));
            for (long i = 0; i < split.recordCount(); i++) {
                rows.add(AvroRowStorage.fromRecord(reader.nextRecord(), cols, targetTypes));
            }
        }
        return rows;
    }

    /**
     * Reads the whole file as a single split (splits the file into one range and
     * reads it). Convenience for callers that want the map-reduce row contract
     * without computing a split set.
     *
     * @param avroFile the Avro object-container file
     * @return every row of the file
     * @throws IOException if the file cannot be read
     */
    public static List<Object[]> readAll(File avroFile) throws IOException {
        FileLayout layout = analyzeFile(avroFile);
        if (layout.blocks().isEmpty()) {
            return List.of();
        }
        SplitDescriptor whole = buildSplit(layout.blocks(), 0, layout.blocks().size());
        return readSplit(avroFile, whole, null);
    }

    // ─── Structural analysis ──────────────────────────────────────────

    /**
     * Probes the header and scans every data block of the file without decoding
     * records. The result gives external engines (Hadoop {@code InputFormat},
     * Spark {@code FileFormat}) everything needed to plan splits and read them:
     * writer schema, codec, sync marker, per-block byte layout, and totals.
     *
     * @param avroFile the Avro object-container file
     * @return the file's structural summary
     * @throws IOException if the file is missing, not Avro data, or corrupt
     */
    public static FileLayout analyzeFile(File avroFile) throws IOException {
        if (avroFile == null || !avroFile.isFile()) {
            throw new IOException(AvroFileConstants.MSG_FILE_NOT_FOUND + avroFile);
        }
        try (AvroDataFileReader probe = new AvroDataFileReader(avroFile)) {
            long headerEnd = probe.getPosition();
            List<BlockRange> blocks = scanBlocks(avroFile, probe.getSyncMarker(), headerEnd);
            return new FileLayout(probe.getSchema(), probe.getCodecName(), probe.getSyncMarker(),
                    headerEnd, blocks, avroFile.length());
        }
    }

    /**
     * Validates that the given splits tile the file's data section exactly:
     * the first split starts at the first block, every split starts at a known
     * block boundary, consecutive splits are contiguous, no split overlaps
     * another, and the summed record counts equal the file's total.
     *
     * @param avroFile the Avro object-container file
     * @param splits   the split set to validate
     * @return {@code true} if the splits cover the file without gaps or overlaps
     * @throws IOException if the file cannot be read
     */
    public static boolean validateSplits(File avroFile, List<SplitDescriptor> splits) throws IOException {
        FileLayout layout = analyzeFile(avroFile);
        List<BlockRange> blocks = layout.blocks();
        if (blocks.isEmpty()) {
            return splits.isEmpty();
        }
        if (splits.isEmpty()) {
            return false;
        }

        long totalRecords = 0;
        int prevEndBlock = -1;
        for (SplitDescriptor s : splits) {
            if (s.firstBlock() > s.endBlock() || s.blockCount() != s.endBlock() - s.firstBlock()) {
                return false;
            }
            if (prevEndBlock >= 0 && prevEndBlock != s.firstBlock()) {
                return false; // gap or overlap between splits
            }
            prevEndBlock = s.endBlock();
            totalRecords += s.recordCount();
        }
        int firstBlockOf = splits.get(0).firstBlock();
        boolean startsAtZero = firstBlockOf == 0 && matchesHeaderPos(blocks, splits.get(0));
        boolean endsAtLast = splits.get(splits.size() - 1).endBlock() == blocks.size();
        boolean recordsMatch = totalRecords == layout.totalRecords();

        for (SplitDescriptor s : splits) {
            if (!matchesHeaderPos(blocks, s)) {
                return false;
            }
        }
        return startsAtZero && endsAtLast && recordsMatch;
    }

    private static boolean matchesHeaderPos(List<BlockRange> blocks, SplitDescriptor s) {
        if (s.firstBlock() < 0 || s.firstBlock() >= blocks.size()) {
            return false;
        }
        return blocks.get(s.firstBlock()).headerPos() == s.start();
    }

    // ─── Byte-level block scan ────────────────────────────────────────

    /**
     * Walks the raw file bytes after the header, reading each block's
     * {@code [count, size, payload, sync]} layout and verifying the trailing sync
     * marker. Never decodes records.
     */
    private static List<BlockRange> scanBlocks(File avroFile, byte[] sync, long headerEnd) throws IOException {
        List<BlockRange> result = new ArrayList<>();
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
                if (count < 0) {
                    throw new IOException("Negative record count (" + count + ") at offset "
                            + blockHeaderStart + " in " + avroFile);
                }
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
                long blockEnd = syncPos + AvroDataFileReader.SYNC_SIZE;
                result.add(new BlockRange(blockHeaderStart, blockEnd, count, size));
                pos = blockEnd;
            }
        }
        return result;
    }

    // ─── Column/type derivation ───────────────────────────────────────

    private static List<String> buildColumns(Schema schema) {
        List<String> cols = new ArrayList<>();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field f : schema.getFields()) {
                cols.add(f.name());
            }
        }
        return cols;
    }

    private static Map<String, Class<?>> buildColumnTypes(Schema schema) {
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field f : schema.getFields()) {
                types.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
            }
        }
        return types;
    }

    // ─── Configuration resolution ─────────────────────────────────────

    /**
     * Resolves {@code avro.split.size} from a system property, then the root
     * {@code config.properties}, then the default.
     */
    public static long resolveSplitSize() {
        return resolveLong(SPLIT_SIZE_KEY, DEFAULT_SPLIT_SIZE);
    }

    /**
     * Resolves {@code avro.split.min.size} from a system property, then the root
     * {@code config.properties}, then the default.
     */
    public static long resolveMinSplitSize() {
        return resolveLong(SPLIT_MIN_SIZE_KEY, DEFAULT_MIN_SPLIT_SIZE);
    }

    private static long resolveLong(String key, long fallback) {
        String sys = System.getProperty(key);
        if (sys != null) {
            try {
                long v = Long.parseLong(sys.trim());
                if (v > 0) {
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
        String raw = props.getProperty(key);
        if (raw != null) {
            try {
                long v = Long.parseLong(raw.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return fallback;
    }

    // ─── Raw I/O helpers ──────────────────────────────────────────────

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
}