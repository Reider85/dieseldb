package diesel.storage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.storage.json.JsonParserConfig;

/**
 * Byte-offset parallel reader for JSON Lines files (prompt 54). JSONL is
 * structurally simpler than CSV/TSV - a data row is exactly one physical line,
 * every nested structure is locked inside that line - so the pre-scan needs no
 * quoting analysis (unlike {@link DelimitedIndexManager}).
 *
 * <p><b>Pre-scan.</b> One byte-level pass records the file offset and the
 * 1-based physical line number of every non-blank line (plus the presence of a
 * UTF-8 BOM). A line is blank when all of its bytes are {@code <= 0x20}, which
 * is exactly consistent with the reader's {@code line.trim().isEmpty()} skip:
 * any valid or malformed JSON data line necessarily contains bytes above
 * {@code 0x20}, and Java {@code String.trim()} only removes characters
 * {@code <= 0x20}. Broken (non-object / malformed JSON) lines are <em>not</em>
 * validated on the byte pass - their file:line diagnostics stay with
 * {@link JsonlRowReader} (prompt 48), the same coordinates the pre-scan
 * preserves.
 *
 * <p><b>Partitions.</b> Data lines are grouped into consecutive ranges; each
 * task opens a {@link FileChannel}, positions at its first line's byte offset
 * and reads exactly up to the next partition's first data-line offset (or EOF),
 * so no line is re-read and total I/O stays close to the file size. The first
 * partition always starts at byte {@code 0}, so a BOM and leading blank lines
 * are consumed (and numbered) exactly like the sequential reader. Because
 * partition boundaries fall on data-line starts, lines never cross a boundary.
 * Each partition reader is seeded with its absolute first physical line number,
 * so diagnostics keep the same file:line coordinates as a sequential pass.
 *
 * <p><b>Determinism.</b> Results are merged in task order (partition order ==
 * file order), so the row order does not depend on the number of threads or
 * on scheduling. Repeated loads of an unchanged file reuse the cached pre-scan
 * (mtime/size check, same contract as prompt 34).
 *
 * <p><b>Compressed files (prompt 52).</b> Compressed JSONL ({@code .jsonl.zst},
 * {@code .jsonl.lz4}, {@code .jsonl.snappy}) is <em>not</em> byte-addressable:
 * the codec writes one whole-stream frame and the shared
 * {@link CompressionFactory} exposes no per-frame/block API, so the parallel
 * pre-scan cannot split it. Callers therefore read compressed files
 * sequentially (identical to the delimited backends, prompt 39); this loader
 * is only invoked for plain files and the fall-back is documented here.
 */
public final class JsonlParallelLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlParallelLoader.class);

    /**
     * Shared daemon pool used for parallel JSONL partition reads. Daemon
     * threads so the pool does not block JVM exit.
     */
    private static final ForkJoinPool READ_POOL = new ForkJoinPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("diesel-jsonl-read-" + t.getPoolIndex());
                return t;
            },
            null, true);

    /** Rows per partition upper bound used when splitting the file. */
    private static final int PARTITION_BLOCK_ROWS = 1000;

    private static final int BOM_B0 = 0xEF;
    private static final int BOM_B1 = 0xBB;
    private static final int BOM_B2 = 0xBF;

    private JsonlParallelLoader() {
    }

    /**
     * The result of a parallel load pass: rows and their per-row present-column
     * presence flags (prompt 47), both in file order, plus the union of the
     * nested-JSON holder columns discovered by the partition readers (prompt
     * 45). The row count is the number of rows successfully parsed (mirrors the
     * sequential load's {@code lineCount}). The caller must replay
     * {@code nestedColumnIndexes} onto its shared
     * {@link JsonlSchemaManager} so a save after a parallel load re-embeds
     * nested structures instead of double-encoding them as strings.
     */
    public record JsonlLoadResult(List<Object[]> rows, List<boolean[]> presence,
                                  Set<Integer> nestedColumnIndexes) {
    }

    /**
     * The result of the byte pre-scan: the file offset and absolute physical
     * line number of every non-blank line, plus diagnostic statistics.
     */
    public static final class LineIndex {
        /** Byte offset of every non-blank line start, ascending. */
        final long[] dataOffsets;
        /** 1-based physical line number of every non-blank line start. */
        final long[] dataLineNumbers;
        /** Number of blank (whitespace-only / empty) physical lines skipped. */
        final int blankLineCount;
        /** Whether the file starts with a UTF-8 BOM (EF BB BF). */
        final boolean bom;
        /** Whole-file byte buffer from the scan, or {@code null} when not retained. */
        private final byte[] fileBytes;

        LineIndex(long[] dataOffsets, long[] dataLineNumbers, int blankLineCount, boolean bom, byte[] fileBytes) {
            this.dataOffsets = dataOffsets;
            this.dataLineNumbers = dataLineNumbers;
            this.blankLineCount = blankLineCount;
            this.bom = bom;
            this.fileBytes = fileBytes;
        }

        /** Returns the number of non-blank (data) lines. */
        public int dataLineCount() {
            return dataOffsets.length;
        }

        /** The whole-file byte buffer produced by this scan, or {@code null} when not retained. */
        public byte[] fileBytes() {
            return fileBytes;
        }
    }

    /**
     * Cached pre-scan keyed by the file path, mtime and size. Held by the
     * owning {@link JsonlIndexManager} exactly like
     * {@code DelimitedIndexManager.LineIndexCache} (volatile copy-on-write, so
     * concurrent loads observe one consistent snapshot).
     */
    public static final class LineIndexCache {
        final String path;
        final long lastModified;
        final long length;
        final LineIndex index;

        LineIndexCache(String path, long lastModified, long length, LineIndex index) {
            this.path = path;
            this.lastModified = lastModified;
            this.length = length;
            this.index = index;
        }

        boolean matches(File file) {
            return path.equals(file.getAbsolutePath())
                    && lastModified == file.lastModified()
                    && length == file.length();
        }
    }

    /**
     * Single byte-level pass over the file (prompt 54 item 1). Splits physical
     * lines exactly like {@link BufferedReader#readLine()} ({@code \n},
     * {@code \r\n} and lone {@code \r} terminators), records the offset and
     * 1-based physical line number of every non-blank line, and detects the
     * UTF-8 BOM plus the number of blank lines. The cached result is reused
     * until the file's mtime or size changes.
     */
    public static LineIndex preScan(File file, Charset charset) throws IOException {
        return scan(file, charset, false);
    }

    /**
     * Like {@link #preScan(File, Charset)}, but the scanned whole-file byte
     * buffer is retained on the returned {@link LineIndex} so block readers can
     * slice column projections without re-reading the file (prompt 55).
     */
    public static LineIndex preScanKeepBytes(File file, Charset charset) throws IOException {
        return scan(file, charset, true);
    }

    private static LineIndex scan(File file, Charset charset, boolean keepBytes) throws IOException {
        byte[] bytes;
        try (InputStream in = new java.io.BufferedInputStream(new FileInputStream(file))) {
            bytes = in.readAllBytes();
        }
        boolean bom = bytes.length >= 3
                && (bytes[0] & 0xFF) == BOM_B0
                && (bytes[1] & 0xFF) == BOM_B1
                && (bytes[2] & 0xFF) == BOM_B2;
        long[] offsets = new long[1024];
        long[] lineNumbers = new long[1024];
        int count = 0;
        int blankCount = 0;
        long physicalLine = 1;
        int lineStart = 0;
        int i = 0;
        while (i < bytes.length) {
            byte b = bytes[i];
            if (b == (byte) '\r' || b == (byte) '\n') {
                boolean blank = isBlank(bytes, lineStart, i);
                if (blank) {
                    blankCount++;
                } else {
                    if (count == offsets.length) {
                        offsets = grow(offsets);
                        lineNumbers = grow(lineNumbers);
                    }
                    offsets[count] = lineStart;
                    lineNumbers[count] = physicalLine;
                    count++;
                }
                if (b == (byte) '\r' && i + 1 < bytes.length && bytes[i + 1] == (byte) '\n') {
                    i += 2;
                } else {
                    i += 1;
                }
                physicalLine++;
                lineStart = i;
            } else {
                i += 1;
            }
        }
        if (lineStart < bytes.length) {
            boolean blank = isBlank(bytes, lineStart, bytes.length);
            if (blank) {
                blankCount++;
            } else {
                if (count == offsets.length) {
                    offsets = grow(offsets);
                    lineNumbers = grow(lineNumbers);
                }
                offsets[count] = lineStart;
                lineNumbers[count] = physicalLine;
                count++;
            }
        }
        long[] trimmedOffsets = count == offsets.length ? offsets : java.util.Arrays.copyOf(offsets, count);
        long[] trimmedLines = count == lineNumbers.length ? lineNumbers : java.util.Arrays.copyOf(lineNumbers, count);
        return new LineIndex(trimmedOffsets, trimmedLines, blankCount, bom, keepBytes ? bytes : null);
    }

    /**
     * Runs the parallel partition read and merges the partitions in file order
     * (prompt 54 items 2 and 4). Requires a pre-scan of {@code file} produced
     * for the same physical file; partitions beyond {@link #PARTITION_BLOCK_ROWS}
     * or the pool parallelism are carved on data-line boundaries.
     *
     * @return the loaded rows and presence flags in file order
     * @throws IOException on I/O errors
     */
    public static JsonlLoadResult loadParallel(File file, LineIndex index, List<String> columns,
                                               Map<String, Class<?>> columnTypes,
                                               JsonParserConfig config, Charset charset) throws IOException {
        long totalRows = index.dataOffsets.length;
        int partitions = Math.min(READ_POOL.getParallelism(),
                (int) Math.min((totalRows + PARTITION_BLOCK_ROWS - 1) / PARTITION_BLOCK_ROWS, Integer.MAX_VALUE));
        List<Callable<PartitionRows>> tasks = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            long startDataLine = (totalRows * p) / partitions;
            long endDataLine = (totalRows * (p + 1)) / partitions;
            if (endDataLine <= startDataLine) {
                continue;
            }
            long byteStart = p == 0 ? 0 : index.dataOffsets[(int) startDataLine];
            long byteEnd = endDataLine < totalRows ? index.dataOffsets[(int) endDataLine] : file.length();
            long firstLine = p == 0 ? 1 : index.dataLineNumbers[(int) startDataLine];
            boolean atFileStart = p == 0;
            tasks.add(new ByteRangeTask(file, byteStart, byteEnd, firstLine, atFileStart,
                    columns, columnTypes, config, charset));
        }
        List<Object[]> rows = new ArrayList<>((int) Math.min(totalRows, Integer.MAX_VALUE));
        List<boolean[]> presence = new ArrayList<>();
        Set<Integer> nestedColumns = new TreeSet<>();
        long skipped = 0;
        try {
            for (Future<PartitionRows> future : READ_POOL.invokeAll(tasks)) {
                PartitionRows partition = future.get();
                rows.addAll(partition.rows);
                presence.addAll(partition.presence);
                nestedColumns.addAll(partition.nestedColumns);
                skipped += partition.skipped;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("Parallel JSONL read interrupted", e);
        } catch (ExecutionException e) {
            throw new CompletionException("Parallel JSONL read failed", e.getCause());
        }
        if (partitions > 0) {
            LOGGER.info("JsonlParallelLoader {} read {} rows in {} partition(s) ({} blank line(s) skipped"
                            + "{})",
                    file.getPath(), rows.size(), partitions, index.blankLineCount,
                    index.bom ? ", BOM detected" : "");
        }
        if (skipped > 0) {
            LOGGER.warn("{} JSONL load skipped {} malformed line(s) (jsonl.load.error.mode=skip_row)",
                    file.getPath(), skipped);
        }
        return new JsonlLoadResult(rows, presence, nestedColumns);
    }

    private static boolean isBlank(byte[] bytes, int start, int end) {
        for (int i = start; i < end; i++) {
            if ((bytes[i] & 0xFF) > 0x20) {
                return false;
            }
        }
        return true;
    }

    private static long[] grow(long[] array) {
        return java.util.Arrays.copyOf(array, array.length * 2);
    }

    /** Rows decoded by one partition plus its local skipped-row count and the
     * nested-JSON holder columns it discovered. */
    private record PartitionRows(List<Object[]> rows, List<boolean[]> presence, long skipped,
                                 Set<Integer> nestedColumns) {
    }

    /**
     * Reads one byte-offset partition of a JSONL file. Positions a
     * {@link FileChannel} at the partition's first line and reads exactly the
     * bytes up to the next line boundary (or EOF); a {@link JsonlRowReader}
     * seeded with the absolute first physical line number decodes them, so
     * file:line diagnostics match a sequential pass. The reader's final
     * skipped-row summary is suppressed and aggregated by the caller. Each
     * partition owns a private {@link JsonlSchemaManager} (thread-confined) and
     * reports the nested-JSON holder columns it marked (prompt 45); the caller
     * replays the union onto the shared manager.
     */
    private static final class ByteRangeTask implements Callable<PartitionRows> {
        private final File file;
        private final long byteStart;
        private final long byteEnd;
        private final long firstDataLine;
        private final boolean atFileStart;
        private final List<String> columns;
        private final Map<String, Class<?>> columnTypes;
        private final JsonParserConfig config;
        private final Charset charset;

        ByteRangeTask(File file, long byteStart, long byteEnd, long firstDataLine, boolean atFileStart,
                      List<String> columns, Map<String, Class<?>> columnTypes,
                      JsonParserConfig config, Charset charset) {
            this.file = file;
            this.byteStart = byteStart;
            this.byteEnd = byteEnd;
            this.firstDataLine = firstDataLine;
            this.atFileStart = atFileStart;
            this.columns = columns;
            this.columnTypes = columnTypes;
            this.config = config;
            this.charset = charset;
        }

        @Override
        public PartitionRows call() {
            try (FileChannel channel = FileChannel.open(file.toPath())) {
                long span = byteEnd - byteStart;
                if (span > Integer.MAX_VALUE) {
                    throw new IOException("Read partition too large: " + span + " bytes");
                }
                byte[] chunk = new byte[(int) span];
                ByteBuffer buffer = ByteBuffer.wrap(chunk);
                int position = 0;
                while (buffer.hasRemaining()) {
                    int n = channel.read(buffer, byteStart + position);
                    if (n < 0) {
                        break;
                    }
                    position += n;
                }
                List<Object[]> rows = new ArrayList<>();
                List<boolean[]> presence = new ArrayList<>();
                JsonlSchemaManager schema = new JsonlSchemaManager(columns, columnTypes, config);
                try (BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(new ByteArrayInputStream(chunk, 0, position), charset));
                     JsonlRowReader reader = new JsonlRowReader(bufferedReader, schema,
                             file.getPath(), config)) {
                    reader.initPartition(firstDataLine, atFileStart);
                    reader.setSuppressSkipSummary(true);
                    while (reader.hasNext()) {
                        Object[] row = reader.nextArray();
                        if (row != null) {
                            rows.add(row);
                            boolean[] present = reader.getLastRowPresent();
                            presence.add(present != null ? present : allPresent(columns.size()));
                        }
                    }
                    return new PartitionRows(rows, presence, reader.getSkippedRowCount(),
                            schema.nestedJsonIndexes());
                }
            } catch (IOException e) {
                throw new CompletionException("Failed to read JSONL byte range " + byteStart + ".." + byteEnd, e);
            }
        }

        private static boolean[] allPresent(int size) {
            boolean[] present = new boolean[size];
            java.util.Arrays.fill(present, true);
            return present;
        }
    }
}