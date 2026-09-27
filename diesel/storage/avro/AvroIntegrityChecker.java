package diesel.storage.avro;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import diesel.ConfigKeys;

/**
 * AVRO block integrity checking - CRC checksums per block, validation at read
 * time, bit-rot / corruption detection and integrity statistics (Prompt 83).
 *
 * <p>The Avro object-container format does not natively store a per-block
 * checksum, so this class computes the {@link CRC32} of every block's
 * <em>decompressed</em> payload and exposes it for comparison. Two references
 * are supported:
 * <ul>
 *   <li><b>In-memory CRC cache</b> - every observed per-block CRC is recorded
 *       keyed by {@code (file, blockIndex)}; a later read of the same block
 *       whose CRC no longer matches is flagged as bit rot.</li>
 *   <li><b>Sidecar {@code .crc} file</b> - when
 *       {@code avro.integrity.store.sidecar} is enabled a compact
 *       {@code <file>.avro.crc} companion is written after the first scan and
 *       used as the expected-CRC baseline, so corruption is detected across
 *       process restarts.</li>
 * </ul>
 *
 * <p>Beyond the CRC comparison the checker performs full structural validation
 * of every block - record count / payload-size varints, truncated payloads,
 * missing or mismatched trailing sync markers, undecompilable compressed
 * payloads. All problems are collected per block and aggregated into an
 * {@link IntegrityReport}; cumulative counters live in {@link IntegrityStats}.
 *
 * <p>Behaviour is configured by {@link #resolve()} reading {@code avro.integrity.*}
 * keys from a system property, then the root {@code config.properties}, then
 * the code defaults:
 * <ul>
 *   <li>{@code avro.integrity.check.on.read} (default {@code true}) - whether
 *       the read path is expected to run {@link #checkOnRead} per block;</li>
 *   <li>{@code avro.integrity.check.on.open} (default {@code false}) - whether
 *       a full-file scan should run when a file is opened;</li>
 *   <li>{@code avro.integrity.fail.on.mismatch} (default {@code false}) -
 *       when {@code true} a CRC mismatch or structural corruption raises
 *       {@link IOException} instead of being recorded and walked past;</li>
 *   <li>{@code avro.integrity.store.sidecar} (default {@code false}) - persist
 *       the observed per-block CRCs to a {@code .crc} companion file for
 *       cross-run bit-rot detection;</li>
 *   <li>{@code avro.integrity.config.file} - overrides the config.properties
 *       location (test-support hook, mirrors the other AVRO config classes).</li>
 * </ul>
 *
 * <p>All IO is performed on fresh {@link RandomAccessFile}/{@link FileChannel}
 * handles per scan and codecs are created inside the scan, so the class is
 * thread-safe; the CRC cache and the {@link IntegrityStats} counters are
 * concurrent collections/atomics.
 *
 * @since Prompt 83
 */
public final class AvroIntegrityChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroIntegrityChecker.class);

    /** Config key: run CRC validation on the read path. */
    public static final String CHECK_ON_READ_KEY = "avro.integrity.check.on.read";
    /** Config key: full-file validation on open. */
    public static final String CHECK_ON_OPEN_KEY = "avro.integrity.check.on.open";
    /** Config key: throw {@link IOException} on a corrupt/mismatching block. */
    public static final String FAIL_ON_MISMATCH_KEY = "avro.integrity.fail.on.mismatch";
    /** Config key: persist per-block CRCs to a {@code .crc} sidecar. */
    public static final String STORE_SIDECAR_KEY = "avro.integrity.store.sidecar";

    /** Code-level default for read-path validation. */
    public static final boolean DEFAULT_CHECK_ON_READ = true;
    /** Code-level default for validate-on-open. */
    public static final boolean DEFAULT_CHECK_ON_OPEN = false;
    /** Code-level default for fail-fast on mismatch. */
    public static final boolean DEFAULT_FAIL_ON_MISMATCH = false;
    /** Code-level default for sidecar persistence. */
    public static final boolean DEFAULT_STORE_SIDECAR = false;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.integrity.config.file";

    /** The fixed 16-byte Avro sync marker size. */
    private static final int SYNC_SIZE = 16;

    /** Marker {@code -1} means "no expected CRC known". */
    static final long NO_EXPECTED_CRC = -1L;

    /** Sidecar file suffix ({@code <target>.avro.crc}). */
    public static final String SIDECAR_SUFFIX = ".crc";
    /** First line of a sidecar file. */
    public static final String SIDECAR_MAGIC = "diesel-avro-crc-v1";

    private final boolean checkOnRead;
    private final boolean checkOnOpen;
    private final boolean failOnMismatch;
    private final boolean storeSidecar;

    /** Observed per-block CRCs, keyed {@code file -> (blockIndex -> crc)}. */
    private final ConcurrentHashMap<File, ConcurrentHashMap<Integer, Long>> observedCrcs =
            new ConcurrentHashMap<>();

    /** Cumulative integrity statistics. */
    private final IntegrityStats stats = new IntegrityStats();

    /**
     * Per-block integrity outcome.
     *
     * <p>A structurally intact block with no reference CRC is reported
     * {@code valid} with {@code expectedCrc == -1}; a block whose computed CRC
     * differs from the reference is <em>invalid</em> (bit rot / corruption).
     * Structural problems (truncation, sync mismatch, failed decompression)
     * also produce {@code valid == false} with the problems listed.
     *
     * @param blockIndex        zero-based block sequence number
     * @param recordCount       records declared by the block ({@code -1} when the
     *                          block header was unreadable)
     * @param compressedSize    compressed payload bytes (as stored on disk)
     * @param decompressedSize  payload bytes after codec decompression
     * @param computedCrc       CRC32 of the decompressed payload
     * @param expectedCrc       reference CRC32, or {@code -1} when none known
     * @param valid             whether the block passed integrity checks
     * @param problems          per-block problem descriptions (empty when valid)
     * @param contentEndOffset  file offset just past this block's trailing sync
     *                          marker (the next block's header position)
     * @param scanTimeNanos     wall-clock time spent on this block
     */
    public record BlockIntegrityResult(
            int blockIndex,
            long recordCount,
            long compressedSize,
            long decompressedSize,
            long computedCrc,
            long expectedCrc,
            boolean valid,
            List<String> problems,
            long contentEndOffset,
            long scanTimeNanos) {

        /** Whether a reference CRC existed and disagreed with the computed one. */
        public boolean crcMismatch() {
            return expectedCrc >= 0 && computedCrc != expectedCrc;
        }

        @Override
        public String toString() {
            return "BlockIntegrityResult{block=" + blockIndex
                    + ", records=" + recordCount
                    + ", compressed=" + compressedSize
                    + ", decompressed=" + decompressedSize
                    + ", crc=" + Long.toHexString(computedCrc)
                    + ", expected=" + (expectedCrc < 0 ? "none" : Long.toHexString(expectedCrc))
                    + ", valid=" + valid
                    + ", problems=" + problems + '}';
        }
    }

    /**
     * Aggregated outcome of {@link #validateFile(File)}.
     *
     * @param file                  the scanned Avro file
     * @param blocksChecked         number of data blocks examined
     * @param blocksValid           number of blocks that passed integrity checks
     * @param blocksCorrupted       number of blocks flagged corrupt / bit-rotted
     * @param totalCompressedBytes  sum of stored (compressed) payload bytes
     * @param totalDecompressedBytes sum of decompressed payload bytes
     * @param totalScanTimeNanos    wall-clock duration of the scan
     * @param checkedAt             when the scan completed
     * @param blocks                per-block results (file order)
     * @param problems              file-level problems (empty when fully valid)
     */
    public record IntegrityReport(
            File file,
            long blocksChecked,
            long blocksValid,
            long blocksCorrupted,
            long totalCompressedBytes,
            long totalDecompressedBytes,
            long totalScanTimeNanos,
            Instant checkedAt,
            List<BlockIntegrityResult> blocks,
            List<String> problems) {

        /** Whether every block passed and no file-level problem was reported. */
        public boolean fullyValid() {
            return blocksCorrupted == 0 && problems.isEmpty();
        }

        /** Human-readable single-line summary for logging. */
        public String summary() {
            return String.format(
                    "AvroIntegrityChecker{%s: blocks=%d valid=%d corrupt=%d compressed=%dB "
                            + "decompressed=%dB scan=%dms fullyValid=%s}",
                    file == null ? "<null>" : file.getName(),
                    blocksChecked, blocksValid, blocksCorrupted,
                    totalCompressedBytes, totalDecompressedBytes,
                    totalScanTimeNanos / 1_000_000, fullyValid());
        }
    }

    /**
     * Aggregated outcome of {@link #scanAll(File)} across a data directory.
     *
     * @param dataDir     the scanned directory
     * @param files       per-file reports (file-name order)
     * @param filesValid  reports whose file is fully valid
     * @param filesCorrupt reports with at least one corrupt block or problem
     * @param totalBlocks number of blocks checked across all files
     * @param totalCorrupt number of corrupt blocks across all files
     */
    public record DirectoryIntegrityReport(
            File dataDir,
            List<IntegrityReport> files,
            long filesValid,
            long filesCorrupt,
            long totalBlocks,
            long totalCorrupt) {

        public boolean allValid() {
            return filesCorrupt == 0;
        }
    }

    /**
     * Cumulative integrity-check statistics (thread-safe). Snapshot via
     * {@link #snapshot()}; counters reset via {@link #reset()}.
     */
    public static final class IntegrityStats {
        private final AtomicLong checks = new AtomicLong();
        private final AtomicLong filesValidated = new AtomicLong();
        private final AtomicLong blocksValidated = new AtomicLong();
        private final AtomicLong blocksCorrupted = new AtomicLong();
        private final AtomicLong crcMismatches = new AtomicLong();
        private final AtomicLong bytesValidated = new AtomicLong();
        private final AtomicLong totalScanTimeNanos = new AtomicLong();

        /**
         * Point-in-time snapshot of the counters.
         *
         * @param checks              validation invocations completed
         * @param filesValidated      files scanned by {@code validateFile}/{@code scanAll}
         * @param blocksValidated     blocks checked (valid + corrupt)
         * @param blocksCorrupted     blocks flagged corrupt / bit-rotted
         * @param crcMismatches       CRC comparisons that disagreed
         * @param bytesValidated      decompressed payload bytes validated
         * @param totalScanTimeNanos  accumulated scan time
         */
        public record Snapshot(
                long checks,
                long filesValidated,
                long blocksValidated,
                long blocksCorrupted,
                long crcMismatches,
                long bytesValidated,
                long totalScanTimeNanos) {
        }

        void recordCheck(long scanTimeNanos) {
            checks.incrementAndGet();
            totalScanTimeNanos.addAndGet(scanTimeNanos);
        }

        void recordFile() {
            filesValidated.incrementAndGet();
        }

        void recordBlocks(long valid, long corrupt, long bytes) {
            blocksValidated.addAndGet(valid + corrupt);
            blocksCorrupted.addAndGet(corrupt);
            bytesValidated.addAndGet(bytes);
        }

        void recordCrcMismatch() {
            crcMismatches.incrementAndGet();
        }

        /** Returns an immutable snapshot of every counter. */
        public Snapshot snapshot() {
            return new Snapshot(checks.get(), filesValidated.get(), blocksValidated.get(),
                    blocksCorrupted.get(), crcMismatches.get(), bytesValidated.get(),
                    totalScanTimeNanos.get());
        }

        /** Resets all counters to zero. */
        public void reset() {
            checks.set(0);
            filesValidated.set(0);
            blocksValidated.set(0);
            blocksCorrupted.set(0);
            crcMismatches.set(0);
            bytesValidated.set(0);
            totalScanTimeNanos.set(0);
        }
    }

    /**
     * Creates a checker with explicit behaviour flags.
     *
     * @param checkOnRead    whether the read path runs {@link #checkOnRead}
     * @param checkOnOpen    whether files are fully validated on open
     * @param failOnMismatch whether corruption raises {@link IOException}
     * @param storeSidecar   whether observed CRCs are persisted to a sidecar
     */
    public AvroIntegrityChecker(boolean checkOnRead, boolean checkOnOpen,
                                boolean failOnMismatch, boolean storeSidecar) {
        this.checkOnRead = checkOnRead;
        this.checkOnOpen = checkOnOpen;
        this.failOnMismatch = failOnMismatch;
        this.storeSidecar = storeSidecar;
    }

    /**
     * Resolves the checker from a system property, then the root
     * {@code config.properties}, then the code defaults.
     *
     * @return a configured checker (never {@code null})
     */
    public static AvroIntegrityChecker resolve() {
        return new AvroIntegrityChecker(
                getBoolean(CHECK_ON_READ_KEY, DEFAULT_CHECK_ON_READ),
                getBoolean(CHECK_ON_OPEN_KEY, DEFAULT_CHECK_ON_OPEN),
                getBoolean(FAIL_ON_MISMATCH_KEY, DEFAULT_FAIL_ON_MISMATCH),
                getBoolean(STORE_SIDECAR_KEY, DEFAULT_STORE_SIDECAR));
    }

    /** Whether read-path CRC validation is enabled. */
    public boolean checkOnRead() {
        return checkOnRead;
    }

    /** Whether a full-file scan runs on open. */
    public boolean checkOnOpen() {
        return checkOnOpen;
    }

    /** Whether corruption raises {@link IOException} instead of being recorded. */
    public boolean failOnMismatch() {
        return failOnMismatch;
    }

    /** Whether observed CRCs are persisted to a {@code .crc} sidecar. */
    public boolean storeSidecar() {
        return storeSidecar;
    }

    /** Returns the cumulative integrity statistics. */
    public IntegrityStats stats() {
        return stats;
    }

    /** Resets the cumulative integrity statistics. */
    public void resetStats() {
        stats.reset();
    }

    // ─── CRC computation ────────────────────────────────────────────

    /**
     * Computes the CRC32 of the given payload bytes.
     *
     * @param data the bytes to checksum (may be {@code null} or empty)
     * @return CRC32 value (0 for null/empty input)
     */
    public static long computeCrc(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    // ─── Full-file validation ───────────────────────────────────────

    /**
     * Scans every data block of the given Avro file: reads each block's
     * header, compressed payload and trailing sync marker from the raw bytes,
     * decompresses the payload with the header codec and computes its CRC32.
     * The computed CRC is compared against the reference (in-memory cache or
     * {@code .crc} sidecar when present) and the references are updated with
     * the fresh observation. When {@code avro.integrity.store.sidecar} is on
     * the updated baseline is written back after a complete scan.
     *
     * <p>Structural faults (negative/invalid size, truncation, sync mismatch,
     * failed decompression) and CRC mismatches are recorded; the walk stops at
     * the first faulty block because everything past it is unverifiable. With
     * {@code avro.integrity.fail.on.mismatch} the corresponding
     * {@link IOException} is thrown instead.
     *
     * @param avroFile the Avro object-container file
     * @return the aggregated integrity report
     * @throws IOException if the file is missing, its header cannot be parsed
     *                     (not Avro data), or {@code failOnMismatch} is set and
     *                     a fault is found
     */
    public IntegrityReport validateFile(File avroFile) throws IOException {
        long start = System.nanoTime();
        if (avroFile == null || !avroFile.isFile()) {
            throw new IOException(AvroFileConstants.MSG_FILE_NOT_FOUND + avroFile);
        }

        HeaderProbe probe = readHeader(avroFile);
        List<BlockIntegrityResult> results = new ArrayList<>();
        List<String> fileProblems = new ArrayList<>();
        boolean clean = true;
        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "r");
             FileChannel ch = raf.getChannel()) {
            long fileLen = raf.length();
            long pos = probe.headerEnd();
            int index = 0;
            while (pos < fileLen) {
                if (isTrailingFlushMarker(ch, pos, fileLen, probe, avroFile)) {
                    break;
                }
                BlockIntegrityResult r = validateOneBlock(ch, avroFile, pos, fileLen, probe, index);
                results.add(r);
                if (!r.valid()) {
                    clean = false;
                    fileProblems.addAll(r.problems());
                    break;
                }
                pos = r.contentEndOffset();
                index++;
            }
        }
        if (clean && storeSidecar && !results.isEmpty()) {
            writeSidecar(avroFile, results);
        }

        long checked = results.size();
        long valid = 0;
        long corrupt = 0;
        long compressed = 0;
        long decompressed = 0;
        List<Integer> corruptIndexes = new ArrayList<>();
        for (BlockIntegrityResult r : results) {
            compressed += r.compressedSize();
            decompressed += r.decompressedSize();
            if (r.valid()) {
                valid++;
            } else {
                corrupt++;
                corruptIndexes.add(r.blockIndex());
                if (r.crcMismatch()) {
                    stats.recordCrcMismatch();
                }
            }
        }

        IntegrityReport report = new IntegrityReport(
                avroFile, checked, valid, corrupt, compressed, decompressed,
                System.nanoTime() - start, Instant.now(),
                Collections.unmodifiableList(results),
                Collections.unmodifiableList(fileProblems));

        stats.recordCheck(report.totalScanTimeNanos());
        stats.recordFile();
        stats.recordBlocks(valid, corrupt, decompressed);

        if (corrupt > 0) {
            LOGGER.warn("Avro integrity check: {} corrupt block(s) in {} - {}",
                    corrupt, avroFile, report.summary());
        } else {
            LOGGER.debug("Avro integrity check passed: {}", report.summary());
        }
        return report;
    }

    /**
     * Walks and validates one block starting at {@code headerPos}, returning
     * the block result. Structural faults either raise {@link IOException}
     * (fail mode) or produce a {@code valid == false} result (record mode).
     */
    private BlockIntegrityResult validateOneBlock(FileChannel ch, File avroFile,
                                                  long headerPos, long fileLen,
                                                  HeaderProbe probe, int blockIndex)
            throws IOException {
        long blockStart = System.nanoTime();
        long pos = headerPos;
        long count;
        long size;
        try {
            long[] cv = readZigzagVlq(ch, pos);
            count = cv[0];
            pos = cv[1];
            long[] sv = readZigzagVlq(ch, pos);
            size = sv[0];
            pos = sv[1];
        } catch (IOException e) {
            return mismatchOrInvalid(headerPos, blockStart, blockIndex, e.getMessage());
        }
        if (count < 0 || size < 0 || size > Integer.MAX_VALUE) {
            return mismatchOrInvalid(headerPos, blockStart, blockIndex,
                    AvroFileConstants.MSG_INVALID_BLOCK_HEADER + count + AvroFileConstants.MSG_SIZE_FIELD + size
                            + ") at offset " + headerPos);
        }

        long payloadStart = pos;
        long syncPos = payloadStart + size;
        if (syncPos + SYNC_SIZE > fileLen) {
            return mismatchOrInvalid(headerPos, blockStart, blockIndex,
                    AvroFileConstants.MSG_TRUNCATED_BLOCK + headerPos + AvroFileConstants.MSG_EXPECTED_SYNC_MARKER
                            + syncPos + AvroFileConstants.MSG_FILE_ENDS_AT + fileLen);
        }

        byte[] compressed = new byte[(int) size];
        readFully(ch, payloadStart, compressed, fileLen, avroFile);

        byte[] sync = new byte[SYNC_SIZE];
        readFully(ch, syncPos, sync, fileLen, avroFile);
        if (!Arrays.equals(sync, probe.sync())) {
            return mismatchOrInvalid(headerPos, blockStart, blockIndex,
                    AvroFileConstants.MSG_SYNC_MARKER_MISMATCH + syncPos
                            + " (corrupt file or interrupted write)");
        }

        byte[] decompressed;
        try {
            decompressed = decompress(compressed, probe.codec());
        } catch (IOException e) {
            return mismatchOrInvalid(headerPos, blockStart, blockIndex,
                    "Failed to decompress block " + blockIndex + " (bit rot?): " + e.getMessage());
        }

        long computed = computeCrc(decompressed);
        List<String> problems = new ArrayList<>();
        long expected = NO_EXPECTED_CRC;
        if (probe.hasCrc(blockIndex)) {
            expected = probe.crc(blockIndex);
            if (expected != computed) {
                if (failOnMismatch) {
                    throw new IOException("Avro integrity: block " + blockIndex + " of "
                            + avroFile + " CRC mismatch: computed "
                            + Long.toHexString(computed) + ", expected "
                            + Long.toHexString(expected) + " (bit rot / corruption)");
                }
                problems.add("CRC mismatch for block " + blockIndex + ": computed="
                        + Long.toHexString(computed) + ", expected=" + Long.toHexString(expected)
                        + " (bit rot / corruption)");
            }
        }

        // Record / refresh the observation for future comparisons.
        observedCrcs.computeIfAbsent(avroFile, k -> new ConcurrentHashMap<>())
                .put(blockIndex, computed);
        probe.remember(blockIndex, computed);

        return new BlockIntegrityResult(blockIndex, count, size, decompressed.length,
                computed, expected, problems.isEmpty(),
                Collections.unmodifiableList(problems), syncPos + SYNC_SIZE, System.nanoTime() - blockStart);
    }

    /**
     * In fail mode raises {@link IOException} carrying the {@code message};
     * otherwise returns an <em>invalid</em> block result so the caller can
     * record it and stop the walk. The single {@link BlockIntegrityResult}
     * supplied (never actually returned, only used for type flow) is
     * synthesised by the caller-side invocation below.
     */
    private BlockIntegrityResult mismatchOrInvalid(long headerPos, long blockStart,
                                                   int blockIndex, String message)
            throws IOException {
        if (failOnMismatch) {
            throw new IOException("Avro integrity failed at offset " + headerPos
                    + " of block " + blockIndex + ": " + message);
        }
        return new BlockIntegrityResult(blockIndex, -1, 0, 0, 0L, NO_EXPECTED_CRC, false,
                List.of(message), -1L, System.nanoTime() - blockStart);
    }

    private byte[] decompress(byte[] compressed, Codec codec) throws IOException {
        if (codec == null) {
            return compressed;
        }
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed));
        byte[] data = new byte[out.remaining()];
        out.get(data);
        return data;
    }

    // ─── Single-block validation ────────────────────────────────────

    /**
     * Validates a single data block by its ordinal index. The file is walked
     * from the header to the requested block; a structurally faulty block
     * before it raises {@link IOException} in fail mode or aborts the walk
     * (the result is {@code null} then).
     *
     * @param avroFile   the Avro object-container file
     * @param blockIndex zero-based block index
     * @return the block's integrity result, or {@code null} when the file has
     *         fewer than {@code blockIndex + 1} blocks or a fault blocks the walk
     * @throws IOException if the file is missing or its header cannot be parsed
     */
    public BlockIntegrityResult validateBlock(File avroFile, int blockIndex) throws IOException {
        if (blockIndex < 0) {
            throw new IllegalArgumentException("blockIndex must be >= 0, got " + blockIndex);
        }
        HeaderProbe probe = readHeader(avroFile);
        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "r");
             FileChannel ch = raf.getChannel()) {
            long fileLen = raf.length();
            long pos = probe.headerEnd();
            int index = 0;
            while (pos < fileLen) {
                if (fileLen - pos == SYNC_SIZE) {
                    return null;
                }
                long headerPos = pos;
                long[] cv = readZigzagVlq(ch, pos);
                long count = cv[0];
                pos = cv[1];
                long[] sv = readZigzagVlq(ch, pos);
                long size = sv[0];
                pos = sv[1];
                if (count < 0 || size < 0 || size > Integer.MAX_VALUE) {
                    return null;
                }
                if (index == blockIndex) {
                    return validateOneBlock(ch, avroFile, headerPos, fileLen, probe, index);
                }
                pos = pos + size + SYNC_SIZE;
                index++;
            }
        }
        return null;
    }

    // ─── Read-path hook ─────────────────────────────────────────────

    /**
     * Read-time CRC validation hook. Computes the CRC of a freshly decoded
     * block payload and compares it against the reference observed for that
     * (file, block) previously (from {@link #validateFile} or an earlier
     * {@code checkOnRead}); the fresh observation is recorded for the next
     * comparison. With {@code avro.integrity.fail.on.mismatch} a mismatch
     * raises {@link IllegalStateException} instead of returning {@code false}.
     *
     * @param avroFile            the file being read
     * @param blockIndex          zero-based block index
     * @param decompressedPayload the decompressed payload bytes
     * @return {@code true} when the block passed, or no reference was known yet
     */
    public boolean checkOnRead(File avroFile, int blockIndex, byte[] decompressedPayload) {
        long start = System.nanoTime();
        long computed = computeCrc(decompressedPayload);
        ConcurrentHashMap<Integer, Long> map = observedCrcs.get(avroFile);
        Long expected = map == null ? null : map.get(blockIndex);
        long scanNanos = System.nanoTime() - start;

        if (expected != null && expected != computed) {
            stats.recordCrcMismatch();
            stats.recordCheck(scanNanos);
            stats.recordBlocks(0, 1, decompressedPayload == null ? 0 : decompressedPayload.length);
            if (failOnMismatch) {
                throw new IllegalStateException("Avro block " + blockIndex + " in " + avroFile
                        + " changed on disk: computed CRC " + Long.toHexString(computed)
                        + " != expected " + Long.toHexString(expected) + " (bit rot)");
            }
            LOGGER.warn("Avro integrity: block {} in {} changed on disk (computed CRC {}, "
                            + "expected {}) - bit rot or external modification",
                    blockIndex, avroFile, Long.toHexString(computed), Long.toHexString(expected));
            return false;
        }
        observedCrcs.computeIfAbsent(avroFile, k -> new ConcurrentHashMap<>())
                .put(blockIndex, computed);
        stats.recordCheck(scanNanos);
        stats.recordBlocks(1, 0, decompressedPayload == null ? 0 : decompressedPayload.length);
        return true;
    }

    // ─── Directory scan ─────────────────────────────────────────────

    /**
     * Validates every {@code .avro} data file in a data directory.
     *
     * @param dataDir the directory to scan (missing/non-directory → empty report)
     * @return a directory aggregate of per-file reports
     * @throws IOException if an Avro file's header cannot be parsed
     */
    public DirectoryIntegrityReport scanAll(File dataDir) throws IOException {
        if (dataDir == null || !dataDir.isDirectory()) {
            return new DirectoryIntegrityReport(dataDir, List.of(), 0, 0, 0, 0);
        }
        List<File> avroFiles = new ArrayList<>();
        File[] listed = dataDir.listFiles();
        if (listed != null) {
            for (File f : listed) {
                if (f.isFile() && f.getName().toLowerCase(Locale.ROOT).endsWith(".avro")) {
                    avroFiles.add(f);
                }
            }
        }
        avroFiles.sort(Comparator.comparing(File::getName, String.CASE_INSENSITIVE_ORDER));

        List<IntegrityReport> reports = new ArrayList<>();
        long filesValid = 0;
        long filesCorrupt = 0;
        long totalBlocks = 0;
        long totalCorrupt = 0;
        for (File f : avroFiles) {
            IntegrityReport r = validateFile(f);
            reports.add(r);
            totalBlocks += r.blocksChecked();
            totalCorrupt += r.blocksCorrupted();
            if (r.fullyValid()) {
                filesValid++;
            } else {
                filesCorrupt++;
            }
        }
        return new DirectoryIntegrityReport(dataDir,
                Collections.unmodifiableList(reports), filesValid, filesCorrupt,
                totalBlocks, totalCorrupt);
    }

    // ─── Sidecar store ──────────────────────────────────────────────

    /** Returns the sidecar path for the given Avro file ({@code <file>.avro.crc}). */
    public static File sidecarFile(File avroFile) {
        File abs = avroFile.getAbsoluteFile();
        return new File(abs.getParentFile(), abs.getName() + SIDECAR_SUFFIX);
    }

    /**
     * Reads the {@code .crc} sidecar companion into a per-block CRC map.
     *
     * @param avroFile the Avro file whose sidecar is read
     * @return the block-index → CRC32 map (empty when no usable sidecar exists)
     * @throws IOException on I/O errors
     */
    public Map<Integer, Long> readSidecar(File avroFile) throws IOException {
        File sidecar = sidecarFile(avroFile);
        if (!sidecar.isFile()) {
            return Map.of();
        }
        Map<Integer, Long> crcs = new ConcurrentHashMap<>();
        for (String line : Files.readAllLines(sidecar.toPath(), StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.equals(SIDECAR_MAGIC)) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            if (parts.length == 2) {
                try {
                    crcs.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
                } catch (NumberFormatException ignored) {
                    // malformed line - skipped, valid entries are retained
                }
            }
        }
        return crcs;
    }

    /**
     * Writes the {@code .crc} sidecar companion for the validated file so a
     * future scan can compare against a persisted baseline.
     *
     * @param avroFile the validated Avro file
     * @param results  the per-block results (only valid blocks are stored)
     * @throws IOException on I/O errors
     */
    public void writeSidecar(File avroFile, List<BlockIntegrityResult> results) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(SIDECAR_MAGIC).append('\n');
        sb.append("# diesel avro per-block CRC32 baseline\n");
        for (BlockIntegrityResult r : results) {
            if (r.valid()) {
                sb.append(r.blockIndex()).append(' ').append(r.computedCrc()).append('\n');
            }
        }
        File sidecar = sidecarFile(avroFile);
        File parent = sidecar.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        Files.write(sidecar.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    // ─── Header probing ─────────────────────────────────────────────

    /** Resolved state needed to walk blocks and compare CRCs. */
    private static final class HeaderProbe {
        private final byte[] sync;
        private final long headerEnd;
        private final Codec codec;
        private final Map<Integer, Long> expectedCrcs;

        HeaderProbe(byte[] sync, long headerEnd, Codec codec, Map<Integer, Long> expected) {
            this.sync = sync;
            this.headerEnd = headerEnd;
            this.codec = codec;
            this.expectedCrcs = expected;
        }

        byte[] sync() {
            return sync;
        }

        long headerEnd() {
            return headerEnd;
        }

        Codec codec() {
            return codec;
        }

        boolean hasCrc(int blockIndex) {
            return expectedCrcs.containsKey(blockIndex);
        }

        long crc(int blockIndex) {
            return expectedCrcs.get(blockIndex);
        }

        void remember(int blockIndex, long crc) {
            expectedCrcs.put(blockIndex, crc);
        }
    }

    /** Reads the file header via {@link AvroDataFileReader} and resolves the codec. */
    private HeaderProbe readHeader(File avroFile) throws IOException {
        byte[] sync;
        long headerEnd;
        String codecName;
        try (AvroDataFileReader probe = new AvroDataFileReader(avroFile)) {
            sync = probe.getSyncMarker();
            headerEnd = probe.getPosition();
            codecName = probe.getCodecName();
        }
        Codec codec = "null".equals(codecName) ? null : createCodec(codecName);
        Map<Integer, Long> expected = new ConcurrentHashMap<>(
                observedCrcs.getOrDefault(avroFile, new ConcurrentHashMap<>()));
        if (storeSidecar) {
            expected.putAll(readSidecar(avroFile));
        }
        return new HeaderProbe(sync, headerEnd, codec, expected);
    }

    /**
     * Creates a {@link Codec} for the given codec name. {@code
     * CodecFactory#createInstance()} is protected in this Avro version, so the
     * instance is obtained reflectively.
     */
    private static Codec createCodec(String codecName) throws IOException {
        try {
            Method createInstance = CodecFactory.class.getDeclaredMethod("createInstance");
            createInstance.setAccessible(true);
            return (Codec) createInstance.invoke(CodecFactory.fromString(codecName));
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IOException(AvroFileConstants.MSG_UNSUPPORTED_CODEC + codecName + "': " + e.getMessage(), e);
        }
    }

    // ─── Raw I/O helpers (mirror AvroSyncMarkerManager) ─────────────

    private static long[] readZigzagVlq(FileChannel ch, long pos) throws IOException {
        long value = 0;
        int shift = 0;
        long p = pos;
        while (true) {
            ByteBuffer one = ByteBuffer.allocate(1);
            int got = ch.read(one, p);
            if (got < 0) {
                throw new IOException("expected varint at offset " + p + ", file ends");
            }
            byte b = one.array()[0];
            p++;
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
            if (shift > 63) {
                throw new IOException("malformed varint at offset " + pos);
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

    /**
     * Checks whether the remaining bytes at {@code pos} form a trailing flush
     * marker (exactly SYNC_SIZE bytes matching the header sync).
     */
    private static boolean isTrailingFlushMarker(FileChannel ch, long pos, long fileLen,
                                                  HeaderProbe probe, File avroFile) throws IOException {
        if (fileLen - pos != SYNC_SIZE) {
            return false;
        }
        byte[] tail = new byte[SYNC_SIZE];
        readFully(ch, pos, tail, fileLen, avroFile);
        return Arrays.equals(tail, probe.sync());
    }

    // ─── Configuration helpers ──────────────────────────────────────

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return raw != null && (raw.equalsIgnoreCase("true") || raw.equals("1"));
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (var in = Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroIntegrityChecker{checkOnRead=" + checkOnRead
                + ", checkOnOpen=" + checkOnOpen
                + ", failOnMismatch=" + failOnMismatch
                + ", storeSidecar=" + storeSidecar
                + ", stats=" + stats.snapshot() + '}';
    }
}