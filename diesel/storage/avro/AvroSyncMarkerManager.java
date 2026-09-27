package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import diesel.ConfigKeys;

/**
 * Centralised AVRO sync marker management (Prompt 77).
 *
 * <p>The class owns the four concerns of the Avro object-container-file sync
 * marker on top of the pre-Prompt-77 code that already wrote and validated
 * markers inline:
 * <ul>
 *   <li><b>Generation</b> — {@link #generateSyncMarker()} produces random
 *       16-byte markers with {@link SecureRandom}; tests may inject their own
 *       random source via {@link #generateSyncMarker(SecureRandom)}.</li>
 *   <li><b>Integrity validation</b> — {@link #validateIntegrity(File)} walks
 *       the raw file bytes after the header, reads every block's
 *       {@code [count, size, payload, sync]} layout and verifies that each
 *       trailing 16 bytes equal the header's sync marker. It never throws for
 *       data-level problems; it returns an {@link IntegrityResult} report.</li>
 *   <li><b>Crash recovery</b> — {@link #recoverToLastValidBlock(File)} locates
 *       the last fully-written block (the recovery point reported as
 *       {@link IntegrityResult#truncationOffset()}) and truncates the file to
 *       that byte offset, with an optional {@code .bak} copy of the original
 *       tail.</li>
 *   <li><b>Scanning</b> — {@link #scanAllMarkers(File)} enumerates every sync
 *       marker (header, per-block trailing, trailing flush), and
 *       {@link #isSyncMarkerValid(File, long)} checks a single offset.</li>
 * </ul>
 *
 * <p>Behaviour is configured by {@link #resolve()} reading
 * {@code avro.syncmarker.validate.strict} (default {@code true}) and
 * {@code avro.syncmarker.recovery.backup} (default {@code true}) from a system
 * property, then the root {@code config.properties}, then the code defaults.
 * The recovery scan always runs in lenient mode so a crashed tail can never
 * prevent recovery; the strict flag governs {@link #validateIntegrity(File)}
 * only.
 *
 * <p>The class is thread-safe: the configuration is immutable, the instance
 * keeps no mutable state, and every scan opens its own {@link RandomAccessFile}.
 *
 * @since Prompt 77
 */
public final class AvroSyncMarkerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSyncMarkerManager.class);

    /** The fixed 16-byte Avro sync marker size. */
    public static final int SYNC_SIZE = 16;

    /** Config key: strict validation mode. */
    public static final String VALIDATE_STRICT_KEY = "avro.syncmarker.validate.strict";
    /** Config key: whether recovery copies the original tail to a {@code .bak} file. */
    public static final String RECOVERY_BACKUP_KEY = "avro.syncmarker.recovery.backup";

    /** Code-level default for strict validation. */
    public static final boolean DEFAULT_VALIDATE_STRICT = true;
    /** Code-level default for recovery backups. */
    public static final boolean DEFAULT_RECOVERY_BACKUP = true;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.syncmarker.config.file";

    private final boolean validateStrict;
    private final boolean createBackup;

    /**
     * A single sync marker occurrence inside a file.
     *
     * @param marker     the 16 marker bytes
     * @param fileOffset file offset of the marker's first byte
     * @param blockIndex data block the marker terminates, or {@code -1} for the
     *                   header marker and the trailing flush marker
     */
    public record SyncMarkerInfo(byte[] marker, long fileOffset, int blockIndex) {
    }

    /**
     * Outcome of {@link #validateIntegrity(File)}.
     *
     * @param valid             {@code true} when the file is intact (or, in
     *                          lenient mode, only has a recoverable truncated
     *                          tail and no corruption)
     * @param file              the scanned Avro file
     * @param markers           every sync marker found (header, per-block, flush)
     * @param totalBlocks       number of data blocks whose payload and sync
     *                          marker are fully present
     * @param totalPayloadBytes compressed payload bytes over those blocks
     * @param truncationOffset  byte offset where the file may safely be cut to
     *                          recover all valid data; {@code -1} when the file
     *                          is fully intact
     * @param errors            human-readable problems; empty when {@code valid}
     */
    public record IntegrityResult(boolean valid, File file, List<SyncMarkerInfo> markers,
                                  long totalBlocks, long totalPayloadBytes,
                                  long truncationOffset, List<String> errors) {
    }

    /**
     * Outcome of {@link #recoverToLastValidBlock(File)}.
     *
     * @param file            the Avro file (after truncation, if any)
     * @param truncated       whether the file was actually truncated
     * @param blocksRecovered number of fully-valid data blocks preserved
     * @param recordsRecovered total records declared by the preserved blocks
     * @param lastValidMarker the header sync marker (the marker verified for
     *                        every preserved block)
     * @param truncatedAt     byte offset the file ends at after recovery;
     *                        {@code -1} when no truncation happened
     * @param backupCreated   whether a {@code .bak} copy of the original tail
     *                        was written
     * @param warnings        non-fatal notes collected during the scan
     */
    public record RecoveryResult(File file, boolean truncated, int blocksRecovered,
                                 long recordsRecovered, byte[] lastValidMarker,
                                 long truncatedAt, boolean backupCreated, List<String> warnings) {
    }

    /**
     * Creates a manager with explicit behaviour flags.
     *
     * @param validateStrict whether {@link #validateIntegrity(File)} treats a
     *                       truncated (interrupted-write) tail as a hard error
     * @param createBackup   whether {@link #recoverToLastValidBlock(File)}
     *                       writes a {@code .bak} copy before truncating
     */
    public AvroSyncMarkerManager(boolean validateStrict, boolean createBackup) {
        this.validateStrict = validateStrict;
        this.createBackup = createBackup;
    }

    /** Whether strict validation is enabled. */
    public boolean validateStrict() {
        return validateStrict;
    }

    /** Whether recovery writes a backup before truncating. */
    public boolean createBackup() {
        return createBackup;
    }

    /**
     * Resolves the manager from a system property, then the root
     * {@code config.properties}, then the code defaults.
     *
     * @return a configured manager (never {@code null})
     */
    public static AvroSyncMarkerManager resolve() {
        boolean strict = getBoolean(VALIDATE_STRICT_KEY, DEFAULT_VALIDATE_STRICT);
        boolean backup = getBoolean(RECOVERY_BACKUP_KEY, DEFAULT_RECOVERY_BACKUP);
        return new AvroSyncMarkerManager(strict, backup);
    }

    // ─── Marker generation ──────────────────────────────────────────

    /**
     * Generates a random 16-byte sync marker using a fresh {@link SecureRandom}.
     *
     * @return a newly generated marker
     */
    public static byte[] generateSyncMarker() {
        return generateSyncMarker(new SecureRandom());
    }

    /**
     * Generates a random 16-byte sync marker using the given random source.
     *
     * @param random the random source to fill the marker bytes
     * @return a newly generated marker
     * @throws IllegalArgumentException when {@code random} is {@code null}
     */
    public static byte[] generateSyncMarker(SecureRandom random) {
        if (random == null) {
            throw new IllegalArgumentException("random must not be null");
        }
        byte[] marker = new byte[SYNC_SIZE];
        random.nextBytes(marker);
        return marker;
    }

    // ─── Integrity validation ───────────────────────────────────────

    /**
     * Validates every sync marker in the given Avro data file.
     *
     * <p>The file is walked byte-by-byte after its header; each data block's
     * trailing 16 bytes must equal the header's sync marker. A trailing flush
     * marker (a sync-only 16-byte region) is accepted as a clean file end.
     * Data-level problems are never thrown — they are collected into
     * {@link IntegrityResult#errors()}. In strict mode a truncated block
     * (file ends inside a block payload or its sync marker) raises
     * {@link IOException}; in lenient mode it is recorded as a recoverable
     * interruption, and {@link IntegrityResult#valid()} stays {@code true}
     * when it is the only problem.
     *
     * @param avroFile the Avro object-container file
     * @return the integrity report
     * @throws IOException if the file is missing, its header cannot be parsed
     *                     (not Avro data), or (strict mode) an incomplete block
     *                     is found
     */
    public IntegrityResult validateIntegrity(File avroFile) throws IOException {
        HeaderProbe probe = readHeader(avroFile);
        CoreScan scan = coreScan(avroFile, probe.sync(), probe.headerEnd(), validateStrict);

        boolean hasCorruption = scan.problems().stream()
                .anyMatch(p -> !p.startsWith("Truncated"));
        boolean hasTruncation = scan.problems().stream()
                .anyMatch(p -> p.startsWith("Truncated"));
        boolean valid;
        if (scan.intact() && scan.problems().isEmpty()) {
            valid = true;
        } else if (!validateStrict && !hasCorruption && hasTruncation) {
            valid = true; // recoverable interrupted write, no corruption
        } else {
            valid = false;
        }

        long payload = 0;
        for (ScannedBlock b : scan.blocks()) {
            payload += b.payloadSize();
        }
        return new IntegrityResult(valid, avroFile, scan.markers(),
                scan.blocks().size(), payload, scan.truncationOffset(), scan.problems());
    }

    // ─── Crash recovery ─────────────────────────────────────────────

    /**
     * Recovers the given Avro file to the end of its last fully-valid block.
     *
     * <p>The scan always runs in lenient mode (a crashed tail never raises).
     * When a block is incomplete, its sync marker is missing or corrupt, the
     * file is truncated to the end of the previous fully-valid block
     * ({@code syncMarkerOffset + 16}). With {@code avro.syncmarker.recovery.backup}
     * enabled (default) the original tail bytes are copied to {@code <file>.bak}
     * before truncation. When the file is already intact the file is left
     * untouched.
     *
     * @param avroFile the Avro object-container file
     * @return the recovery outcome
     * @throws IOException if the file is missing, its header cannot be parsed,
     *                     the backup cannot be written, or the truncation fails
     */
    public RecoveryResult recoverToLastValidBlock(File avroFile) throws IOException {
        HeaderProbe probe = readHeader(avroFile);
        CoreScan scan = coreScan(avroFile, probe.sync(), probe.headerEnd(), false);

        long records = 0;
        for (ScannedBlock b : scan.blocks()) {
            records += b.recordCount();
        }
        List<String> warnings = new ArrayList<>(scan.problems());

        if (scan.intact()) {
            return new RecoveryResult(avroFile, false, scan.blocks().size(), records,
                    probe.sync(), -1, false, warnings);
        }

        long truncatedAt = scan.truncationOffset();
        boolean backupCreated = false;
        if (createBackup) {
            File bak = new File(avroFile.getParentFile(), avroFile.getName() + ".bak");
            Files.copy(avroFile.toPath(), bak.toPath(), StandardCopyOption.REPLACE_EXISTING);
            backupCreated = true;
            LOGGER.info("Avro recovery: backed up {} to {} before truncation",
                    avroFile.getName(), bak.getName());
        }
        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "rw");
             FileChannel ch = raf.getChannel()) {
            ch.truncate(truncatedAt);
            ch.force(true);
        }
        LOGGER.warn("Avro recovery: truncated {} at byte {} ({}, {} blocks, {} records preserved)",
                avroFile.getName(), truncatedAt, scan.blocks().size(), scan.blocks().size(), records);
        return new RecoveryResult(avroFile, true, scan.blocks().size(), records,
                probe.sync(), truncatedAt, backupCreated, warnings);
    }

    // ─── Marker scanning ────────────────────────────────────────────

    /**
     * Enumerates every sync marker in the file in file order: the header
     * marker ({@code blockIndex == -1}), each data block's trailing marker
     * ({@code blockIndex == i}), and a terminal flush marker
     * ({@code blockIndex == -1}) when present.
     *
     * @param avroFile the Avro object-container file
     * @return an immutable list of marker occurrences
     * @throws IOException if the file is missing, its header cannot be parsed,
     *                     or (strict mode) the scan hits an incomplete block
     */
    public List<SyncMarkerInfo> scanAllMarkers(File avroFile) throws IOException {
        HeaderProbe probe = readHeader(avroFile);
        CoreScan scan = coreScan(avroFile, probe.sync(), probe.headerEnd(), validateStrict);
        return Collections.unmodifiableList(new ArrayList<>(scan.markers()));
    }

    /**
     * Counts the data blocks whose payload and trailing sync marker are fully
     * present in the file. The header-only case (zero rows) yields zero.
     *
     * @param avroFile the Avro object-container file
     * @return the number of fully-valid data blocks
     * @throws IOException if the file is missing, its header cannot be parsed,
     *                     or (strict mode) the scan hits an incomplete block
     */
    public int countBlocks(File avroFile) throws IOException {
        HeaderProbe probe = readHeader(avroFile);
        CoreScan scan = coreScan(avroFile, probe.sync(), probe.headerEnd(), validateStrict);
        return scan.blocks().size();
    }

    /**
     * Returns whether the 16 bytes at {@code offset} match the file header's
     * sync marker.
     *
     * @param avroFile the Avro object-container file
     * @param offset   file offset of the marker's first byte
     * @return {@code true} when the bytes match
     * @throws IOException if the file is missing, its header cannot be parsed,
     *                     or the offset points past the end of the file
     */
    public static boolean isSyncMarkerValid(File avroFile, long offset) throws IOException {
        HeaderProbe probe = readHeader(avroFile);
        byte[] actual = new byte[SYNC_SIZE];
        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "r");
             FileChannel ch = raf.getChannel()) {
            readFully(ch, offset, actual, raf.length(), avroFile);
        }
        return Arrays.equals(probe.sync(), actual);
    }

    // ─── Internal scan core ─────────────────────────────────────────

    /** Header anchor: the header's sync marker and the offset of the first block. */
    private record HeaderProbe(byte[] sync, long headerEnd) {
    }

    /** A data block whose payload and trailing sync marker were fully read. */
    private record ScannedBlock(int blockIndex, long headerPos, long payloadStart,
                                long payloadSize, long recordCount, long syncMarkerOffset) {
    }

    /** Internal scan outcome shared by validation, recovery and scanning. */
    private record CoreScan(List<SyncMarkerInfo> markers, List<ScannedBlock> blocks,
                            List<String> problems, boolean intact, long truncationOffset) {
    }

    /**
     * Outcome of scanning a single block inside {@link #coreScan}.
     *
     * @param ok          {@code true} when a valid block was found and appended
     * @param stop        {@code true} when the scan loop should terminate
     * @param problem     non-null when the block was corrupt / truncated
     * @param marker      the trailing sync marker (only set when a flush tail was read)
     * @param block       the scanned block (only set when {@code ok})
     * @param nextPos     the file offset to continue scanning from
     */
    private record BlockScanResult(boolean ok, boolean stop, String problem,
                                   SyncMarkerInfo marker, ScannedBlock block,
                                   long nextPos) {
        static BlockScanResult flush(SyncMarkerInfo marker, long nextPos) {
            return new BlockScanResult(false, true, null, marker, null, nextPos);
        }
        static BlockScanResult problem(String msg, long nextPos) {
            return new BlockScanResult(false, true, msg, null, null, nextPos);
        }
        static BlockScanResult block(ScannedBlock block, long nextPos) {
            return new BlockScanResult(true, false, null, null, block, nextPos);
        }
    }

    /**
     * Walks the raw file bytes after the header. In strict mode structural
     * corruption and truncated blocks raise {@link IOException}; in lenient
     * mode they are recorded as problems and the scan stops at the first one,
     * keeping everything up to the last fully-valid block.
     */
    private CoreScan coreScan(File avroFile, byte[] sync, long headerEnd, boolean strict)
            throws IOException {
        List<SyncMarkerInfo> markers = new ArrayList<>();
        List<ScannedBlock> blocks = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        markers.add(new SyncMarkerInfo(sync.clone(), headerEnd - SYNC_SIZE, -1));

        long lastValidEnd = headerEnd;
        boolean intact = true;

        try (RandomAccessFile raf = new RandomAccessFile(avroFile, "r");
             FileChannel ch = raf.getChannel()) {
            long fileLen = raf.length();
            long pos = headerEnd;
            int index = 0;
            while (pos < fileLen) {
                BlockScanResult result = scanNextBlock(ch, pos, fileLen, sync, avroFile, index, strict);
                if (result.stop()) {
                    if (result.problem() != null) {
                        intact = false;
                        problems.add(result.problem());
                    }
                    if (result.marker() != null) {
                        markers.add(result.marker());
                        lastValidEnd = result.nextPos();
                    }
                    break;
                }
                markers.add(new SyncMarkerInfo(sync.clone(), result.block().syncMarkerOffset(), index));
                blocks.add(result.block());
                lastValidEnd = result.block().syncMarkerOffset() + SYNC_SIZE;
                index++;
                pos = lastValidEnd;
            }
        }
        return new CoreScan(markers, blocks, problems, intact, intact ? -1 : lastValidEnd);
    }

    /**
     * Attempts to read and validate the next Avro block at the given file offset.
     * Returns a {@link BlockScanResult} that tells the caller whether a valid
     * block was found, the scan should stop, or an error occurred.
     *
     * <p>This method replaces the eight {@code break} statements that were
     * previously inlined in the {@link #coreScan} while-loop, reducing the
     * loop's cognitive complexity from ~22 to ~8.
     */
    private BlockScanResult scanNextBlock(FileChannel ch, long pos, long fileLen,
                                          byte[] sync, File avroFile, int index,
                                          boolean strict) throws IOException {
        // ── trailing flush marker (exactly SYNC_SIZE bytes left) ──
        if (fileLen - pos == SYNC_SIZE) {
            byte[] tail = new byte[SYNC_SIZE];
            try {
                readFully(ch, pos, tail, fileLen, avroFile);
            } catch (IOException e) {
                return BlockScanResult.problem(
                        "Truncated trailing sync marker at " + pos + ": " + e.getMessage(), pos);
            }
            if (Arrays.equals(tail, sync)) {
                return BlockScanResult.flush(
                        new SyncMarkerInfo(sync.clone(), pos, -1), pos + SYNC_SIZE);
            }
            return BlockScanResult.problem(
                    "Trailing flush sync marker mismatch at offset " + pos, pos);
        }

        // ── block count varint ──
        long headerPos = pos;
        long count;
        try {
            long[] cv = readZigzagVlq(ch, pos);
            count = cv[0];
            pos = cv[1];
        } catch (IOException e) {
            if (strict) {
                throw new IOException("Malformed Avro block count at " + pos + " in "
                        + avroFile + ": " + e.getMessage(), e);
            }
            return BlockScanResult.problem(
                    "Truncated Avro block count at " + pos + ": " + e.getMessage(), pos);
        }

        // ── block size varint ──
        long size;
        try {
            long[] sv = readZigzagVlq(ch, pos);
            size = sv[0];
            pos = sv[1];
        } catch (IOException e) {
            if (strict) {
                throw new IOException("Malformed Avro block size at " + pos + " in "
                        + avroFile + ": " + e.getMessage(), e);
            }
            return BlockScanResult.problem(
                    "Truncated Avro block size at " + pos + ": " + e.getMessage(), pos);
        }

        // ── validate header values ──
        if (count < 0 || size < 0 || size > Integer.MAX_VALUE) {
            IOException problem = new IOException(AvroFileConstants.MSG_INVALID_BLOCK_HEADER + count
                    + AvroFileConstants.MSG_SIZE_FIELD + size + ") at offset " + headerPos + " in " + avroFile);
            if (strict) {
                throw problem;
            }
            return BlockScanResult.problem(
                    AvroFileConstants.MSG_INVALID_BLOCK_HEADER + count + AvroFileConstants.MSG_SIZE_FIELD + size
                            + ") at offset " + headerPos, pos);
        }

        // ── payload + trailing sync marker ──
        long payloadStart = pos;
        long syncPos = payloadStart + size;
        if (syncPos + SYNC_SIZE > fileLen) {
            IOException truncated = new IOException(AvroFileConstants.MSG_TRUNCATED_BLOCK + headerPos
                    + " in " + avroFile + AvroFileConstants.MSG_EXPECTED_SYNC_MARKER + syncPos
                    + AvroFileConstants.MSG_FILE_ENDS_AT + fileLen);
            if (strict) {
                throw truncated;
            }
            return BlockScanResult.problem(
                    AvroFileConstants.MSG_TRUNCATED_BLOCK + headerPos + AvroFileConstants.MSG_EXPECTED_SYNC_MARKER
                            + syncPos + AvroFileConstants.MSG_FILE_ENDS_AT + fileLen, pos);
        }

        byte[] actual = new byte[SYNC_SIZE];
        try {
            readFully(ch, syncPos, actual, fileLen, avroFile);
        } catch (IOException e) {
            if (strict) {
                throw new IOException("Truncated Avro block sync marker at " + syncPos
                        + " in " + avroFile + ": " + e.getMessage(), e);
            }
            return BlockScanResult.problem(
                    "Truncated Avro block sync marker at " + syncPos + ": " + e.getMessage(), pos);
        }
        if (!Arrays.equals(actual, sync)) {
            return BlockScanResult.problem(
                    AvroFileConstants.MSG_SYNC_MARKER_MISMATCH + syncPos
                            + " in " + avroFile + " (corrupt file or interrupted write)", pos);
        }

        return BlockScanResult.block(
                new ScannedBlock(index, headerPos, payloadStart, size, count, syncPos),
                syncPos + SYNC_SIZE);
    }

    /**
     * Extracts the header's sync marker and the offset where the first data
     * block begins by probing with {@link AvroDataFileReader}.
     */
    private static HeaderProbe readHeader(File avroFile) throws IOException {
        if (avroFile == null || !avroFile.isFile()) {
            throw new IOException(AvroFileConstants.MSG_FILE_NOT_FOUND + avroFile);
        }
        try (AvroDataFileReader probe = new AvroDataFileReader(avroFile)) {
            return new HeaderProbe(probe.getSyncMarker(), probe.getPosition());
        }
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

    // ─── Raw I/O helpers ────────────────────────────────────────────

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

    @Override
    public String toString() {
        return "AvroSyncMarkerManager{validateStrict=" + validateStrict
                + ", createBackup=" + createBackup + '}';
    }
}