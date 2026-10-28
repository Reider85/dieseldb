package diesel.backup;

import diesel.storage.page.Page;
import diesel.storage.page.PageHeader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Incremental physical backup (prompt4 #42, R3-011 step 3/4).
 *
 * <p>Copies only the state that changed since a base backup (a full backup or
 * a previous incremental):
 *
 * <ul>
 *   <li><b>pages.db delta</b> - the page file is scanned page-by-page; a page is
 *       copied into {@code pages.db.delta} when its CRC32 differs from the CRC
 *       recorded in the base backup's {@code backup_state.idx}. The page LSN is
 *       recorded in the state file as well; because the LSN field is part of the
 *       page bytes, the CRC comparison subsumes the "LSN &gt; baseLsn" rule
 *       (note: normal - non-WAL - writes keep LSN=0 in DieselDB, see
 *       BufferPoolFlusher's flush predicate, so an LSN-only filter would miss
 *       them - CRC is the authoritative change detector).</li>
 *   <li><b>Data-file delta</b> - top-level data files (same extension filter as
 *       {@link PhysicalBackup}) whose size/CRC changed, plus files deleted since
 *       the base (recorded in the manifest so the restore chain can remove them
 *       from the target).</li>
 * </ul>
 *
 * <p>Every incremental writes its own {@code backup_state.idx} (a cumulative
 * snapshot of file CRCs and page CRCs), which becomes the comparison base for
 * the next incremental in the chain. When the base backup has no state file
 * (a step-2 full backup predates this format), file comparison falls back to
 * the base manifest's size entries and every page counts as changed - the
 * first incremental then carries the full page set and seeds the state chain.
 *
 * <p>Manifest linkage: {@code backup_manifest.json} has {@code backupType}
 * "incremental" and a metadata map with {@code parentBackup} (the base backup
 * directory name), {@code baseLsn}, {@code maxLsn}, {@code changedPageCount},
 * {@code changedFileCount}, {@code deletedFiles} and {@code pageSize}.
 *
 * <p>Restore of a chain: {@link #restoreChain} applies the full backup through
 * {@link PhysicalRestore} and then every incremental in order (overwriting
 * changed files, deleting removed ones, patching pages.db at page offsets).
 */
public final class IncrementalBackup {

    /** Cumulative change-detection state written into every incremental backup. */
    public static final String STATE_FILE = "backup_state.idx";
    /** Page delta file: magic + pageSize + record count + (pageNum, image)*. */
    public static final String PAGE_DELTA_FILE = "pages.db.delta";
    /** Page file name inside the database directory (mirrors Database.createPageManager). */
    public static final String PAGE_FILE = "pages.db";
    /** Manifest file name (shared with the full-backup format). */
    public static final String MANIFEST_FILE = "backup_manifest.json";

    private static final int DELTA_MAGIC = 0x5044454C; // "PDEL"
    private static final int DELTA_HEADER_BYTES = 4 + 4 + 8; // magic + pageSize + recordCount
    private static final int DELTA_RECORD_HEADER_BYTES = 8;  // pageNum
    private static final long CRC_UNKNOWN = Long.MIN_VALUE;

    private final Path databaseDir;
    private final Path backupDir;
    private final Path baseBackupDir;

    /**
     * @param databaseDir   the live database directory (read-only)
     * @param backupDir     target directory for this incremental (must not contain a manifest yet)
     * @param baseBackupDir the full backup or previous incremental to diff against
     *                     ({@code null} = no base: everything counts as changed)
     */
    public IncrementalBackup(Path databaseDir, Path backupDir, Path baseBackupDir) {
        if (databaseDir == null || backupDir == null) {
            throw new IllegalArgumentException("databaseDir and backupDir must not be null");
        }
        this.databaseDir = databaseDir;
        this.backupDir = backupDir;
        this.baseBackupDir = baseBackupDir;
    }

    /**
     * Runs the incremental backup.
     *
     * @return the saved manifest (also written to {@code backup_manifest.json})
     * @throws IOException if the backup directory already holds a manifest, or a
     *                     copy/scan fails
     */
    public BackupManifest backup() throws IOException {
        if (Files.exists(backupDir.resolve(MANIFEST_FILE))) {
            throw new IOException("Backup directory already contains a manifest (refusing to overwrite): "
                    + backupDir);
        }
        Files.createDirectories(backupDir);

        BackupState baseState = loadBaseState();

        BackupManifest manifest = new BackupManifest();
        BackupState currentState = new BackupState();

        int changedFiles = copyChangedDataFiles(baseState, currentState, manifest);
        int changedPages = copyChangedPages(baseState, currentState, manifest);

        // Files the base knew about but the database no longer has.
        for (String name : baseState.files.keySet()) {
            if (!currentState.files.containsKey(name)) {
                currentState.deletedFiles.add(name);
            }
        }

        currentState.save(backupDir.resolve(STATE_FILE));

        manifest.setInfo(new BackupManifest.BackupInfo(
                System.currentTimeMillis(),
                databaseDir.toString(),
                manifest.getFileEntries().size(),
                manifest.getTotalSize(),
                "incremental"));
        manifest.getMetadata().put("parentBackup",
                baseBackupDir == null ? "" : baseBackupDir.getFileName().toString());
        manifest.getMetadata().put("baseLsn", String.valueOf(baseState.maxLsn));
        manifest.getMetadata().put("maxLsn", String.valueOf(currentState.maxLsn));
        manifest.getMetadata().put("changedPageCount", String.valueOf(changedPages));
        manifest.getMetadata().put("changedFileCount", String.valueOf(changedFiles));
        manifest.getMetadata().put("deletedFiles", String.join(",", currentState.deletedFiles));
        manifest.getMetadata().put("pageSize", String.valueOf(currentState.pageSize));
        manifest.save(backupDir.resolve(MANIFEST_FILE));
        return manifest;
    }

    // ------------------------------------------------------------------
    // Data-file delta
    // ------------------------------------------------------------------

    private int copyChangedDataFiles(BackupState base, BackupState current, BackupManifest manifest) throws IOException {
        int changed = 0;
        for (Map.Entry<String, Path> e : scanDataFiles().entrySet()) {
            String name = e.getKey();
            Path source = e.getValue();
            long size = Files.size(source);
            long crc = crcOfFile(source);
            current.files.put(name, new BackupState.FileMeta(size, crc));

            BackupState.FileMeta baseMeta = base.files.get(name);
            boolean differs = baseMeta == null
                    || (baseMeta.crc == CRC_UNKNOWN ? baseMeta.size != size : baseMeta.crc != crc);
            if (differs) {
                Files.copy(source, backupDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
                // Legacy checksum format keeps BackupManifest.verifyIntegrity working;
                // the strong per-file CRC lives in backup_state.idx.
                // Checksum string must match BackupManifest.calculateChecksum (size-based).
                manifest.addFile(name, size, "checksum-" + size);
                changed++;
            }
        }
        return changed;
    }

    /** Top-level data files with the same extension filter as {@link PhysicalBackup#isDataFile}. */
    private Map<String, Path> scanDataFiles() throws IOException {
        Map<String, Path> result = new TreeMap<>();
        if (!Files.isDirectory(databaseDir)) {
            return result;
        }
        try (var stream = Files.list(databaseDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(IncrementalBackup::isDataFile)
                    .forEach(p -> result.put(p.getFileName().toString(), p));
        }
        return result;
    }

    private static boolean isDataFile(Path file) {
        String name = file.getFileName().toString();
        return name.endsWith(".table")
                || name.endsWith(".csv")
                || name.endsWith(".tsv")
                || name.endsWith(".jsonl")
                || name.endsWith(".avro")
                || name.endsWith(".log")
                || name.endsWith(".wal");
    }

    // ------------------------------------------------------------------
    // Page delta
    // ------------------------------------------------------------------

    private int copyChangedPages(BackupState base, BackupState current, BackupManifest manifest) throws IOException {
        Path pageFile = databaseDir.resolve(PAGE_FILE);
        if (!Files.exists(pageFile) || Files.size(pageFile) == 0) {
            current.pageSize = base.pageSize;
            return 0;
        }

        int pageSize = readPageSize(pageFile);
        current.pageSize = pageSize;
        long fileSize = Files.size(pageFile);
        long pageCount = fileSize / pageSize;

        List<Long> changed = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(pageFile, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(pageSize);
            for (long pageNum = 0; pageNum < pageCount; pageNum++) {
                buf.clear();
                readFully(channel, buf, pageNum * pageSize);
                byte[] image = buf.array();
                long crc = crc32(image);
                long lsn = readPageLsn(image);
                current.pages.put(pageNum, crc);
                if (lsn > current.maxLsn) {
                    current.maxLsn = lsn;
                }
                Long baseCrc = base.pages.get(pageNum);
                if (baseCrc == null || baseCrc != crc) {
                    changed.add(pageNum);
                }
            }
            // A trailing partial page (torn extend, fileSize % pageSize != 0) is
            // not readable as a page and is skipped.
        }

        if (changed.isEmpty()) {
            return 0;
        }

        Path delta = backupDir.resolve(PAGE_DELTA_FILE);
        try (FileChannel in = FileChannel.open(pageFile, StandardOpenOption.READ);
             FileChannel out = FileChannel.open(delta,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(DELTA_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            header.putInt(DELTA_MAGIC);
            header.putInt(pageSize);
            header.putLong(changed.size());
            header.flip();
            writeFully(out, header, 0);

            ByteBuffer rec = ByteBuffer.allocate(DELTA_RECORD_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            ByteBuffer pageBuf = ByteBuffer.allocate(pageSize);
            long pos = DELTA_HEADER_BYTES;
            for (long pageNum : changed) {
                rec.clear();
                rec.putLong(pageNum);
                rec.flip();
                writeFully(out, rec, pos);
                pos += DELTA_RECORD_HEADER_BYTES;

                pageBuf.clear();
                // readFully leaves the buffer flipped (position=0, limit=pageSize).
                readFully(in, pageBuf, pageNum * pageSize);
                writeFully(out, pageBuf, pos);
                pos += pageSize;
            }
        }
        manifest.addFile(PAGE_DELTA_FILE, Files.size(delta), "checksum-" + Files.size(delta));
        return changed.size();
    }

    /** Reads the self-describing page size from the first page header. */
    private static int readPageSize(Path pageFile) throws IOException {
        try (FileChannel channel = FileChannel.open(pageFile, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(PageHeader.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, header, 0);
            byte[] h = header.array();
            int magic = ByteBuffer.wrap(h).order(ByteOrder.BIG_ENDIAN).getInt();
            if (magic == PageHeader.MAGIC) {
                int pageSize = ByteBuffer.wrap(h).order(ByteOrder.BIG_ENDIAN).getInt(56);
                if (Page.isAllowedSize(pageSize)) {
                    return pageSize;
                }
            }
        }
        // Zero-filled or foreign first page: fall back to the configured size.
        return diesel.storage.page.PageManager.PAGE_SIZE;
    }

    private static long readPageLsn(byte[] image) {
        if (image.length < PageHeader.HEADER_SIZE) {
            return 0L;
        }
        ByteBuffer bb = ByteBuffer.wrap(image).order(ByteOrder.BIG_ENDIAN);
        if (bb.getInt() != PageHeader.MAGIC) {
            return 0L; // zero-filled (allocated but never flushed) page
        }
        return bb.getLong(32);
    }

    // ------------------------------------------------------------------
    // Restore chain
    // ------------------------------------------------------------------

    /**
     * Restores {@code fullBackupDir} into {@code targetDir} and applies every
     * incremental backup in order.
     *
     * @param fullBackupDir   a step-2 full backup directory
     * @param incrementalDirs incrementals in chain order (oldest first)
     * @param targetDir       restore target (created if absent)
     * @throws IOException on restore/apply failure; an incremental whose manifest
     *                     is missing or not of type "incremental" is rejected
     */
    public static void restoreChain(Path fullBackupDir, List<Path> incrementalDirs, Path targetDir)
            throws IOException {
        new PhysicalRestore(fullBackupDir, targetDir).restore();
        for (Path inc : incrementalDirs) {
            applyIncremental(inc, targetDir);
        }
    }

    /** Applies one incremental backup on top of an already-restored target. */
    public static void applyIncremental(Path incrementalDir, Path targetDir) throws IOException {
        Path manifestPath = incrementalDir.resolve(MANIFEST_FILE);
        if (!Files.exists(manifestPath)) {
            throw new IOException("Incremental manifest not found: " + manifestPath);
        }
        BackupManifest manifest = BackupManifest.load(manifestPath);
        if (manifest.getInfo() != null && !"incremental".equals(manifest.getInfo().getBackupType())) {
            throw new IOException("Not an incremental backup: " + manifestPath
                    + " (type=" + manifest.getInfo().getBackupType() + ")");
        }

        Files.createDirectories(targetDir);

        // 1. Files deleted since the base.
        String deleted = manifest.getMetadata().getOrDefault("deletedFiles", "");
        if (!deleted.isEmpty()) {
            for (String name : deleted.split(",")) {
                if (!name.isBlank()) {
                    Files.deleteIfExists(targetDir.resolve(name.trim()));
                }
            }
        }

        // 2. Changed/new files (page delta handled separately below).
        for (BackupManifest.FileEntry entry : manifest.getFileEntries()) {
            if (PAGE_DELTA_FILE.equals(entry.getRelativePath())) {
                continue;
            }
            Path source = incrementalDir.resolve(entry.getRelativePath());
            if (!Files.exists(source)) {
                throw new IOException("Backup file listed in manifest is missing: " + source);
            }
            Files.copy(source, targetDir.resolve(entry.getRelativePath()),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        // 3. Page delta.
        Path delta = incrementalDir.resolve(PAGE_DELTA_FILE);
        if (Files.exists(delta)) {
            applyPageDelta(delta, targetDir.resolve(PAGE_FILE));
        }
    }

    /**
     * Patches the target page file: every delta record is written at
     * {@code pageNum * pageSize}, extending the file as needed. Applying a delta
     * is idempotent (page images are absolute, not relative).
     */
    private static void applyPageDelta(Path delta, Path pageFile) throws IOException {
        try (FileChannel in = FileChannel.open(delta, StandardOpenOption.READ)) {
            long deltaSize = in.size();
            ByteBuffer header = ByteBuffer.allocate(DELTA_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(in, header, 0);
            if (header.getInt(0) != DELTA_MAGIC) {
                throw new IOException("Bad page-delta magic in " + delta);
            }
            int pageSize = header.getInt(4);
            long recordCount = header.getLong(8);
            long expected = DELTA_HEADER_BYTES + recordCount * (DELTA_RECORD_HEADER_BYTES + pageSize);
            if (expected != deltaSize) {
                throw new IOException("Truncated page delta: expected " + expected
                        + " bytes, found " + deltaSize + " (" + delta + ")");
            }

            try (FileChannel out = FileChannel.open(pageFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                ByteBuffer rec = ByteBuffer.allocate(DELTA_RECORD_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize);
                long pos = DELTA_HEADER_BYTES;
                for (long i = 0; i < recordCount; i++) {
                    rec.clear();
                    readFully(in, rec, pos);
                    long pageNum = rec.getLong(0);
                    pos += DELTA_RECORD_HEADER_BYTES;

                    pageBuf.clear();
                    readFully(in, pageBuf, pos);
                    pos += pageSize;

                    writeFully(out, pageBuf, pageNum * pageSize);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Base state
    // ------------------------------------------------------------------

    private BackupState loadBaseState() throws IOException {
        if (baseBackupDir == null || !Files.isDirectory(baseBackupDir)) {
            return new BackupState();
        }
        Path stateFile = baseBackupDir.resolve(STATE_FILE);
        if (Files.exists(stateFile)) {
            return BackupState.load(stateFile);
        }
        // Base without state file (step-2 full backup): fall back to manifest sizes;
        // pages have no baseline, so every page counts as changed.
        BackupState state = new BackupState();
        Path manifestPath = baseBackupDir.resolve(MANIFEST_FILE);
        if (Files.exists(manifestPath)) {
            BackupManifest base = BackupManifest.load(manifestPath);
            for (BackupManifest.FileEntry e : base.getFileEntries()) {
                state.files.put(e.getRelativePath(),
                        new BackupState.FileMeta(e.getSize(), CRC_UNKNOWN));
            }
        }
        return state;
    }

    // ------------------------------------------------------------------
    // Byte helpers
    // ------------------------------------------------------------------

    private static long crcOfFile(Path file) throws IOException {
        CRC32 crc = new CRC32();
        byte[] chunk = new byte[64 * 1024];
        try (var in = Files.newInputStream(file)) {
            int n;
            while ((n = in.read(chunk)) > 0) {
                crc.update(chunk, 0, n);
            }
        }
        return crc.getValue();
    }

    private static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
    }

    private static void readFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new IOException("Unexpected EOF at offset " + pos
                        + " (wanted " + buf.remaining() + " more bytes)");
            }
            pos += n;
        }
        buf.flip();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.write(buf, pos);
            if (n < 0) {
                throw new IOException("Short write at offset " + pos);
            }
            pos += n;
        }
    }

    // ------------------------------------------------------------------
    // Cumulative state file
    // ------------------------------------------------------------------

    /**
     * Cumulative snapshot of the database as of one backup: per-file
     * (size, CRC32) and per-page CRC32, plus the max page LSN. Text format:
     * {@code pagesize N}, {@code maxlsn N}, {@code F name size crc},
     * {@code P pageNum crc}. Written by every incremental and consumed as the
     * comparison base by the next one.
     */
    static final class BackupState {
        static final class FileMeta {
            final long size;
            final long crc;

            FileMeta(long size, long crc) {
                this.size = size;
                this.crc = crc;
            }
        }

        final Map<String, FileMeta> files = new TreeMap<>();
        final Map<Long, Long> pages = new TreeMap<>();
        final List<String> deletedFiles = new ArrayList<>();
        int pageSize = diesel.storage.page.PageManager.PAGE_SIZE;
        long maxLsn = -1L;

        void save(Path path) throws IOException {
            try (Writer w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                w.write("# diesel incremental backup state v1\n");
                w.write("pagesize " + pageSize + "\n");
                w.write("maxlsn " + maxLsn + "\n");
                for (Map.Entry<String, FileMeta> e : files.entrySet()) {
                    w.write("F " + e.getKey() + " " + e.getValue().size + " "
                            + e.getValue().crc + "\n");
                }
                for (Map.Entry<Long, Long> e : pages.entrySet()) {
                    w.write("P " + e.getKey() + " " + e.getValue() + "\n");
                }
            }
        }

        static BackupState load(Path path) throws IOException {
            BackupState state = new BackupState();
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.isBlank() || line.startsWith("#")) {
                        continue;
                    }
                    String[] parts = line.split(" ", 4);
                    switch (parts[0]) {
                        case "pagesize" -> state.pageSize = Integer.parseInt(parts[1].trim());
                        case "maxlsn" -> state.maxLsn = Long.parseLong(parts[1].trim());
                        case "F" -> state.files.put(parts[1],
                                new BackupState.FileMeta(Long.parseLong(parts[2]), Long.parseLong(parts[3])));
                        case "P" -> state.pages.put(Long.parseLong(parts[1]), Long.parseLong(parts[2]));
                        default -> { /* forward compatibility: ignore unknown records */ }
                    }
                }
            }
            return state;
        }
    }
}
