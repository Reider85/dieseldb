package diesel;

import diesel.storage.avro.AvroCrashDetector;
import diesel.storage.avro.AvroCrashDetector.CrashDetectionReport;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroIntegrityChecker;
import diesel.storage.avro.AvroIntegrityChecker.IntegrityReport;
import diesel.storage.avro.AvroReadIterator;
import diesel.storage.avro.AvroRecoveryManager;
import diesel.storage.avro.AvroRecoveryManager.RecoveryReport;
import diesel.storage.avro.AvroSyncMarkerManager;
import diesel.storage.avro.AvroSyncMarkerManager.IntegrityResult;
import diesel.storage.avro.AvroSyncMarkerManager.SyncMarkerInfo;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 93 AVRO recovery integration tests: runs the full end-to-end crash
 * recovery pipeline — {@link AvroCrashDetector} discovery, {@link
 * AvroSyncMarkerManager} block validation, {@link AvroIntegrityChecker} block
 * verification and {@link AvroRecoveryManager} orchestration — against mixed
 * crash scenarios in a single data directory, then asserts the directory is
 * clean and every surviving file passes a post-recovery integrity scan.
 */
@Tag("storage")
@StorageType("avro")
class AvroRecoveryTest {

    private static final String[] PROP_KEYS = {
            "avro.recovery.detect.on.startup",
            "avro.recovery.cleanup.temps",
            "avro.recovery.config.file",
            "avro.syncmarker.recovery.backup",
            "avro.block.sync.interval",
            AvroIntegrityChecker.STORE_SIDECAR_KEY,
            AvroIntegrityChecker.CHECK_ON_OPEN_KEY,
            AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY
    };

    @TempDir
    Path tempDir;

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveProps() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreProps() {
        for (String key : PROP_KEYS) {
            String value = prevProps.get(key);
            if (value != null) {
                System.setProperty(key, value);
            } else {
                System.clearProperty(key);
            }
        }
    }

    // ─── Test data helpers ─────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User-" + id);
        m.put("AGE", (int) (id % 100));
        return m;
    }

    /** Writes a multi-block file (small sync interval) and returns it. */
    private File writeMultiBlock(String name, long rows) throws IOException {
        System.setProperty("avro.block.sync.interval", "256");
        File f = new File(tempDir.toFile(), name);
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (long i = 0; i < rows; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private static Map<String, Class<?>> types(Schema schema) {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        for (Schema.Field f : schema.getFields()) {
            m.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
        }
        return m;
    }

    private static long countRows(File f) throws IOException {
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols(), types(reader.getSchema()))) {
            long n = 0;
            while (it.hasNext()) {
                it.next();
                n++;
            }
            return n;
        }
    }

    /** Flips bit 0 of the byte at {@code offset}. */
    private static void corruptByte(File f, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 1);
        }
    }

    private static void truncate(File f, long length) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(length);
        }
    }

    private void writeText(File f, String text) throws IOException {
        try (OutputStream out = new FileOutputStream(f)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    private long lastBlockSyncOffset(File f) throws IOException {
        return AvroSyncMarkerManager.resolve().scanAllMarkers(f).stream()
                .filter(m -> m.blockIndex() >= 0)
                .reduce((a, b) -> b)
                .map(SyncMarkerInfo::fileOffset)
                .orElseThrow();
    }

    /**
     * The offset just past the last intact block boundary (what recovery should
     * keep). Uses a lenient integrity scan so it also works on the crashed
     * (truncated) file before recovery runs.
     */
    private long lastIntactBoundary(File f) throws IOException {
        AvroSyncMarkerManager lenient = new AvroSyncMarkerManager(false, true);
        IntegrityResult r = lenient.validateIntegrity(f);
        return r.truncationOffset();
    }

    /** Crashes the last block by truncating 3 bytes past the final sync marker. */
    private void crashLastBlock(File f) throws IOException {
        truncate(f, lastBlockSyncOffset(f) + 3);
    }

    // ─── Full pipeline: detect → recover → verify ─────────────────

    @Test
    void fullPipelineRecoversTruncatedFileCleanly() throws IOException {
        File f = writeMultiBlock("users.avro", 2000);
        crashLastBlock(f);
        long expectedEnd = lastIntactBoundary(f);

        // 1. Detect: the crash must be flagged
        CrashDetectionReport report = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertTrue(report.hasIssues());
        assertEquals(1, report.corruptedFiles().size());
        assertTrue(report.corruptedFiles().get(0).needsRecovery());

        // 2. Recover
        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertEquals(1, rr.filesRecovered());

        // 3. Verify: file ends at the last intact block boundary
        assertEquals(expectedEnd, f.length(), "file must end at the last consistent block boundary");

        // 4. Verify: deep-row scan sees only the surviving blocks
        IntegrityResult after = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        assertTrue(after.valid(), "recovered file must validate: " + after.errors());
        assertTrue(countRows(f) > 0, "surviving records are preserved");
        assertTrue(countRows(f) < 2000, "crashed block rows are dropped");

        // 5. Verify: no file is flagged as corrupted anymore
        CrashDetectionReport clean = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertTrue(clean.corruptedFiles().isEmpty(),
                "recovered file must no longer be flagged corrupted: " + clean.corruptedFiles());
        assertFalse(clean.hasIssues());
    }

    @Test
    void fullPipelineLeavesHealthyFileUntouched() throws IOException {
        File f = writeMultiBlock("healthy.avro", 300);

        long before = f.length();
        CrashDetectionReport report = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertFalse(report.hasIssues());
        assertEquals(0, report.issueCount());

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertEquals(0, rr.filesRecovered());
        assertEquals(before, f.length(), "healthy file must be untouched");
    }

    @Test
    void postRecoveryIntegrityCheckerPasses() throws IOException {
        File f = writeMultiBlock("users.avro", 1500);
        crashLastBlock(f);

        AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());

        AvroIntegrityChecker checker = AvroIntegrityChecker.resolve();
        IntegrityReport report = checker.validateFile(f);
        assertTrue(report.fullyValid(),
                "recovered file must pass the integrity checker: " + report.problems());
        assertTrue(report.blocksChecked() >= 1);
        assertEquals(0, report.blocksCorrupted());
    }

    @Test
    void recoveringWithBackupThenIntegrityCheckOfBackup() throws IOException {
        File f = writeMultiBlock("users.avro", 800);
        crashLastBlock(f);
        long crashedLen = f.length();

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertEquals(1, rr.filesRecovered());

        // The backup must hold the crashed bytes and be independently readable
        File bak = new File(tempDir.toFile(), "users.avro.bak");
        assertTrue(bak.isFile(), "backup must exist after truncation recovery");
        assertEquals(crashedLen, bak.length(), "backup captures the crashed tail length");
        assertTrue(f.length() < bak.length());
    }

    // ─── Multi-file and mixed crash scenarios ──────────────────────

    @Test
    void multiFilePipelineRecoversIndependentTables() throws IOException {
        // three independent table files, each with its own crash
        File healthy = writeMultiBlock("a.avro", 100);
        File truncated = writeMultiBlock("b.avro", 2000);
        crashLastBlock(truncated);
        File corrMarker = writeMultiBlock("c.avro", 2000);
        List<SyncMarkerInfo> markers = AvroSyncMarkerManager.resolve().scanAllMarkers(corrMarker);
        long corruptOffset = markers.stream()
                .filter(m -> m.blockIndex() == 1)
                .map(SyncMarkerInfo::fileOffset)
                .findFirst().orElseThrow();
        corruptByte(corrMarker, corruptOffset);

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());

        assertEquals(100, countRows(healthy), "healthy table untouched");
        assertTrue(countRows(truncated) > 0 && countRows(truncated) < 2000,
                "truncated table restored to its last intact state");
        assertTrue(countRows(corrMarker) > 0 && countRows(corrMarker) < 2000,
                "corrupted-marker table had the bad block dropped");

        // every surviving .avro file must pass a full integrity scan
        for (File f : new File[]{healthy, truncated, corrMarker}) {
            IntegrityReport ir = AvroIntegrityChecker.resolve().validateFile(f);
            assertTrue(ir.fullyValid(), f.getName() + " must be valid after recovery: " + ir.problems());
        }
    }

    @Test
    void garbageFileFailsButOthersStillRecover() throws IOException {
        File garbage = new File(tempDir.toFile(), "garbage.avro");
        writeText(garbage, "this is not an avro file at all");
        File truncated = writeMultiBlock("users.avro", 2000);
        crashLastBlock(truncated);

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(1, rr.filesFailed(), "unreadable file must be reported as failed");
        assertTrue(rr.allSuccessful() == false, "garbage input must surface in the report");
        assertEquals(1, rr.filesRecovered());

        // the garbage is untouched, the recoverable one is restored
        assertEquals(setupGarbageBytes(), fileText(garbage));
        assertTrue(countRows(truncated) > 0 && countRows(truncated) < 2000);
    }

    private String setupGarbageBytes() {
        return "this is not an avro file at all";
    }

    private String fileText(File f) throws IOException {
        return new String(java.nio.file.Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
    }

    @Test
    void orphanTempAndTruncatedFileTogether() throws IOException {
        writeText(new File(tempDir.toFile(), "orphan.avro.tmp"), "half-written");
        File orphanTarget = new File(tempDir.toFile(), "orphan.avro");
        File users = writeMultiBlock("users.avro", 1000);
        crashLastBlock(users);

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertTrue(!new File(tempDir.toFile(), "orphan.avro.tmp").exists(),
                "unusable temp must be discarded");
        assertFalse(orphanTarget.exists(), "no target fabricated from garbage");
        assertTrue(countRows(users) > 0 && countRows(users) < 1000);
    }

    // ─── Recovery idempotency ──────────────────────────────────────

    @Test
    void doubleRecoveryIsIdempotent() throws IOException {
        File f = writeMultiBlock("users.avro", 1200);
        crashLastBlock(f);
        long expectedEnd = lastIntactBoundary(f);

        RecoveryReport first = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(first.allSuccessful());
        assertEquals(1, first.filesRecovered());

        long lengthAfterFirst = f.length();
        assertEquals(expectedEnd, lengthAfterFirst);

        RecoveryReport second = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(second.allSuccessful());
        assertEquals(0, second.filesRecovered(), "a clean directory must not be re-recovered");
        assertEquals(lengthAfterFirst, f.length(), "second pass must not rewrite the file");
        assertEquals(countRows(f), countRows(f), "row count stable across passes");
    }

    @Test
    void emptyDirectoryRecoveryIsNoOp() throws IOException {
        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertEquals(0, rr.filesRecovered());
        assertEquals(0, rr.totalFilesScanned());
    }

    // ─── Row-level preservation ────────────────────────────────────

    @Test
    void recoveredRowsMatchSurvivingBlocksExactly() throws IOException {
        File f = writeMultiBlock("users.avro", 500);
        crashLastBlock(f);
        long expectedEnd = lastIntactBoundary(f);

        AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(expectedEnd, f.length());

        long rows = countRows(f);
        assertTrue(rows > 0);
        // row ID i is stored as (long) i; the first surviving rows keep their identity
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols(), types(reader.getSchema()))) {
            assertTrue(it.hasNext());
            Object[] first = it.next();
            assertEquals(0L, first[0], "first row must be row 0");
            assertEquals("User-0", first[1]);
        }
    }

    // ─── Codec-compressed file recovery ────────────────────────────

    @Test
    void compressedFileRecoveryPreservesBlocks() throws IOException {
        System.setProperty("avro.compression.codec", "deflate");
        System.setProperty("avro.block.sync.interval", "256");
        File f = new File(tempDir.toFile(), "compressed.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (int i = 0; i < 2000; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        crashLastBlock(f);

        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(rr.allSuccessful());
        assertEquals(1, rr.filesRecovered());

        long rows = countRows(f);
        assertTrue(rows > 0 && rows < 2000, "deflate-compressed recovery keeps surviving blocks");
        IntegrityReport ir = AvroIntegrityChecker.resolve().validateFile(f);
        assertTrue(ir.fullyValid(), "compressed recovered file must pass integrity: " + ir.problems());
    }

    // ─── Large dataset stress recovery ─────────────────────────────

    @LargeTest
    void largeFileRecoveryUnderLimit() throws IOException {
        File f = writeMultiBlock("big.avro", 50_000);
        crashLastBlock(f);
        long expectedEnd = lastIntactBoundary(f);

        long start = System.nanoTime();
        RecoveryReport rr = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        long takenMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(rr.allSuccessful());
        assertEquals(1, rr.filesRecovered());
        assertEquals(expectedEnd, f.length());
        assertTrue(countRows(f) > 0);
        assertTrue(takenMs < 120_000, "50k-row recovery must stay under 120s (got " + takenMs + "ms)");
    }

    // ─── Concurrent recovery locking ───────────────────────────────

    @LargeTest
    void concurrentRecoverAllOnSameDirectory()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        for (int t = 0; t < 4; t++) {
            File f = writeMultiBlock("table" + t + ".avro", 3000);
            crashLastBlock(f);
        }

        int workers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Callable<RecoveryReport>> tasks = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                tasks.add(() -> AvroRecoveryManager.resolve().recoverAll(tempDir.toFile()));
            }
            List<Future<RecoveryReport>> futures = pool.invokeAll(tasks);
            for (Future<RecoveryReport> future : futures) {
                RecoveryReport rr = future.get();
                assertNotNull(rr);
            }
        } finally {
            pool.shutdownNow();
        }

        // after all concurrent passes: every table is at a valid block boundary
        for (int t = 0; t < 4; t++) {
            File f = new File(tempDir.toFile(), "table" + t + ".avro");
            IntegrityResult r = AvroSyncMarkerManager.resolve().validateIntegrity(f);
            assertTrue(r.valid(), "table" + t + " must be valid after concurrent recovery: " + r.errors());
            assertTrue(countRows(f) > 0);
        }
    }

    // ─── Startup recovery round-trip ───────────────────────────────

    @Test
    void recoveryOnStartupThenRuntimeRecoveryConsistent() throws IOException {
        File f = writeMultiBlock("users.avro", 2000);
        crashLastBlock(f);

        RecoveryReport startup = AvroRecoveryManager.resolve().recoverOnStartup(tempDir.toFile());
        assertTrue(startup.allSuccessful());
        assertEquals(1, startup.filesRecovered());
        assertTrue(countRows(f) > 0);

        // a second startup pass is a no-op
        RecoveryReport startupAgain = AvroRecoveryManager.resolve().recoverOnStartup(tempDir.toFile());
        assertTrue(startupAgain.allSuccessful());
        assertEquals(0, startupAgain.filesRecovered());
    }

    // ─── Sync marker bridge through recovery ───────────────────────

    @Test
    void syncMarkerScanAfterRecoveryListsOnlyIntactBlocks() throws IOException {
        File f = writeMultiBlock("users.avro", 900);
        int totalBlocks = AvroSyncMarkerManager.resolve().countBlocks(f);
        crashLastBlock(f);

        AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());

        List<SyncMarkerInfo> after = AvroSyncMarkerManager.resolve().scanAllMarkers(f);
        int blockMarkers = (int) after.stream().filter(m -> m.blockIndex() >= 0).count();
        assertEquals(totalBlocks - 1, blockMarkers, "recovery drops exactly the crashed block");
    }
}