package diesel.storage.avro;

import diesel.storage.avro.AvroCrashDetector.BakFile;
import diesel.storage.avro.AvroCrashDetector.CorruptedFile;
import diesel.storage.avro.AvroCrashDetector.CrashDetectionReport;
import diesel.storage.avro.AvroCrashDetector.EmptyFile;
import diesel.storage.avro.AvroCrashDetector.TempArtifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * AVRO crash recovery orchestration (Prompt 82).
 *
 * <p>Sits on top of {@link AvroCrashDetector} (which finds the crash artifacts)
 * and {@link AvroSyncMarkerManager} (Prompt 77, which repairs one file to its
 * last consistent block):
 * <ul>
 *   <li><b>Rollback of unfinished transactions</b> <i>(task 2)</i> — an orphaned
 *       {@code <target>.tmp} is the atomic-write window of an in-flight save.
 *       When it already holds a complete, valid Avro data file the transaction
 *       is <b>rolled forward</b>: the temp is promoted (renamed) over the
 *       target, so the interrupted save commits. When it is unusable garbage
 *       the incomplete transaction is <b>rolled back</b>: the temp is discarded
 *       (governed by {@code avro.recovery.cleanup.temps}) and the previous
 *       target, if any, stays untouched.</li>
 *   <li><b>Restoration to the last consistent state</b> <i>(task 3)</i> — a
 *       corrupt/truncated {@code .avro} file is passed to
 *       {@link AvroSyncMarkerManager#recoverToLastValidBlock(File)}, which
 *       truncates the file to the end of the last block whose sync marker is
 *       intact, preserving only fully-consistent data. A file whose header is
 *       unreadable has no recoverable consistent point and is reported as
 *       failed without being touched.</li>
 *   <li><b>Detection of incomplete blocks by sync markers</b> <i>(task 1)</i> —
 *       delegated to {@link AvroSyncMarkerManager#validateIntegrity(File)} in
 *       lenient mode through {@link AvroCrashDetector#detectCrash(File)}.</li>
 *   <li><b>Recovery logging</b> <i>(task 4)</i> — every promoted, discarded,
 *       truncated, skipped or failed file is logged, and a final summary is
 *       emitted with the counts and total recovery time.</li>
 * </ul>
 *
 * <p>{@code .bak} backups are never touched automatically: they are the safety
 * net of a previous recovery and stay for manual inspection.
 *
 * <p>Configuration is resolved per call from a system property, then the root
 * {@code config.properties}, then the code defaults, via
 * {@link AvroCrashDetector#resolve()}. Use {@link #recoverOnStartup(File)} as
 * the storage-startup integration hook: it is a no-op when
 * {@code avro.recovery.detect.on.startup = off}.
 *
 * <p>The class is thread-safe: it keeps no mutable state.
 *
 * @since Prompt 82
 */
public final class AvroRecoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecoveryManager.class);

    private final AvroCrashDetector detector;
    private final AvroSyncMarkerManager syncMarkerManager;

    /**
     * The action applied to a single file during recovery.
     */
    public enum RecoveryAction {
        /** The file was intact and required no action. */
        NONE,
        /** A truncated/corrupt file was truncated to its last consistent block. */
        TRUNCATED,
        /** A complete orphaned {@code .tmp} was renamed over its target. */
        PROMOTED_TMP,
        /** An unusable orphaned {@code .tmp} was removed (transaction rollback). */
        DISCARDED_TMP,
        /** Recovery truncated the file and wrote a {@code .bak} backup first. */
        BACKUP_CREATED,
        /** Recovery could not repair the file (header unreadable or IO error). */
        FAILED
    }

    /**
     * Outcome for a single file.
     *
     * @param file              the file that was inspected
     * @param action            the recovery action applied
     * @param success           whether recovery considered the file handled
     * @param message           human-readable detail
     * @param recoveryTimeNanos wall-clock duration for this file
     */
    public record FileRecovery(File file, RecoveryAction action, boolean success,
                               String message, long recoveryTimeNanos) {
    }

    /**
     * Outcome of a whole-directory recovery pass.
     *
     * @param dataDir            the scanned directory
     * @param recoveries         per-file results in file order
     * @param totalFilesScanned  artifacts inspected (temps + corrupt + empty + backups)
     * @param filesRecovered     files that were acted on (truncated/promoted/discarded)
     * @param filesSkipped       intact files left untouched
     * @param filesFailed        files that could not be repaired
     * @param startedAt          when the pass started
     * @param completedAt        when the pass finished
     * @param totalRecoveryTimeNanos wall-clock duration of the whole pass
     */
    public record RecoveryReport(File dataDir,
                                 List<FileRecovery> recoveries,
                                 int totalFilesScanned,
                                 int filesRecovered,
                                 int filesSkipped,
                                 int filesFailed,
                                 Instant startedAt,
                                 Instant completedAt,
                                 long totalRecoveryTimeNanos) {

        /** Whether every inspected file was handled successfully. */
        public boolean allSuccessful() {
            return filesFailed == 0;
        }
    }

    /**
     * Creates a recovery manager with explicit behaviour flags.
     *
     * @param detectOnStartup whether automatic startup detection is enabled
     * @param cleanupTemps    whether orphaned temps are removed after inspection
     */
    public AvroRecoveryManager(boolean detectOnStartup, boolean cleanupTemps) {
        this.detector = new AvroCrashDetector(detectOnStartup, cleanupTemps);
        this.syncMarkerManager = AvroSyncMarkerManager.resolve();
    }

    /**
     * Resolves the manager from system properties / {@code config.properties}
     * via {@link AvroCrashDetector#resolve()}.
     *
     * @return a configured recovery manager (never {@code null})
     */
    public static AvroRecoveryManager resolve() {
        AvroCrashDetector d = AvroCrashDetector.resolve();
        return new AvroRecoveryManager(d.detectOnStartup(), d.cleanupTemps());
    }

    /** The underlying detector (exposes the resolved flags). */
    public AvroCrashDetector detector() {
        return detector;
    }

    /**
     * Startup integration hook. Runs a full {@link #recoverAll(File)} pass when
     * {@code avro.recovery.detect.on.startup} is enabled, otherwise logs and
     * returns an empty report.
     *
     * @param dataDir the Avro data directory
     * @return the recovery report (empty when detection is disabled)
     * @throws IOException if detection fails
     */
    public RecoveryReport recoverOnStartup(File dataDir) throws IOException {
        if (!detector.detectOnStartup()) {
            LOGGER.info("Avro recovery: startup detection is disabled; skipped {}", dataDir.getPath());
            return new RecoveryReport(dataDir, List.of(), 0, 0, 0, 0,
                    Instant.now(), Instant.now(), 0);
        }
        return recoverAll(dataDir);
    }

    /**
     * Recovers every crashed table file in {@code dataDir}.
     *
     * @param dataDir the Avro data directory
     * @return the recovery report
     * @throws IOException if detection fails (integrity scan errors on a file)
     */
    public RecoveryReport recoverAll(File dataDir) throws IOException {
        Instant startedAt = Instant.now();
        long t0 = System.nanoTime();
        CrashDetectionReport detection = detector.detectCrash(dataDir);

        List<FileRecovery> recoveries = new ArrayList<>();

        for (TempArtifact tmp : detection.orphanedTemps()) {
            recoveries.add(recoverOrphanedTemp(tmp));
        }
        for (CorruptedFile cf : detection.corruptedFiles()) {
            recoveries.add(recoverCorruptedFile(cf));
        }
        for (EmptyFile ef : detection.emptyFiles()) {
            LOGGER.warn("Avro recovery: empty data file {} ignored (no consistent state to restore)",
                    ef.file().getPath());
            recoveries.add(new FileRecovery(ef.file(), RecoveryAction.NONE, true,
                    "empty file left in place", 0));
        }
        for (BakFile bak : detection.backupFiles()) {
            LOGGER.info("Avro recovery: backup {} present (original: {}); left untouched",
                    bak.file().getName(), bak.originalName());
        }

        int recovered = 0;
        int skipped = 0;
        int failed = 0;
        for (FileRecovery r : recoveries) {
            switch (r.action()) {
                case TRUNCATED, PROMOTED_TMP, DISCARDED_TMP, BACKUP_CREATED -> recovered++;
                case NONE -> skipped++;
                case FAILED -> failed++;
            }
        }

        long totalNanos = System.nanoTime() - t0;
        Instant completedAt = Instant.now();
        RecoveryReport report = new RecoveryReport(
                dataDir,
                Collections.unmodifiableList(recoveries),
                detection.orphanedTemps().size() + detection.corruptedFiles().size()
                        + detection.emptyFiles().size() + detection.backupFiles().size(),
                recovered, skipped, failed, startedAt, completedAt, totalNanos);

        LOGGER.info("Avro recovery: {} pass on {} — {} recovered, {} skipped, {} failed, {} ms",
                failed == 0 ? "clean" : "completed-with-failures",
                dataDir.getPath(), recovered, skipped, failed, totalNanos / 1_000_000);
        return report;
    }

    /**
     * Recovers a single crashed {@code .avro} file to its last consistent block.
     *
     * @param avroFile the Avro data file
     * @return the file-level recovery outcome
     */
    public FileRecovery recoverFile(File avroFile) {
        long t0 = System.nanoTime();
        CorruptedFile cf;
        try {
            List<CorruptedFile> found = detector.detectCrash(avroFile.getParentFile()).corruptedFiles()
                    .stream().filter(c -> c.file().equals(avroFile))
                    .toList();
            cf = found.isEmpty() ? null : found.get(0);
        } catch (IOException e) {
            return failure(avroFile, "detection failed: " + e.getMessage(), t0);
        }
        if (cf == null) {
            LOGGER.info("Avro recovery: {} is intact; nothing to do", avroFile.getPath());
            return new FileRecovery(avroFile, RecoveryAction.NONE, true,
                    "file is intact", System.nanoTime() - t0);
        }
        return recoverCorruptedFile(cf);
    }

    // ─── Per-artifact recovery steps ────────────────────────────────

    private FileRecovery recoverOrphanedTemp(TempArtifact tmp) {
        long t0 = System.nanoTime();
        File tmpFile = tmp.file();
        File target = new File(tmpFile.getParentFile(), tmp.targetName());
        try {
            AvroSyncMarkerManager.IntegrityResult integrity;
            boolean complete = false;
            try {
                integrity = syncMarkerManager.validateIntegrity(tmpFile);
                complete = integrity.valid()
                        && integrity.truncationOffset() < 0
                        && integrity.errors().isEmpty();
            } catch (IOException e) {
                LOGGER.warn("Avro recovery: temp {} is not a complete Avro file ({}); "
                        + "the interrupted write will be rolled back", tmpFile.getName(), e.getMessage());
                integrity = null;
            }
            if (complete) {
                moveTempOverTarget(tmpFile, target);
                LOGGER.info("Avro recovery: rolled forward interrupted write — promoted {} to {} ({} block(s))",
                        tmpFile.getName(), target.getName(), integrity.totalBlocks());
                return new FileRecovery(tmpFile, RecoveryAction.PROMOTED_TMP, true,
                        "promoted to " + target.getName(), System.nanoTime() - t0);
            }
            if (detector.cleanupTemps()) {
                Files.deleteIfExists(tmpFile.toPath());
                LOGGER.warn("Avro recovery: rolled back interrupted write — discarded unusable {} ({} bytes)",
                        tmpFile.getName(), tmp.size());
                return new FileRecovery(tmpFile, RecoveryAction.DISCARDED_TMP, true,
                        "unusable temp discarded", System.nanoTime() - t0);
            }
            LOGGER.warn("Avro recovery: incomplete temp {} left in place (cleanup disabled)", tmpFile.getName());
            return new FileRecovery(tmpFile, RecoveryAction.NONE, true,
                    "unusable temp left (cleanup disabled)", System.nanoTime() - t0);
        } catch (IOException e) {
            LOGGER.error("Avro recovery failed for temp {}: {}", tmpFile.getPath(), e.getMessage());
            return failure(tmpFile, e.getMessage(), t0);
        }
    }

    private static void moveTempOverTarget(File tmpFile, File target) throws IOException {
        try {
            Files.move(tmpFile.toPath(), target.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(tmpFile.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private FileRecovery recoverCorruptedFile(CorruptedFile cf) {
        long t0 = System.nanoTime();
        File file = cf.file();
        if (cf.integrityResult() == null) {
            LOGGER.error("Avro recovery: {} has an unreadable header — no consistent "
                    + "block can be located, file left untouched", file.getPath());
            return failure(file, "unreadable header, no recovery possible", t0);
        }
        if (!cf.needsRecovery()) {
            LOGGER.info("Avro recovery: {} validation reported {}; file left intact",
                    file.getName(), cf.integrityResult().errors());
            return new FileRecovery(file, RecoveryAction.NONE, true,
                    "no recovery needed", System.nanoTime() - t0);
        }
        try {
            AvroSyncMarkerManager.RecoveryResult rr =
                    syncMarkerManager.recoverToLastValidBlock(file);
            if (!rr.truncated()) {
                return new FileRecovery(file, RecoveryAction.NONE, true,
                        "integrity restored without truncation", System.nanoTime() - t0);
            }
            RecoveryAction action = rr.backupCreated()
                    ? RecoveryAction.BACKUP_CREATED
                    : RecoveryAction.TRUNCATED;
            String message = String.format("truncated to %,d bytes — %,d block(s), %,d record(s) preserved%s",
                    rr.truncatedAt(), rr.blocksRecovered(), rr.recordsRecovered(),
                    rr.backupCreated() ? ", backup written" : "");
            LOGGER.warn("Avro recovery: restored {} to last consistent state — {}", file.getName(), message);
            return new FileRecovery(file, action, true, message, System.nanoTime() - t0);
        } catch (IOException e) {
            LOGGER.error("Avro recovery failed for {}: {}", file.getPath(), e.getMessage());
            return failure(file, e.getMessage(), t0);
        }
    }

    private static FileRecovery failure(File file, String message, long startNanos) {
        return new FileRecovery(file, RecoveryAction.FAILED, false, message,
                System.nanoTime() - startNanos);
    }
}