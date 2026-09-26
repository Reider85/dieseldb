package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * AVRO crash detection (Prompt 82).
 *
 * <p>Scans an Avro data directory for the artifacts of interrupted writes and
 * crashes:
 * <ul>
 *   <li><b>Orphaned {@code .tmp} files</b> — {@code AtomicFileWriter} (Prompt 30)
 *       writes {@code <target>.tmp} before renaming over the target; a leftover
 *       {@code .tmp} means the writing JVM died before {@code commit()}, so the
 *       in-flight transaction is incomplete.</li>
 *   <li><b>Corrupt / truncated {@code .avro} files</b> — validated per block
 *       through {@link AvroSyncMarkerManager#validateIntegrity(File)} in lenient
 *       mode: an incomplete block tail (interrupted write) is reported as a
 *       recoverable interruption, a sync-marker mismatch as corruption. Each
 *       match carries the truncated/fully-intact decision needed by the
 *       recovery layer.</li>
 *   <li><b>{@code .bak} files</b> — recovery backups written by
 *       {@link AvroSyncMarkerManager#recoverToLastValidBlock(File)} or by a
 *       previous recovery run. Informational only, never acted on
 *       automatically.</li>
 *   <li><b>Empty {@code .avro} files</b> — zero-byte files that can never be
 *       parsed as Avro data.</li>
 * </ul>
 *
 * <p>Configuration is resolved per call from a system property, then the root
 * {@code config.properties}, then the code defaults:
 * <ul>
 *   <li>{@code avro.recovery.detect.on.startup} (default {@code true}) — whether
 *       an automatic detection on storage startup is expected;</li>
 *   <li>{@code avro.recovery.cleanup.temps} (default {@code true}) — whether
 *       orphaned {@code .tmp} artifacts should be removed after inspection.</li>
 *   <li>{@code avro.recovery.config.file} — overrides the config.properties
 *       location (test-support hook, mirrors the other AVRO config classes).</li>
 * </ul>
 *
 * <p>The class is thread-safe: it keeps no mutable state and every scan opens
 * its own file handles through {@link AvroSyncMarkerManager}.
 *
 * @since Prompt 82
 */
public final class AvroCrashDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCrashDetector.class);

    /** Config key: automatic crash detection on startup. */
    public static final String DETECT_ON_STARTUP_KEY = "avro.recovery.detect.on.startup";
    /** Config key: delete orphaned {@code .tmp} files after inspection. */
    public static final String CLEANUP_TEMPS_KEY = "avro.recovery.cleanup.temps";

    /** Code-level default for startup detection. */
    public static final boolean DEFAULT_DETECT_ON_STARTUP = true;
    /** Code-level default for temp cleanup. */
    public static final boolean DEFAULT_CLEANUP_TEMPS = true;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.recovery.config.file";

    private static final String TMP_SUFFIX = ".tmp";
    private static final String BAK_SUFFIX = ".bak";
    private static final String AVRO_SUFFIX = ".avro";

    private final boolean detectOnStartup;
    private final boolean cleanupTemps;
    private final AvroSyncMarkerManager syncMarkerManager;

    /**
     * An orphaned {@code <target>.tmp} left by an interrupted {@code AtomicFileWriter}
     * commit.
     *
     * @param file         the {@code .tmp} artifact
     * @param targetName   the intended {@code <target>} name (suffix removed)
     * @param size         file size in bytes
     * @param lastModified file mtime
     */
    public record TempArtifact(File file, String targetName, long size, Instant lastModified) {
    }

    /**
     * An Avro data file whose integrity scan found a problem.
     *
     * @param file            the {@code .avro} file
     * @param integrityResult the lenient-mode integrity report, or {@code null}
     *                        when the header is unreadable (not Avro data or
     *                        fully corrupt header) so no scan was possible
     * @param needsRecovery   {@code true} when a recovery action (truncation to
     *                        the last consistent block) is warranted
     */
    public record CorruptedFile(File file,
                                AvroSyncMarkerManager.IntegrityResult integrityResult,
                                boolean needsRecovery) {
    }

    /**
     * A {@code <file>.bak} backup left by a previous recovery run.
     *
     * @param file         the {@code .bak} artifact
     * @param originalName the name of the file the backup was taken from
     * @param size         file size in bytes
     * @param lastModified file mtime
     */
    public record BakFile(File file, String originalName, long size, Instant lastModified) {
    }

    /**
     * A zero-byte {@code .avro} file that can never be parsed as Avro data.
     *
     * @param file the empty file
     */
    public record EmptyFile(File file) {
    }

    /**
     * Outcome of {@link #detectCrash(File)}.
     *
     * @param dataDir        the scanned directory
     * @param orphanedTemps  orphaned {@code .tmp} artifacts (file order)
     * @param corruptedFiles corrupt/truncated {@code .avro} files (file order)
     * @param backupFiles    {@code .bak} backups found (informational)
     * @param emptyFiles     zero-byte {@code .avro} files
     * @param detectedAt     when the scan completed
     * @param scanTimeNanos  wall-clock duration of the scan
     */
    public record CrashDetectionReport(File dataDir,
                                       List<TempArtifact> orphanedTemps,
                                       List<CorruptedFile> corruptedFiles,
                                       List<BakFile> backupFiles,
                                       List<EmptyFile> emptyFiles,
                                       Instant detectedAt,
                                       long scanTimeNanos) {

        /** Whether any actionable problem was found (temps, corrupt files, empties). */
        public boolean hasIssues() {
            return !orphanedTemps.isEmpty() || !corruptedFiles.isEmpty() || !emptyFiles.isEmpty();
        }

        /** Number of actionable problems (temps + corrupt files + empty files). */
        public int issueCount() {
            return orphanedTemps.size() + corruptedFiles.size() + emptyFiles.size();
        }
    }

    /**
     * Creates a detector with explicit behaviour flags.
     *
     * @param detectOnStartup whether startup detection is expected
     * @param cleanupTemps    whether orphaned {@code .tmp} files are removed
     *                        after inspection
     */
    public AvroCrashDetector(boolean detectOnStartup, boolean cleanupTemps) {
        this.detectOnStartup = detectOnStartup;
        this.cleanupTemps = cleanupTemps;
        this.syncMarkerManager = new AvroSyncMarkerManager(false, true);
    }

    /**
     * Resolves the detector from a system property, then the root
     * {@code config.properties}, then the code defaults.
     *
     * @return a configured detector (never {@code null})
     */
    public static AvroCrashDetector resolve() {
        return new AvroCrashDetector(
                getBoolean(DETECT_ON_STARTUP_KEY, DEFAULT_DETECT_ON_STARTUP),
                getBoolean(CLEANUP_TEMPS_KEY, DEFAULT_CLEANUP_TEMPS));
    }

    /** Whether automatic startup detection is enabled. */
    public boolean detectOnStartup() {
        return detectOnStartup;
    }

    /** Whether orphaned temp files are removed after inspection. */
    public boolean cleanupTemps() {
        return cleanupTemps;
    }

    /**
     * Scans {@code dataDir} for crash artifacts.
     *
     * <p>A missing or non-directory {@code dataDir} is not an error: it simply
     * produces an empty report (first run before any save, or a different data
     * root), so a recovery pass over a fresh workspace stays a no-op.
     *
     * @param dataDir the Avro data directory to scan
     * @return the detection report (entries sorted by file name, case-insensitive)
     * @throws IOException if an Avro data file cannot be read for integrity
     *                     validation
     */
    public CrashDetectionReport detectCrash(File dataDir) throws IOException {
        long start = System.nanoTime();
        if (dataDir == null || !dataDir.isDirectory()) {
            LOGGER.info("Avro crash detection: data directory {} not present; nothing to scan",
                    dataDir == null ? "<null>" : dataDir.getPath());
            return new CrashDetectionReport(dataDir, List.of(), List.of(), List.of(), List.of(),
                    Instant.now(), System.nanoTime() - start);
        }

        List<TempArtifact> temps = new ArrayList<>();
        List<CorruptedFile> corrupted = new ArrayList<>();
        List<BakFile> backups = new ArrayList<>();
        List<EmptyFile> empties = new ArrayList<>();

        List<File> files = new ArrayList<>();
        File[] listed = dataDir.listFiles();
        if (listed != null) {
            for (File f : listed) {
                if (f.isFile()) {
                    files.add(f);
                }
            }
        }
        files.sort(Comparator.comparing(File::getName, String.CASE_INSENSITIVE_ORDER));

        for (File f : files) {
            String name = f.getName();
            String lower = name.toLowerCase(Locale.ROOT);
            if (lower.endsWith(TMP_SUFFIX)) {
                temps.add(new TempArtifact(f, name.substring(0, name.length() - TMP_SUFFIX.length()),
                        f.length(), modifiedOf(f)));
            } else if (lower.endsWith(BAK_SUFFIX)) {
                backups.add(new BakFile(f, name.substring(0, name.length() - BAK_SUFFIX.length()),
                        f.length(), modifiedOf(f)));
            } else if (lower.endsWith(AVRO_SUFFIX)) {
                classifyAvroFile(f, empties, corrupted);
            }
        }

        long nanos = System.nanoTime() - start;
        CrashDetectionReport report = new CrashDetectionReport(
                dataDir,
                Collections.unmodifiableList(temps),
                Collections.unmodifiableList(corrupted),
                Collections.unmodifiableList(backups),
                Collections.unmodifiableList(empties),
                Instant.now(), nanos);
        if (report.hasIssues()) {
            LOGGER.warn("Avro crash detection: {} issue(s) found in {} ({} orphaned temp(s), "
                            + "{} corrupt/truncated file(s), {} empty file(s))",
                    report.issueCount(), dataDir.getPath(), temps.size(), corrupted.size(), empties.size());
        } else {
            LOGGER.info("Avro crash detection: {} is clean ({})", dataDir.getPath(), report);
        }
        return report;
    }

    private static Instant modifiedOf(File file) {
        return Instant.ofEpochMilli(file.lastModified());
    }

    /**
     * Classifies a single .avro file as empty, corrupt, or healthy.
     * Adds the appropriate artifact to the provided lists.
     */
    private void classifyAvroFile(File f, List<EmptyFile> empties,
                                  List<CorruptedFile> corrupted) {
        if (f.length() == 0) {
            empties.add(new EmptyFile(f));
            return;
        }
        AvroSyncMarkerManager.IntegrityResult integrity;
        try {
            integrity = syncMarkerManager.validateIntegrity(f);
        } catch (IOException e) {
            LOGGER.warn("Avro crash detection: cannot validate {} (unreadable header): {}",
                    f.getPath(), e.getMessage());
            corrupted.add(new CorruptedFile(f, null, true));
            return;
        }
        boolean needsRecovery = !integrity.valid() || integrity.truncationOffset() >= 0;
        if (needsRecovery || !integrity.errors().isEmpty()) {
            corrupted.add(new CorruptedFile(f, integrity, needsRecovery));
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
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }
}