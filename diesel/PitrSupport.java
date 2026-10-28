package diesel;

import diesel.recovery.PitrResult;
import diesel.recovery.PointInTimeRecovery;
import diesel.wal.WALConfig;
import diesel.wal.WALManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Public facade for point-in-time recovery (prompt 4 #43): opens the restored
 * database directory, replays the staged WAL up to a target timestamp, persists
 * the result and closes.
 *
 * <p>Lives in the {@code diesel} package because {@code Database} is
 * package-private; {@code diesel.backup.PitrManager} calls this facade instead
 * of reaching into the engine directly. The facade owns the engine lifecycle
 * (open → load → replay → persist → close) so callers never touch
 * package-private classes.
 *
 * <p>The WAL subsystem is intentionally disabled while applying: PITR reads
 * the staged WAL through a standalone read-only {@link WALManager} and applies
 * logical records directly, so no new WAL entries are produced during replay.
 */
public final class PitrSupport {

    private PitrSupport() {
        // utility class
    }

    /**
     * Opens the database at {@code dataDir}, replays the staged WAL directory
     * {@code walDir} up to {@code targetTimestampMs} and persists the result.
     *
     * @param dataDir           the restored database directory (tables loaded from disk)
     * @param walDir            the staged WAL directory holding the post-backup segments
     * @param lastBackupLsn     the backup's high-water LSN; records at or below are skipped
     * @param targetTimestampMs the restore target in epoch millis
     * @return the pass statistics
     * @throws IOException if opening, replaying or persisting fails
     */
    public static PitrResult recoverToTimestamp(Path dataDir, Path walDir,
                                                long lastBackupLsn, long targetTimestampMs)
            throws IOException {
        Objects.requireNonNull(dataDir, "dataDir");
        Objects.requireNonNull(walDir, "walDir");
        if (!Files.isDirectory(dataDir)) {
            throw new IOException("Restored database directory not found: " + dataDir);
        }
        if (!Files.isDirectory(walDir)) {
            throw new IOException("Staged WAL directory not found: " + walDir);
        }

        // WAL disabled: PITR must not produce new log records while applying.
        Database database = new Database(dataDir.toString());
        try {
            database.loadTablesFromDisk();

            WALConfig walConfig = WALConfig.of(walDir, 8 * 1024 * 1024L);
            try (WALManager wal = new WALManager(walConfig)) {
                DatabasePitrSink sink = new DatabasePitrSink(database);
                PitrResult result = PointInTimeRecovery.recover(
                        wal, lastBackupLsn, targetTimestampMs, sink);

                // Persist the applied state so the restored directory is durable.
                database.saveTablesToDisk();
                return result;
            }
        } finally {
            database.close();
        }
    }
}
