package diesel;

import diesel.backup.PitrManager;
import diesel.recovery.PitrResult;
import diesel.wal.FsyncPolicy;
import diesel.wal.WALConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PITR acceptance (prompt4 #43, PitrTest): a full backup followed by committed
 * transactions, restored to a target timestamp that falls between the backup
 * and the latest commit, reproduces exactly the committed state at that
 * instant.
 *
 * <p>The scenario anchors the clock with {@code Thread.sleep} gaps between
 * phases (no wall-bound assertion — only the ordering of real timestamps
 * matters), then restores to a target captured <i>between</i> phase 1 and
 * phase 2. Phase 1 must be visible; phase 2 (committed after the target) must
 * be gone.
 */
@Tag("storage")
public class PitrTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private Path fullDir;
    private Path restoreDir;
    private WALConfig walConfig;
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"), "pitr-test-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        fullDir = root.resolve("backup-full");
        restoreDir = root.resolve("restored");
        Files.createDirectories(dataDir);
        walConfig = enabledWalConfig(walDir);
        database = new Database(dataDir.toString(), walConfig);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            try {
                database.close();
            } catch (RuntimeException ignored) {
                // already closed by the scenario
            }
        }
        deleteTreeQuietly(root);
    }

    static WALConfig enabledWalConfig(Path walDir) {
        return WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES, 100_000,
                WALConfig.DISABLED, walDir.resolve("archive"), 0, WALConfig.DISABLED,
                true, FsyncPolicy.NONE, 5, 64);
    }

    private UUID begin() {
        String result = (String) database.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(result.split(": ")[1]);
    }

    private void commit(UUID tx) {
        database.executeQuery("COMMIT", tx);
    }

    @Test
    void restoreToTargetBetweenCommitsShowsExactlyTheStateAtThatMoment() throws Exception {
        database.executeQuery("CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);

        // Phase 0: committed rows that predate the backup.
        for (long id = 1; id <= 3; id++) {
            UUID tx = begin();
            database.executeQuery("INSERT INTO T (ID, NAME) VALUES (" + id + ", 'pre-" + id + "')", tx);
            commit(tx);
        }

        // Full backup of the phase-0 state.
        takeFullBackup();

        // Phase 1: committed AFTER the backup but BEFORE the target.
        Thread.sleep(30);
        for (long id = 4; id <= 6; id++) {
            UUID tx = begin();
            database.executeQuery("INSERT INTO T (ID, NAME) VALUES (" + id + ", 'early-" + id + "')", tx);
            commit(tx);
        }

        // The PITR target is captured between phase 1 and phase 2.
        Thread.sleep(30);
        long targetTs = System.currentTimeMillis();

        // Phase 2: committed AFTER the target (must NOT appear after restore).
        Thread.sleep(30);
        for (long id = 7; id <= 9; id++) {
            UUID tx = begin();
            database.executeQuery("INSERT INTO T (ID, NAME) VALUES (" + id + ", 'late-" + id + "')", tx);
            commit(tx);
        }

        // Persist phase-2 rows so the backup files themselves are ahead of the
        // target; the restore must rely on WAL replay, not file absence.
        database.saveTablesToDisk();
        database.close();
        database = null;

        PitrResult result = PitrManager.recoverToTimestamp(
                fullDir, List.of(), walDir, restoreDir, targetTs);
        assertTrue(result.getVisibleTxids() > 0, "some transactions must be visible at the target");
        assertTrue(result.getAppliedInserts() > 0, "post-backup pre-target inserts must be applied");

        assertRestoredIds(1, 2, 3, 4, 5, 6);
    }

    private void takeFullBackup() throws IOException {
        // PhysicalBackup requires an existing checkpoint file in the source dir.
        Files.writeString(dataDir.resolve("checkpoint.json"),
                "{\"version\": 1, \"lsn\": 1000, \"timestamp\": 0}");
        new diesel.backup.PhysicalBackup(dataDir, fullDir).backup();
    }

    /** Reopens the restored directory and asserts the exact set of visible IDs. */
    private void assertRestoredIds(long... expected) {
        Database restored = new Database(restoreDir.toString());
        try {
            restored.loadTablesFromDisk();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rows =
                    (List<Map<String, Object>>) restored.executeQuery("SELECT ID FROM T", null);
            Set<Long> actual = new HashSet<>();
            for (Map<String, Object> row : rows) {
                Object value = row.containsKey("ID") ? row.get("ID") : row.get("T.ID");
                actual.add(((Number) value).longValue());
            }
            Set<Long> wanted = new HashSet<>();
            for (long id : expected) {
                wanted.add(id);
            }
            assertEquals(wanted, actual, "restored visible ids must match the target-time state");
        } finally {
            restored.close();
        }
    }

    private static void deleteTreeQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> -a.compareTo(b)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }
}
