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
 * PITR acceptance (prompt4 #43, PitrWithActiveTxTest): two transactions that
 * are still active at the restore target are both rolled back, while the
 * transaction that committed before the target stays visible.
 *
 * <p>The active transactions are left open (never committed) when the source
 * database is closed, so their rows are physically present in the persisted
 * files (DieselDB persists committed tables at commit time and on close) but
 * must be hidden by the PITR replay. The committed transaction is visible.
 */
@Tag("storage")
public class PitrWithActiveTxTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private Path fullDir;
    private Path restoreDir;
    private WALConfig walConfig;
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"), "pitr-active-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        fullDir = root.resolve("backup-full");
        restoreDir = root.resolve("restored");
        Files.createDirectories(dataDir);
        walConfig = PitrTest.enabledWalConfig(walDir);
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

    private UUID begin() {
        String result = (String) database.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(result.split(": ")[1]);
    }

    @Test
    void twoActiveTransactionsAreRolledBackAndCommittedOneStaysVisible() throws Exception {
        database.executeQuery("CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);

        // A committed transaction that must remain visible after PITR.
        UUID committed = begin();
        database.executeQuery("INSERT INTO T (ID, NAME) VALUES (1, 'committed-1')", committed);
        database.executeQuery("COMMIT", committed);

        // Full backup of the committed-only state.
        takeFullBackup();
        Thread.sleep(30);

        // Two transactions that are still active at the restore target.
        UUID activeA = begin();
        database.executeQuery("INSERT INTO T (ID, NAME) VALUES (2, 'active-a')", activeA);
        UUID activeB = begin();
        database.executeQuery("INSERT INTO T (ID, NAME) VALUES (3, 'active-b')", activeB);
        // Deliberately no COMMIT: both remain active at the target.

        // Persist so the two active rows are physically present in the files.
        database.saveTablesToDisk();
        long activeRows = database.getTable("T").getRawRowCount();
        assertEquals(3, activeRows, "all three rows are physically on disk before the restore");

        Thread.sleep(30);
        long targetTs = System.currentTimeMillis();
        database.close();
        database = null;

        PitrResult result = PitrManager.recoverToTimestamp(
                fullDir, List.of(), walDir, restoreDir, targetTs);
        assertTrue(result.getHiddenTxids() >= 2,
                "both active transactions must be classified hidden at the target");

        assertRestoredIds(1);
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
            assertEquals(wanted, actual,
                    "only the committed row must be visible; active-transaction rows are rolled back");
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
