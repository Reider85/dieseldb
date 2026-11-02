package diesel;

import diesel.replication.ReplicationWire;
import diesel.wal.WALConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hot-standby acceptance test (prompt4 #58, R3-016 step 1/5): SELECT works on
 * the replica, every write (DML and DDL) is rejected with a read-only error,
 * and the replication apply pipeline keeps flowing past rejected client SQL.
 */
@Tag("network")
public class HotStandbyTest {

    private static final int SEED_ROWS = 25;

    private Path root;
    private Database master;
    private Database standby;
    private ReplicationCoordinator masterCoord;
    private ReplicationCoordinator standbyCoord;

    @BeforeEach
    void setUp() throws Exception {
        root = Path.of(System.getProperty("java.io.tmpdir"), "hotstandby-" + UUID.randomUUID());
        Path masterData = root.resolve("master-data");
        Path masterWal = root.resolve("master-wal");
        Path standbyData = root.resolve("standby-data");
        Files.createDirectories(masterData);
        Files.createDirectories(standbyData);

        master = new Database(masterData.toString(), enabledWalConfig(masterWal));
        master.executeQuery("CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);
        insertCommitted(master, 1, SEED_ROWS);

        masterCoord = ReplicationCoordinator.startMaster(master, 0);
        ReplicationCoordinator.bootstrapStandby("127.0.0.1", masterCoord.getMasterPort(), standbyData);

        standby = new Database(standbyData.toString());
        standby.loadCatalog();
        standby.loadTablesFromDisk();
        standbyCoord = ReplicationCoordinator.startStandby(
                standby, "127.0.0.1", masterCoord.getMasterPort());

        assertTrue(awaitCount(standby, SEED_ROWS, 5000),
                "standby sees the base-backup rows");
    }

    @AfterEach
    void tearDown() {
        closeQuietly(standbyCoord);
        closeQuietly(masterCoord);
        closeQuietly(standby);
        closeQuietly(master);
        deleteTree(root);
    }

    @Test
    void selectWorksWithBaseBackupRows() {
        assertEquals(SEED_ROWS, countOf(standby, "SELECT COUNT(*) FROM T"),
                "SELECT COUNT(*) is served from the standby");
        List<Map<String, Object>> rows = rowsOf(standby,
                "SELECT NAME FROM T WHERE ID = 1");
        assertEquals(1, rows.size());
        assertEquals("row-1", String.valueOf(firstValue(rows.get(0))));
    }

    @Test
    void writesAreRejectedOnStandby() {
        assertTrue(standby.isHotStandbyMode(), "coordinator enabled hot-standby mode");

        for (String write : new String[]{
                "INSERT INTO T (ID, NAME) VALUES (9999, 'x')",
                "UPDATE T SET NAME = 'y' WHERE ID = 1",
                "DELETE FROM T WHERE ID = 1",
                "CREATE TABLE X (A LONG)",
                "DROP TABLE T",
                "ALTER TABLE T ADD COLUMN EXTRA LONG"}) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> standby.executeQuery(write, null),
                    "write must be rejected: " + write);
            assertTrue(e.getMessage().contains("hot standby"),
                    "read-only error expected, got: " + e.getMessage());
        }

        // The gate must not poison reads: SELECT still works after rejections
        // and the row count is untouched (rejected writes never applied).
        assertEquals(SEED_ROWS, countOf(standby, "SELECT COUNT(*) FROM T"));
        assertThrows(IllegalStateException.class,
                () -> standby.executeQuery("INSERT INTO T (ID, NAME) VALUES (10000, 'x')", null));
        assertEquals(SEED_ROWS, countOf(standby, "SELECT COUNT(*) FROM T"));
    }

    @Test
    void streamContinuesAfterRejectedWrites() throws Exception {
        // Rejected client writes must not disturb the replication pipeline:
        // rows committed on the master after the rejection still land here.
        assertThrows(IllegalStateException.class,
                () -> standby.executeQuery("INSERT INTO T (ID, NAME) VALUES (7777, 'x')", null));

        insertCommitted(master, SEED_ROWS + 1, 10);

        assertTrue(awaitCount(standby, SEED_ROWS + 10, 5000),
                "streaming apply continues after rejected standby writes");
        assertFalse(rowsOf(standby, "SELECT ID FROM T WHERE ID = 7777").size() > 0,
                "the rejected row never appears");
    }

    // ── helpers ─────────────────────────────────────────────────────────

    static WALConfig enabledWalConfig(Path walDir) {
        return WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES, 100_000,
                WALConfig.DISABLED, walDir.resolve("archive"), 0, WALConfig.DISABLED,
                true, diesel.wal.FsyncPolicy.GROUP, 5, 64);
    }

    static void insertCommitted(Database db, int fromId, int count) {
        for (int id = fromId; id < fromId + count; id++) {
            String begin = (String) db.executeQuery("BEGIN TRANSACTION", null);
            UUID tx = UUID.fromString(begin.split(": ")[1]);
            db.executeQuery("INSERT INTO T (ID, NAME) VALUES (" + id + ", 'row-" + id + "')", tx);
            db.executeQuery("COMMIT", tx);
        }
    }

    @SuppressWarnings("unchecked")
    static long countOf(Database db, String sql) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) db.executeQuery(sql, null);
        return ((Number) firstValue(rows.get(0))).longValue();
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rowsOf(Database db, String sql) {
        return (List<Map<String, Object>>) db.executeQuery(sql, null);
    }

    static Object firstValue(Map<String, Object> row) {
        return row.values().iterator().next();
    }

    /** Polls {@code SELECT COUNT(*)} until it reaches {@code expected}. */
    static boolean awaitCount(Database db, long expected, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                if (countOf(db, "SELECT COUNT(*) FROM T") >= expected) {
                    return true;
                }
            } catch (RuntimeException ignored) {
                // table may not be visible for a moment during apply
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    static void closeQuietly(Object closeable) {
        if (closeable != null) {
            if (closeable instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) closeable).close();
                } catch (Exception ignored) {
                    // best effort teardown
                }
            } else if (closeable instanceof diesel.Database) {
                try {
                    ((diesel.Database) closeable).close();
                } catch (Exception ignored) {
                    // best effort teardown
                }
            }
        }
    }

    static void deleteTree(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            walk.sorted((a, b) -> -a.compareTo(b)).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }

    static List<String> ids(Database db) {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> row : rowsOf(db, "SELECT ID FROM T")) {
            out.add(String.valueOf(firstValue(row)));
        }
        return out;
    }

    /** Kept for symmetry with other replication tests. */
    static int wireVersion() {
        return ReplicationWire.VERSION;
    }
}
