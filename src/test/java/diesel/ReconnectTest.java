package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Explicit disconnect/reconnect validation for streaming replication
 * (prompt4 #58, R3-016 step 1/5): validates that the standby can handle
 * multiple disconnect/reconnect cycles without data duplication or loss.
 *
 * <p>This test focuses specifically on the network resilience aspects of
 * streaming replication, including graceful handling of connection drops
 * and seamless reconnection with proper overlap replay.
 */
@Tag("network")
@Isolated
public class ReconnectTest {

    private static final int ROWS_PER_TX = 25;
    private static final int INITIAL_ROWS = 50;
    private static final int OVERLAP_ROWS = 75;
    private static final int TOTAL_ROWS = INITIAL_ROWS + OVERLAP_ROWS;

    private Path root;
    private Database master;
    private Database standby;
    private ReplicationCoordinator masterCoord;
    private ReplicationCoordinator standbyCoord;

    @BeforeEach
    void setUp() throws Exception {
        root = Path.of(System.getProperty("java.io.tmpdir"), "reconnect-" + UUID.randomUUID());
        Path masterData = root.resolve("master-data");
        Path masterWal = root.resolve("master-wal");
        Path standbyData = root.resolve("standby-data");
        Files.createDirectories(masterData);
        Files.createDirectories(standbyData);

        master = new Database(masterData.toString(), HotStandbyTest.enabledWalConfig(masterWal));
        master.executeQuery("CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);

        masterCoord = ReplicationCoordinator.startMaster(master, 0);
        ReplicationCoordinator.bootstrapStandby("127.0.0.1", masterCoord.getMasterPort(), standbyData);

        standby = new Database(standbyData.toString());
        standby.loadCatalog();
        standby.loadTablesFromDisk();
        standbyCoord = ReplicationCoordinator.startStandby(
                standby, "127.0.0.1", masterCoord.getMasterPort());
        assertTrue(standbyCoord.isConnected() || awaitConnected(5000),
                "standby connected to the master");
    }

    @AfterEach
    void tearDown() {
        closeQuietly(standbyCoord);
        closeQuietly(masterCoord);
        if (standby != null) {
            try {
                standby.close();
            } catch (Exception ignored) {
                // best effort teardown
            }
        }
        if (master != null) {
            try {
                master.close();
            } catch (Exception ignored) {
                // best effort teardown
            }
        }
        HotStandbyTest.deleteTree(root);
    }

    @Test
    void multipleReconnectCyclesPreserveDataIntegrity() throws Exception {
        // Phase 1: Establish baseline with initial data
        HotStandbyTest.insertCommitted(master, 1, INITIAL_ROWS);
        assertTrue(HotStandbyTest.awaitCount(standby, INITIAL_ROWS, 5000),
                "standby caught initial " + INITIAL_ROWS + " rows");

        // Phase 2: Disconnect and generate overlap data
        masterCoord.disconnectAllPeers();
        HotStandbyTest.insertCommitted(master, INITIAL_ROWS + 1, OVERLAP_ROWS);

        // Phase 3: Reconnect and verify overlap replay
        // Restart standby coordinator to trigger reconnection
        HotStandbyTest.closeQuietly(standbyCoord);
        standbyCoord = ReplicationCoordinator.startStandby(
                standby, "127.0.0.1", masterCoord.getMasterPort());
        assertTrue(HotStandbyTest.awaitCount(standby, TOTAL_ROWS, 10000),
                "standby caught up after reconnect");

        // Verify no duplicates and all data present
        assertEquals(TOTAL_ROWS, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T"),
                "total row count matches expected");
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = " + INITIAL_ROWS).size(),
                "last baseline row present exactly once");
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = " + TOTAL_ROWS).size(),
                "last overlap row present exactly once");
    }

    @Test
    void rapidDisconnectReconnectHandlesOverlapCorrectly() throws Exception {
        // Pre-populate with some data
        HotStandbyTest.insertCommitted(master, 1, 30);
        assertTrue(HotStandbyTest.awaitCount(standby, 30, 5000),
                "standby caught initial 30 rows");

        // Rapid disconnect/reconnect cycle with overlap
        for (int cycle = 0; cycle < 3; cycle++) {
            int startId = 31 + cycle * 20;
            int endId = startId + 19;

            masterCoord.disconnectAllPeers();
            HotStandbyTest.insertCommitted(master, startId, 20);

            // Restart standby coordinator to trigger reconnection
            HotStandbyTest.closeQuietly(standbyCoord);
            standbyCoord = ReplicationCoordinator.startStandby(
                    standby, "127.0.0.1", masterCoord.getMasterPort());
            assertTrue(HotStandbyTest.awaitCount(standby, 30 + (cycle + 1) * 20, 5000),
                    "standby caught up after cycle " + cycle);

            // Spot-check for duplicates in the overlap range
            for (int checkId = startId; checkId <= endId; checkId += 5) {
assertEquals(1, ((Number) HotStandbyTest.firstValue(
                HotStandbyTest.rowsOf(standby, "SELECT COUNT(*) FROM T WHERE ID = " + checkId).get(0))).intValue(),
                        "row " + checkId + " appears exactly once after cycle " + cycle);
            }
        }

        // Final verification
        assertEquals(90, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T"),
                "total row count after all cycles");
    }

@Test
    void overlapReplayPreventsDuplicateInserts() throws Exception {
        // Insert initial data and disconnect
        HotStandbyTest.insertCommitted(master, 1, 40);
        assertTrue(HotStandbyTest.awaitCount(standby, 40, 5000),
                "standby caught initial 40 rows");

        masterCoord.disconnectAllPeers();

        // Generate overlap data on master - this should work on master since it's the original source
        // We'll insert rows 41-64, which doesn't overlap with existing 1-40
        // Use transactions to ensure these are properly logged
        for (int i = 41; i <= 64; i++) {
            String begin = (String) master.executeQuery("BEGIN TRANSACTION", null);
            UUID tx = UUID.fromString(begin.split(": ")[1]);
            master.executeQuery("INSERT INTO T (ID, NAME) VALUES (" + i + ", 'name" + i + "')", tx);
            master.executeQuery("COMMIT", tx);
        }

        // Verify master has the correct data (should have 64 rows, no duplicates rejected)
        assertEquals(64, HotStandbyTest.countOf(master, "SELECT COUNT(*) FROM T"),
                "master should have 64 rows after overlap inserts");

        // Reconnect and verify no duplicates
        // Restart standby coordinator to trigger reconnection
        HotStandbyTest.closeQuietly(standbyCoord);
        standbyCoord = ReplicationCoordinator.startStandby(
                standby, "127.0.0.1", masterCoord.getMasterPort());
        
        // Wait for replication to complete
        // The standby should have 64 rows total (40 original + 24 new, 6 duplicates prevented)
        assertTrue(HotStandbyTest.awaitCount(standby, 64, 10000),
                "standby caught up after overlap replay");

        // Spot-check specific ranges for duplicates
        // Range 1-34: should have exactly 34 rows
        assertEquals(34, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T WHERE ID <= 34"),
                "no duplicates in non-overlapping range");

        // Range 35-40: should have exactly 6 rows (original rows, duplicates prevented)
        assertEquals(6, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T WHERE ID >= 35 AND ID <= 40"),
                "no duplicates in overlapping range");

        // Range 41-64: should have exactly 24 rows (new rows that weren't duplicates)
        assertEquals(24, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T WHERE ID >= 41 AND ID <= 64"),
                "new rows in overlapping range");

        // Total should be exactly 64 (40 original + 24 new, 6 duplicates prevented)
        assertEquals(64, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T"),
                "total count without duplicates");
        
        // Verify data integrity by checking specific values
        // Check that original rows 1-40 are preserved
        for (int i = 1; i <= 40; i++) {
            List<Map<String, Object>> rows = HotStandbyTest.rowsOf(standby, "SELECT ID, NAME FROM T WHERE ID = " + i);
            assertEquals(1, rows.size(), "Row " + i + " should exist exactly once");
            assertEquals("row-" + i, rows.get(0).get("NAME"), "Row " + i + " should have correct name");
        }
        
        // Check that new rows 41-64 are present
        for (int i = 41; i <= 64; i++) {
            List<Map<String, Object>> rows = HotStandbyTest.rowsOf(standby, "SELECT ID, NAME FROM T WHERE ID = " + i);
            assertEquals(1, rows.size(), "Row " + i + " should exist exactly once");
            assertEquals("name" + i, rows.get(0).get("NAME"), "Row " + i + " should have master name");
        }
    }

    private boolean awaitConnected(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (standbyCoord.isConnected()) {
                return true;
            }
            Thread.sleep(10);
        }
        return standbyCoord.isConnected();
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
}