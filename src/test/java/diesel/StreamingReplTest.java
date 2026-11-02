package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Physical streaming replication acceptance test (prompt4 #58, R3-016
 * step 1/5): master + 1 standby at ~1000 inserts/sec with slave-behind-master
 * lag under 100 ms.
 *
 * <p>Load: 20 explicit transactions × 50 rows = 1000 inserts (explicit MVCC
 * transactions are the WAL-logged path of this engine — see
 * {@code InsertQuery.appendInsertWal}). Lag is measured as the time from a
 * master {@code COMMIT} returning to the standby's {@code SELECT COUNT(*)}
 * observing the committed rows; the poll runs concurrently with the seeder so
 * the measurement observes the stream under real write pressure.
 * {@code @Isolated} keeps unrelated classes from stealing the 100 ms budget
 * on this 4-core machine (same policy as GroupCommitThroughputTest).
 */
@Tag("network")
@Isolated
public class StreamingReplTest {

    private static final int TX_COUNT = 5;
    private static final int ROWS_PER_TX = 200;
    private static final int ROWS = TX_COUNT * ROWS_PER_TX;
    /** Acceptance budget: slave behind master < 100 ms (prompt4 #58). */
    private static final long MAX_LAG_MS = 100;

    private Path root;
    private Database master;
    private Database standby;
    private ReplicationCoordinator masterCoord;
    private ReplicationCoordinator standbyCoord;

    @BeforeEach
    void setUp() throws Exception {
        root = Path.of(System.getProperty("java.io.tmpdir"), "streaming-" + UUID.randomUUID());
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
    void thousandInsertsPerSecondLagStaysUnder100Ms() throws Exception {
        // Commit timestamps per transaction: rows of a transaction become
        // visible on the standby only at its COMMIT (buffered-until-commit),
        // so every row of tx t shares t's commit time. Atomic types publish
        // them to the polling thread without additional synchronization.
        final java.util.concurrent.atomic.AtomicLongArray commitNanos =
                new java.util.concurrent.atomic.AtomicLongArray(TX_COUNT);
        final java.util.concurrent.atomic.AtomicInteger loadedRows =
                new java.util.concurrent.atomic.AtomicInteger(0);

        long loadStart = System.nanoTime();
        Thread seeder = new Thread(() -> {
            int id = 1;
            for (int t = 0; t < TX_COUNT; t++) {
                String begin = (String) master.executeQuery("BEGIN TRANSACTION", null);
                UUID tx = UUID.fromString(begin.split(": ")[1]);
                for (int r = 0; r < ROWS_PER_TX; r++, id++) {
                    master.executeQuery(
                            "INSERT INTO T (ID, NAME) VALUES (" + id + ", 'row-" + id + "')", tx);
                }
                master.executeQuery("COMMIT", tx);
                // commit time first, then publish the transaction as loaded —
                // the poller must never see loadedRows without commitNanos.
                commitNanos.set(t, System.nanoTime());
                loadedRows.set(t + 1);
            }
        }, "streaming-seeder");
        seeder.start();

        // Poll the standby concurrently; for each transaction whose rows are
        // now visible, lag = (discovery time - commit time).
        long maxLagNanos = 0;
        int seenTx = 0;
        long deadline = System.nanoTime() + 60_000_000_000L;
        while (seenTx < TX_COUNT && System.nanoTime() < deadline) {
            long count = HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T");
            long now = System.nanoTime();
            while (seenTx < loadedRows.get() && count >= (long) (seenTx + 1) * ROWS_PER_TX) {
                long commitTime = commitNanos.get(seenTx);
                if (commitTime != 0) {
                    maxLagNanos = Math.max(maxLagNanos, now - commitTime);
                }
                seenTx++;
            }
            if (seenTx < TX_COUNT) {
                Thread.sleep(0);
            }
        }
        seeder.join(60_000);

        long loadNanos = System.nanoTime() - loadStart;
        long insertsPerSec = ROWS * 1_000_000_000L / Math.max(1, loadNanos);

        assertEquals(ROWS, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T"),
                "standby caught up with all " + ROWS + " rows");
        assertTrue(insertsPerSec >= 1000,
                "seeder only reached " + insertsPerSec + " inserts/sec");
        long maxLagMs = maxLagNanos / 1_000_000L;
        assertTrue(maxLagMs < MAX_LAG_MS,
                "slave-behind-master lag " + maxLagMs + " ms exceeds " + MAX_LAG_MS + " ms");

        // Spot-check values, not only the count.
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT NAME FROM T WHERE ID = " + ROWS).size(),
                "last row's values are applied on the standby");
    }

    @Test
    void applyIsIdempotentAcrossReconnectOverlap() throws Exception {
        // Force a reconnect mid-stream: the overlap replay must not
        // double-apply committed rows.
        HotStandbyTest.insertCommitted(master, 1, 50);
        assertTrue(HotStandbyTest.awaitCount(standby, 50, 5000), "initial catch-up");

        masterCoord.disconnectAllPeers();
        HotStandbyTest.insertCommitted(master, 51, 50);

        assertTrue(HotStandbyTest.awaitCount(standby, 100, 10000),
                "catch-up after forced reconnect");
        assertEquals(100, HotStandbyTest.countOf(standby, "SELECT COUNT(*) FROM T"),
                "replayed overlap did not duplicate rows");
        
        // Comprehensive spot-checks for duplicate prevention
        spotCheckForDuplicates(standby, 1, 100);
    }

    private void spotCheckForDuplicates(Database standby, int startId, int endId) throws Exception {
        // Check boundary conditions
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = " + startId).size(), 
                "first row (" + startId + ") present exactly once");
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = " + endId).size(), 
                "last row (" + endId + ") present exactly once");

        // Check overlap boundary (around row 50-51)
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = 50").size(), 
                "overlap boundary row 50 present exactly once");
        assertEquals(1, HotStandbyTest.rowsOf(standby,
                "SELECT ID FROM T WHERE ID = 51").size(), 
                "overlap boundary row 51 present exactly once");

        // Check random samples across the range
        int[] sampleIds = {5, 25, 45, 55, 75, 95};
        for (int id : sampleIds) {
            assertEquals(1, HotStandbyTest.rowsOf(standby,
                    "SELECT ID FROM T WHERE ID = " + id).size(), 
                    "sample row " + id + " present exactly once");
        }

        // Verify data integrity with content checks
        for (int id : sampleIds) {
            var rows = HotStandbyTest.rowsOf(standby,
                    "SELECT NAME FROM T WHERE ID = " + id);
            assertEquals(1, rows.size(), "row " + id + " has exactly one record");
            assertEquals("row-" + id, rows.get(0).get("NAME"), 
                    "row " + id + " has correct name value");
        }

        // Check for any orphaned or duplicate records by counting ranges
        for (int rangeStart = 1; rangeStart <= endId; rangeStart += 10) {
            int rangeEnd = Math.min(rangeStart + 9, endId);
            int expectedCount = rangeEnd - rangeStart + 1;
            long actualCount = HotStandbyTest.countOf(standby, 
                    "SELECT COUNT(*) FROM T WHERE ID >= " + rangeStart + " AND ID <= " + rangeEnd);
            assertEquals(expectedCount, actualCount, 
                    "range " + rangeStart + "-" + rangeEnd + " has correct count");
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
