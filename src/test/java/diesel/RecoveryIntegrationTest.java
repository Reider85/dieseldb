package diesel;

import diesel.recovery.RecoveryResult;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ARIES recovery integration acceptance test (prompt 4 #19): 100 mixed
 * transactions (50 commit / 50 no-commit), simulated kill, restart → 50 rows
 * visible, 50 rolled back.
 */
@Tag("smoke")
@Tag("storage")
public class RecoveryIntegrationTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private WALConfig walConfig;
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-it-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
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
                // crash-simulated instances are already closed
            }
        }
        try {
            deleteTree(root);
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }

    static WALConfig enabledWalConfig(Path walDir) {
        return WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES, 100_000,
                WALConfig.DISABLED, walDir.resolve("archive"), 0, WALConfig.DISABLED,
                true, FsyncPolicy.NONE, 5, 64);
    }

    private static void deleteTree(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walk(dir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // best-effort cleanup
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private static long countOf(Database db, String sql) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) db.executeQuery(sql, null);
        return ((Number) rows.get(0).values().iterator().next()).longValue();
    }

    @SuppressWarnings("unchecked")
    private static Set<Long> idsOf(Database db) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) db.executeQuery("SELECT ID FROM T", null);
        Set<Long> ids = new HashSet<>();
        for (Map<String, Object> row : rows) {
            Object value = row.containsKey("ID") ? row.get("ID") : row.get("T.ID");
            ids.add(((Number) value).longValue());
        }
        return ids;
    }

    private UUID begin(Database db) {
        String beginResult = (String) db.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(beginResult.split(": ")[1]);
    }

    @Test
    void mixedTransactionsAfterKillRecoverToCommittedOnly() throws Exception {
        database.executeQuery(
                "CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);

        // 50 committed transactions, ids 1..50.
        for (long id = 1; id <= 50; id++) {
            UUID tx = begin(database);
            database.executeQuery(
                    "INSERT INTO T (ID, NAME) VALUES (" + id + ", 'committed-" + id + "')", tx);
            database.executeQuery("COMMIT", tx);
        }
        // 50 uncommitted transactions, ids 51..100.
        for (long id = 51; id <= 100; id++) {
            UUID tx = begin(database);
            database.executeQuery(
                    "INSERT INTO T (ID, NAME) VALUES (" + id + ", 'pending-" + id + "')", tx);
            // no COMMIT — the transaction is still active at the kill
        }

        // Force the uncommitted rows to disk too: the undo must actively hide
        // them, not merely benefit from their absence. Flush both mirrors so
        // the delimited file and the serialized .table agree on 100 rows.
        database.getTable("T").saveToFile("T");
        database.getTable("T").saveToSerializedFile("T");
        assertEquals(100, database.getTable("T").getRawRowCount(),
                "all 100 rows are physically on disk before the kill");

        database.closeForCrashSimulation();
        database = null;

        // Restart: reload tables, run ARIES recovery, then query.
        Database recovered = new Database(dataDir.toString(), walConfig);
        try {
            recovered.loadTablesFromDisk();
            assertEquals(100, recovered.getTable("T").getRawRowCount(),
                    "restart loads all 100 physical rows");

            recovered.runRecovery();

            assertEquals(50, countOf(recovered, "SELECT COUNT(*) FROM T"),
                    "exactly the 50 committed rows are visible after recovery");
            Set<Long> visibleIds = idsOf(recovered);
            Set<Long> expected = java.util.stream.LongStream.rangeClosed(1, 50)
                    .boxed().collect(Collectors.toSet());
            assertEquals(expected, visibleIds,
                    "the visible ids are exactly the committed ones");

            // Tracker: 50 committed, 50 aborted, no id reuse possible.
            TxStatusTracker tracker = recovered.getTxStatusTracker();
            assertEquals(TxStatusTracker.TxStatus.COMMITTED, tracker.getStatus(1L));
            assertEquals(TxStatusTracker.TxStatus.COMMITTED, tracker.getStatus(50L));
            assertEquals(TxStatusTracker.TxStatus.ABORTED, tracker.getStatus(51L));
            assertEquals(TxStatusTracker.TxStatus.ABORTED, tracker.getStatus(100L));
            assertFalse(tracker.isActive(51L), "recovered active tx is not active anymore");
            assertTrue(tracker.registerTransaction() > 100L,
                    "new txids never collide with recovered ones");

            RecoveryManager manager = recovered.getRecoveryManager();
            assertNotNull(manager);
            assertTrue(manager.isRecoveryCompleted());
            assertTrue(manager.getRecoveryDurationMs() >= 0);
            RecoveryResult result = manager.getLastResult();
            assertNotNull(result);
            assertEquals(50, result.getCommittedTxidCount());
            assertEquals(50, result.getActiveTxidCount());
            assertEquals(50, result.getUndo().getUndoneInserts());

            // Idempotence: a second recover must not change the visible state.
            recovered.runRecovery();
            assertEquals(50, countOf(recovered, "SELECT COUNT(*) FROM T"),
                    "second recovery is idempotent");
        } finally {
            recovered.close();
        }
    }

    @Test
    void updateAndDeleteOfActiveTransactionAreUndone() throws Exception {
        database.executeQuery(
                "CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);
        // Seed two committed rows.
        for (long id = 1; id <= 2; id++) {
            UUID seed = begin(database);
            database.executeQuery(
                    "INSERT INTO T (ID, NAME) VALUES (" + id + ", 'row-" + id + "')", seed);
            database.executeQuery("COMMIT", seed);
        }
        // One active transaction: update row 1, delete row 2, insert row 3.
        UUID active = begin(database);
        database.executeQuery("UPDATE T SET NAME = 'updated' WHERE ID = 1", active);
        database.executeQuery("DELETE FROM T WHERE ID = 2", active);
        database.executeQuery("INSERT INTO T (ID, NAME) VALUES (3, 'new-row')", active);

        database.getTable("T").saveToFile("T");
        database.getTable("T").saveToSerializedFile("T");
        database.closeForCrashSimulation();
        database = null;

        Database recovered = new Database(dataDir.toString(), walConfig);
        try {
            recovered.loadTablesFromDisk();
            assertEquals(3, recovered.getTable("T").getRawRowCount(),
                    "restart loads all 3 physical rows, including the uncommitted insert");
            recovered.runRecovery();

            assertEquals(2, countOf(recovered, "SELECT COUNT(*) FROM T"),
                    "uncommitted insert is hidden; update/delete undone → 2 visible rows");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nameRows = (List<Map<String, Object>>)
                    recovered.executeQuery("SELECT NAME FROM T WHERE ID = 1", null);
            Object name = nameRows.get(0).containsKey("NAME")
                    ? nameRows.get(0).get("NAME") : nameRows.get(0).get("T.NAME");
            assertEquals("row-1", String.valueOf(name),
                    "uncommitted update restored the before value");
            assertFalse(idsOf(recovered).contains(3L),
                    "uncommitted insert (id 3) is invisible");
            assertTrue(idsOf(recovered).contains(2L),
                    "uncommitted delete kept row 2 alive");
        } finally {
            recovered.close();
        }
    }

    @Test
    void walDisabledRecoveryIsNoOp() {
        Database plain = new Database(dataDir.toString()); // WAL disabled by default
        try {
            assertNull(plain.getRecoveryManager(), "no recovery manager without WAL");
            assertDoesNotThrow(plain::runRecovery, "runRecovery is a no-op without WAL");
        } finally {
            plain.close();
        }
    }
}
