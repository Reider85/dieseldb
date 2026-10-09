package diesel;

import diesel.wal.WALConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ARIES recovery acceptance test (prompt 4 #19): one long transaction with
 * N inserts (default 1,000,000, override {@code -Ddiesel.recovery.longtx.rows=N})
 * killed mid-flight → every one of its changes is rolled back after restart.
 *
 * <p>Tagged {@code @LargeTest}: skipped by default and in CI; runs with the
 * large profile ({@code make large-test}, 4 GB heap).
 */
public class RecoveryWithLongTransactionTest {

    private static final int DEFAULT_ROWS = 1_000_000;

    private Path root;
    private Path dataDir;
    private Path walDir;
    private WALConfig walConfig;
    private Database database;
    private java.util.logging.Level previousLogLevel;

    @BeforeEach
    void setUp() throws IOException {
        // Hot-path INFO logging (one line per INSERT parse) would dominate a
        // million-row run; demote for the duration of this test only.
        java.util.logging.Logger dieselLogger = java.util.logging.Logger.getLogger("diesel");
        previousLogLevel = dieselLogger.getLevel();
        dieselLogger.setLevel(java.util.logging.Level.WARNING);
        root = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-longtx-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        Files.createDirectories(dataDir);
        walConfig = RecoveryIntegrationTest.enabledWalConfig(walDir);
        database = new Database(dataDir.toString(), walConfig);
    }

    @AfterEach
    void tearDown() {
        java.util.logging.Logger.getLogger("diesel").setLevel(previousLogLevel);
        if (database != null) {
            try {
                database.close();
            } catch (RuntimeException ignored) {
                // crash-simulated instance already closed
            }
        }
        try {
            Files.walk(root)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
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

    @LargeTest
    void millionInsertsInOneActiveTransactionAreFullyRolledBack() throws Exception {
        int rows = Integer.getInteger("diesel.recovery.longtx.rows", DEFAULT_ROWS);
        database.executeQuery(
                "CREATE TABLE T (ID LONG, NAME STRING)", null);

        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID tx = UUID.fromString(beginResult.split(": ")[1]);
        for (int i = 1; i <= rows; i++) {
            database.executeQuery(
                    "INSERT INTO T (ID, NAME) VALUES (" + i + ", 'r" + i + "')", tx);
        }
        // No COMMIT: the single transaction is still active at the kill.
        // Force the physical rows to disk (both mirrors) so the undo must
        // actively hide them.
        database.getTable("T").saveToFile("T");
        database.getTable("T").saveToSerializedFile("T");
        assertEquals(rows, database.getTable("T").getRawRowCount());

        database.closeForCrashSimulation();
        database = null;

        Database recovered = new Database(dataDir.toString(), walConfig);
        try {
            recovered.loadTablesFromDisk();
            assertEquals(rows, recovered.getTable("T").getRawRowCount(),
                    "restart loads every physical row of the long transaction");

            recovered.runRecovery();

            Object result = recovered.executeQuery("SELECT COUNT(*) FROM T", null);
            @SuppressWarnings("unchecked")
            java.util.List<java.util.Map<String, Object>> countRows =
                    (java.util.List<java.util.Map<String, Object>>) result;
            long count = ((Number) countRows.get(0).values().iterator().next()).longValue();
            assertEquals(0, count, "all " + rows + " inserts of the active tx are rolled back");

            RecoveryManager manager = recovered.getRecoveryManager();
            assertNotNull(manager);
            assertEquals(rows, manager.getLastSink().getUndosApplied(),
                    "every insert undo was applied");
            assertEquals(1, manager.getLastResult().getActiveTxidCount());
            assertEquals(0, manager.getLastResult().getCommittedTxidCount());
        } finally {
            recovered.close();
        }
    }
}




