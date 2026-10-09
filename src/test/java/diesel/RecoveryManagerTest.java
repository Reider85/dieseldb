package diesel;

import diesel.wal.WALConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RecoveryManager metric/JMX surface tests (prompt 4 #19).
 */
@Tag("smoke")
@Tag("storage")
public class RecoveryManagerTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-mgr-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        Files.createDirectories(dataDir);
        database = new Database(dataDir.toString(),
                RecoveryIntegrationTest.enabledWalConfig(walDir));
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            try {
                database.close();
            } catch (RuntimeException ignored) {
                // already closed
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

    @Test
    void recoverOnEmptyWalSucceedsAndExportsDuration() throws Exception {
        RecoveryManager manager = database.getRecoveryManager();
        assertNotNull(manager, "WAL-enabled database owns a RecoveryManager");
        assertFalse(manager.isRecoveryCompleted());
        assertEquals(-1, manager.getRecoveryDurationMs());
        assertNull(manager.getLastResult());

        var result = manager.recover();

        assertTrue(manager.isRecoveryCompleted());
        assertTrue(manager.getRecoveryDurationMs() >= 0, "duration metric exported");
        assertNotNull(result);
        assertNotNull(manager.getLastResult());
        assertEquals(0, result.getCommittedTxidCount());
        assertEquals(0, result.getActiveTxidCount());
    }

    @Test
    void jmxAttributesAreReadableAndCloseUnregisters() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        RecoveryManager manager = database.getRecoveryManager();
        assertNotNull(manager);

        ObjectName name = findRegisteredName(server);
        assertNotNull(name, "RecoveryManager MBean is registered");
        assertEquals(Boolean.FALSE, server.getAttribute(name, "recovery.completed"));

        manager.recover();
        assertEquals(Boolean.TRUE, server.getAttribute(name, "recovery.completed"));
        assertTrue(((Number) server.getAttribute(name, "recovery.duration.ms")).longValue() >= 0);
        assertNotNull(server.getAttribute(name, "recovery.last.lsn"));

        manager.close();
        assertFalse(server.isRegistered(name), "close() unregisters the MBean");
        manager.close(); // idempotent
    }

    @Test
    void runRecoveryTwiceIsIdempotent() throws Exception {
        database.executeQuery(
                "CREATE TABLE T (ID LONG PRIMARY KEY, NAME STRING)", null);
        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        UUID tx = UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO T (ID, NAME) VALUES (1, 'a')", tx);
        database.executeQuery("COMMIT", tx);

        database.runRecovery();
        database.runRecovery();

        Object result = database.executeQuery("SELECT COUNT(*) FROM T", null);
        @SuppressWarnings("unchecked")
        java.util.List<java.util.Map<String, Object>> rows =
                (java.util.List<java.util.Map<String, Object>>) result;
        assertEquals(1, ((Number) rows.get(0).values().iterator().next()).longValue());
    }

    private static ObjectName findRegisteredName(MBeanServer server) throws Exception {
        java.util.Set<ObjectName> names = server.queryNames(
                new ObjectName("diesel:type=RecoveryManager,*"), null);
        return names.isEmpty() ? null : names.iterator().next();
    }
}
