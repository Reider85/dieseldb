package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.net.messages.AuthResponseMessage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Properties;

/**
 * Prompt4 #34 acceptance: with {@code tls.enabled=false} (the default) the
 * server keeps serving plain TCP — the v2 wire protocol, compression
 * handshake and AUTH all work exactly as before TLS landed, and no TLS
 * wrapping is applied to accepted sockets.
 */
@Tag("network")
class DisableTlsTest {

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-tls-disabled");
        port = freePort();
        database = new Database(tempDir.toString());
        Properties props = new Properties();
        props.setProperty("tls.enabled", "false");
        server = new DatabaseServer(port, 15000, 0, database, props);
        serverThread = new Thread(server::start, "tls-disabled-server");
        serverThread.start();
        waitForServer(port);
    }

    @AfterEach
    void tearDown() {
        try {
            if (server != null) {
                server.stop();
            }
            if (serverThread != null) {
                serverThread.interrupt();
            }
            if (database != null) {
                database.close();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void plainTcpProbeCompletesHandshakeAndAuth() throws IOException {
        try (WireProbe probe = new WireProbe(port, 10000)) {
            assertFalse(probe.rawSocket() instanceof javax.net.ssl.SSLSocket,
                    "tls.enabled=false must leave accepted sockets as plain TCP");
            assertTrue(probe.handshake() != null, "the compression handshake works over plain TCP");
            AuthResponseMessage auth = probe.auth("admin", "admin");
            assertTrue(auth.isSuccess(), "AUTH works over plain TCP, got: " + auth.getMessage());
        }
    }

    @Test
    void plainTcpQueryRoundTripStillWorks() throws IOException {
        try (WireProbe probe = new WireProbe(port, 10000)) {
            probe.handshake();
            probe.auth("admin", "admin");
            // The bootstrap admin carries the must-change flag; ALTER USER is
            // the only statement the hard block lets through (prompt4 #32).
            Object changed = probe.query("ALTER USER admin PASSWORD 'plain-tcp-admin'");
            assertEquals("Password changed: admin", changed);
            Object result = probe.query("SELECT NAME FROM DIESEL_USERS");
            assertFalse(result instanceof String && ((String) result).startsWith("Error:"),
                    "queries still run over plain TCP when TLS is off, got: " + result);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForServer(int port) throws Exception {
        TestWaitHelper.waitForCondition(() -> {
            try (Socket socket = new Socket("localhost", port)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        }, Duration.ofSeconds(15));
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Best-effort temp-dir cleanup.
                }
            });
        } catch (IOException ignored) {
            // Best-effort temp-dir cleanup.
        }
    }
}
