package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.net.messages.AuthResponseMessage;
import diesel.net.messages.QueryMessage;
import diesel.security.AuthException;
import diesel.security.UserStore;

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

/**
 * Prompt4 #31 end-to-end auth over the v2 wire protocol: the handshake AUTH
 * frame gates the data plane, DatabaseClient authenticates before the first
 * query, a created user can log in, ALTER USER changes one's own password,
 * and the CLI --reset-admin path restores admin/admin from a data directory.
 */
@Tag("network")
class AuthHandshakeTest {

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-auth-handshake");
        port = freePort();
        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, 0, database);
        serverThread = new Thread(server::start, "auth-handshake-server");
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
    void wrongPasswordIsRejectedWithUniformMessage() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "wrong");
        AuthException e = assertThrows(AuthException.class, client::connect);
        assertEquals(AuthException.INVALID_CREDENTIALS, e.getMessage());
        client.disconnect();
    }

    @Test
    void adminLoginIssuesSessionAndFlagsForcedChange() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin");
        try {
            client.connect();
            assertNotNull(client.getSessionToken(), "a successful handshake issues a session token");
            assertTrue(client.isMustChangePassword(), "the bootstrap admin must change its password");
            assertNotNull(client.executeQuery("SELECT NAME FROM DIESEL_USERS"));
        } finally {
            client.disconnect();
        }
    }

    @Test
    void dataPlaneQueryIsRefusedWithoutAuth() throws IOException {
        try (WireProbe probe = new WireProbe(port, 10000)) {
            probe.handshake();
            Object response = probe.query("SELECT NAME FROM DIESEL_USERS");
            assertEquals("Error: Authentication required", response,
                    "an unauthenticated connection cannot run queries");
        }
    }

    @Test
    void authHandshakeGrantsQueryAccess() throws IOException {
        try (WireProbe probe = new WireProbe(port, 10000)) {
            probe.handshake();
            AuthResponseMessage auth = probe.auth("admin", "admin");
            assertTrue(auth.isSuccess());
            assertNotNull(auth.getToken());
            assertTrue(auth.isMustChangePassword());

            Object response = probe.query("SELECT NAME FROM DIESEL_USERS");
            assertFalse(response instanceof String && ((String) response).startsWith("Error:"),
                    "an authenticated probe runs queries, got: " + response);
        }
    }

    @Test
    void createdUserCanLogInOverNetwork() {
        server.getAuthenticator().getUserStore().createUser("bob", "bob-pw");
        DatabaseClient client = new DatabaseClient("localhost", port, "bob", "bob-pw");
        try {
            client.connect();
            assertNotNull(client.getSessionToken());
            assertFalse(client.isMustChangePassword(), "a created user has no forced change");
        } finally {
            client.disconnect();
        }
    }

    @Test
    void alterUserChangesOwnPasswordAndClearsForcedChange() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin");
        try {
            client.connect();
            Object result = client.executeQuery("ALTER USER admin PASSWORD 'fresh-admin-pw'");
            assertEquals("Password changed: admin", result);
        } finally {
            client.disconnect();
        }

        DatabaseClient oldLogin = new DatabaseClient("localhost", port, "admin", "admin");
        assertThrows(AuthException.class, oldLogin::connect, "the default password is dead");
        oldLogin.disconnect();

        DatabaseClient newLogin = new DatabaseClient("localhost", port, "admin", "fresh-admin-pw");
        try {
            newLogin.connect();
            assertFalse(newLogin.isMustChangePassword(),
                    "the ALTER USER completed the forced change");
        } finally {
            newLogin.disconnect();
        }
    }

    @Test
    void cliResetAdminRestoresBootstrapStateFromDataDir() throws Exception {
        // Change the password first, then reset through the same local
        // filesystem path the --reset-admin CLI flag uses.
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin");
        try {
            client.connect();
            client.executeQuery("ALTER USER admin PASSWORD 'doomed-pw'");
        } finally {
            client.disconnect();
        }
        server.stop();
        serverThread.interrupt();
        database.close();

        CliRepl.resetAdmin(tempDir.toString());

        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, 0, database);
        serverThread = new Thread(server::start, "auth-handshake-server-2");
        serverThread.start();
        waitForServer(port);

        DatabaseClient restored = new DatabaseClient("localhost", port, "admin", "admin");
        try {
            restored.connect();
            assertNotNull(restored.getSessionToken(), "reset restores the default credentials");
            assertTrue(restored.isMustChangePassword(), "and the forced change with them");
        } finally {
            restored.disconnect();
        }
        DatabaseClient doomed = new DatabaseClient("localhost", port, "admin", "doomed-pw");
        assertThrows(AuthException.class, doomed::connect, "the pre-reset password is dead");
        doomed.disconnect();
    }

    @Test
    void userStoreAdminConstantsMatchBootstrap() {
        assertEquals("admin", UserStore.ADMIN_NAME);
        assertEquals("admin", UserStore.DEFAULT_ADMIN_PASSWORD);
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
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Intentionally ignored: best-effort temp-dir cleanup.
                }
            });
        } catch (IOException ignored) {
            // Intentionally ignored: the stream may already be closed if walking failed.
        }
    }
}
