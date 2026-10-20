package diesel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.net.CipherSuiteWhitelist;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.List;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Prompt4 #34 acceptance: the cipher-suite whitelist is an allow-list — only
 * TLS 1.3 + PFS TLS 1.2 suites are ever enabled, legacy suites and protocols
 * stay disabled, and a client offering only a legacy suite cannot complete a
 * handshake against a TLS-enabled server.
 */
@Tag("network")
class CipherAuditTest {

    /** A non-PFS, CBC, TLS 1.2-only suite every JSSE still supports. */
    private static final String LEGACY_SUITE = "TLS_RSA_WITH_AES_128_CBC_SHA";

    private static Path keystoreDir;
    private static TestTlsCerts.Keystore keystore;
    private static SSLSocketFactory clientFactory;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeAll
    static void generateKeystoreOnce() throws Exception {
        keystoreDir = Files.createTempDirectory("dieseldb-tls-cipher-keystore");
        keystore = TestTlsCerts.generate(keystoreDir);
        clientFactory = TestTlsCerts.clientFactory(keystore);
    }

    @AfterAll
    static void removeKeystoreOnce() {
        deleteRecursively(keystoreDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-tls-cipher-audit");
        port = freePort();
        database = new Database(tempDir.toString());
        Properties tlsProps = new Properties();
        tlsProps.setProperty("tls.enabled", "true");
        tlsProps.setProperty("tls.keystore.path", keystore.path().toString());
        tlsProps.setProperty("tls.keystore.password", keystore.password());
        server = new DatabaseServer(port, 15000, 0, database, tlsProps);
        serverThread = new Thread(server::start, "tls-cipher-audit-server");
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
    void enabledSuitesAndProtocolsAreExactlyTheWhitelist() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            CipherSuiteWhitelist.apply(socket);

            List<String> enabled = List.of(socket.getEnabledCipherSuites());
            assertFalse(enabled.isEmpty(), "whitelist must enable at least one suite");
            for (String suite : enabled) {
                assertTrue(CipherSuiteWhitelist.ALLOWED_SUITES.contains(suite),
                        "enabled suite must be whitelisted: " + suite);
            }
            // Legacy classes explicitly absent from the enabled set.
            for (String suite : enabled) {
                assertFalse(suite.startsWith("TLS_RSA_"),
                        "non-PFS RSA key exchange must be disabled: " + suite);
                assertFalse(suite.contains("_CBC_"),
                        "CBC suites must be disabled: " + suite);
                assertFalse(suite.startsWith("SSL_"),
                        "SSLv3-grade suites must be disabled: " + suite);
            }

            assertArrayEquals(new String[]{"TLSv1.3", "TLSv1.2"},
                    socket.getEnabledProtocols(),
                    "only TLS 1.3 and TLS 1.2 may be enabled");
        }
    }

    @Test
    void filterDropsLegacySuitesAndKeepsWhitelistOrder() {
        String[] supported = {
                LEGACY_SUITE,
                "TLS_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"};
        String[] filtered = CipherSuiteWhitelist.filter(supported);
        assertArrayEquals(
                new String[]{
                        "TLS_AES_128_GCM_SHA256",
                        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"},
                filtered,
                "legacy suites are dropped; whitelist preference order is preserved");
    }

    @Test
    void legacyOnlyClientHandshakeIsRejected() throws Exception {
        try (SSLSocket socket = (SSLSocket) clientFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
            socket.setEnabledCipherSuites(new String[]{LEGACY_SUITE});
            assertThrows(IOException.class, socket::startHandshake,
                    "a client offering only a non-whitelisted suite must not complete the handshake");
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
