package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.net.CipherSuiteWhitelist;
import diesel.net.MessageType;
import diesel.net.WireProtocol;
import diesel.net.messages.HealthCheckMessage;
import diesel.net.messages.HealthCheckResponse;
import diesel.net.messages.MessageCodecRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Properties;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Prompt4 #34 acceptance: a TLS-enabled server completes a TLS 1.3 handshake
 * (TLS 1.2 fallback works too), the negotiated cipher is whitelisted, and the
 * v2 wire protocol round-trips over the encrypted transport. Also covers the
 * fail-fast path: TLS enabled with a missing keystore aborts startup instead
 * of silently serving plaintext.
 */
@Tag("network")
class TlsConnectionTest {

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
        keystoreDir = Files.createTempDirectory("dieseldb-tls-keystore");
        keystore = TestTlsCerts.generate(keystoreDir);
        clientFactory = TestTlsCerts.clientFactory(keystore);
    }

    @AfterAll
    static void removeKeystoreOnce() {
        deleteRecursively(keystoreDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-tls-connection");
        port = freePort();
        database = new Database(tempDir.toString());
        Properties tlsProps = new Properties();
        tlsProps.setProperty("tls.enabled", "true");
        tlsProps.setProperty("tls.keystore.path", keystore.path().toString());
        tlsProps.setProperty("tls.keystore.password", keystore.password());
        server = new DatabaseServer(port, 15000, 0, database, tlsProps);
        serverThread = new Thread(server::start, "tls-connection-server");
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
    void tls13HandshakeSucceedsAndHealthCheckRoundTrips() throws Exception {
        try (SSLSocket socket = (SSLSocket) clientFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[]{"TLSv1.3"});
            socket.startHandshake();

            assertEquals("TLSv1.3", socket.getSession().getProtocol(),
                    "the server prefers TLS 1.3 and the client offers it");
            assertTrue(CipherSuiteWhitelist.ALLOWED_SUITES.contains(socket.getSession().getCipherSuite()),
                    "negotiated cipher must come from the whitelist, got: "
                            + socket.getSession().getCipherSuite());

            HealthCheckResponse response = healthCheck(socket);
            assertEquals("OK", response.getStatus(),
                    "v2 frames round-trip over the encrypted transport");
        }
    }

    @Test
    void tls12FallbackStillNegotiatesWhitelistedCipher() throws Exception {
        try (SSLSocket socket = (SSLSocket) clientFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
            socket.startHandshake();

            assertEquals("TLSv1.2", socket.getSession().getProtocol());
            assertTrue(CipherSuiteWhitelist.ALLOWED_SUITES.contains(socket.getSession().getCipherSuite()),
                    "TLS 1.2 fallback must still land on a PFS AEAD suite, got: "
                            + socket.getSession().getCipherSuite());
            assertEquals("OK", healthCheck(socket).getStatus());
        }
    }

    @Test
    void enabledWithMissingKeystoreServerRefusesToStart() throws Exception {
        int deadPort = freePort();
        Path secondDir = Files.createTempDirectory("dieseldb-tls-missing-ks");
        Database secondDatabase = new Database(secondDir.toString());
        try {
            Properties badProps = new Properties();
            badProps.setProperty("tls.enabled", "true");
            badProps.setProperty("tls.keystore.path",
                    keystoreDir.resolve("does-not-exist.p12").toString());
            badProps.setProperty("tls.keystore.password", "changeit");
            DatabaseServer broken = new DatabaseServer(deadPort, 15000, 0, secondDatabase, badProps);
            // start() returns early on a broken keystore (fail fast) instead of
            // entering the accept loop, so calling it synchronously is safe.
            broken.start();
            assertThrows(IOException.class,
                    () -> new Socket("localhost", deadPort),
                    "no listener must come up when TLS cannot be initialised");
        } finally {
            secondDatabase.close();
            deleteRecursively(secondDir);
        }
    }

    private static HealthCheckResponse healthCheck(SSLSocket socket) throws IOException {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        HealthCheckMessage message = new HealthCheckMessage(System.currentTimeMillis(), null);
        byte[] payload;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
        try (DataOutputStream payloadOut = new DataOutputStream(buffer)) {
            MessageCodecRegistry.getCodec(message.getType()).encode(message, payloadOut);
        }
        payload = buffer.toByteArray();
        WireProtocol.encodeFrame(message.getType(), payload, false, out);
        out.flush();

        WireProtocol.Frame frame = WireProtocol.decodeFrame(in);
        assertEquals(MessageType.HEALTH_CHECK_RESPONSE, frame.type(),
                "server must answer the health check over TLS");
        return HealthCheckResponse.readFrom(
                new DataInputStream(new ByteArrayInputStream(frame.payload())));
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
