package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Prompt4 #36 acceptance (mTLS require): a client without a certificate is
 * rejected at the handshake, a CA-signed client certificate completes the
 * v2 round-trip, an untrusted certificate is rejected, and a missing server
 * truststore aborts startup (fail fast).
 */
@Tag("network")
class MtlsTest {

    private static Path pkiDir;
    private static TestTlsCerts.MtlsPki pki;
    private static SSLSocketFactory caTrustedFactory;
    private static SSLSocketFactory mtlsFactory;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeAll
    static void generatePkiOnce() throws Exception {
        pkiDir = Files.createTempDirectory("dieseldb-mtls-pki");
        pki = TestTlsCerts.generateMtlsPki(pkiDir);
        caTrustedFactory = TestTlsCerts.clientFactory(pki.ca());
        mtlsFactory = TestTlsCerts.clientFactory(pki.ca(), pki.client());
    }

    @AfterAll
    static void removePkiOnce() {
        deleteRecursively(pkiDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-mtls-require");
        port = freePort();
        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, 0, database, requireProps());
        serverThread = new Thread(server::start, "mtls-require-server");
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

    private static Properties requireProps() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.keystore.path", pki.server().path().toString());
        props.setProperty("tls.keystore.password", pki.server().password());
        props.setProperty("tls.client.auth", "require");
        props.setProperty("tls.ca.path", pki.caPem().toString());
        props.setProperty("tls.truststore.type", "PEM");
        return props;
    }

    @Test
    void clientWithoutCertificateIsRejected() {
        // Rejection surfaces as an IO-level handshake failure: normally
        // SSLHandshakeException, but under load the connection reset can
        // arrive first as SocketException — both are IOExceptions.
        assertThrows(IOException.class, () -> {
            try (SSLSocket socket = (SSLSocket) caTrustedFactory.createSocket("localhost", port)) {
                socket.setEnabledProtocols(new String[]{"TLSv1.3"});
                socket.startHandshake();
                // TLS 1.3 may defer the server's rejection to the first read.
                socket.getInputStream().read();
            }
        }, "require mode must reject a cert-less client at the handshake");
        // The server survives: a valid mTLS client still works right after.
        try (SSLSocket socket = (SSLSocket) mtlsFactory.createSocket("localhost", port)) {
            socket.startHandshake();
            assertEquals("OK", healthCheck(socket).getStatus());
        } catch (Exception e) {
            throw new AssertionError("server must keep serving after a rejected client", e);
        }
    }

    @Test
    void validClientCertificateCompletesRoundTrip() throws Exception {
        try (SSLSocket socket = (SSLSocket) mtlsFactory.createSocket("localhost", port)) {
            socket.startHandshake();
            assertEquals("OK", healthCheck(socket).getStatus(),
                    "CA-signed client certificate completes the v2 round-trip");
        }
    }

    @Test
    void untrustedClientCertificateIsRejected() throws Exception {
        // CA-signed but expired: the client presents it (issuer acceptable)
        // and the server must reject it at verification.
        TestTlsCerts.Keystore expired =
                TestTlsCerts.expiredClientKeystore(Files.createDirectories(
                        tempDir.resolve("expired")), pki.ca());
        SSLSocketFactory expiredFactory =
                TestTlsCerts.clientFactory(pki.ca(), expired);
        assertThrows(IOException.class, () -> {
            try (SSLSocket socket = (SSLSocket) expiredFactory.createSocket("localhost", port)) {
                socket.setEnabledProtocols(new String[]{"TLSv1.3"});
                socket.startHandshake();
                socket.getInputStream().read();
            }
        }, "a presented but expired certificate must be rejected in require mode");
    }

    @Test
    void requireWithoutTruststoreRefusesToStart() throws Exception {
        int deadPort = freePort();
        Path secondDir = Files.createTempDirectory("dieseldb-mtls-no-trust");
        Database secondDatabase = new Database(secondDir.toString());
        try {
            Properties bad = requireProps();
            bad.remove("tls.ca.path");
            DatabaseServer broken = new DatabaseServer(deadPort, 15000, 0, secondDatabase, bad);
            // start() returns early on the fail-fast check, safe to call sync.
            broken.start();
            assertThrows(IOException.class,
                    () -> new Socket("localhost", deadPort),
                    "no listener must come up when mTLS has no truststore");
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
        assertEquals(MessageType.HEALTH_CHECK_RESPONSE, frame.type());
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
