package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
 * Prompt4 #36 acceptance (mTLS request): a client certificate is optional,
 * but when presented it must verify against the server truststore.
 */
@Tag("network")
class MtlsRequestTest {

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
        pkiDir = Files.createTempDirectory("dieseldb-mtls-request-pki");
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
        tempDir = Files.createTempDirectory("dieseldb-mtls-request");
        port = freePort();
        database = new Database(tempDir.toString());
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.keystore.path", pki.server().path().toString());
        props.setProperty("tls.keystore.password", pki.server().password());
        props.setProperty("tls.client.auth", "request");
        props.setProperty("tls.ca.path", pki.caPem().toString());
        props.setProperty("tls.truststore.type", "PEM");
        server = new DatabaseServer(port, 15000, 0, database, props);
        serverThread = new Thread(server::start, "mtls-request-server");
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
    void clientWithoutCertificateIsAccepted() throws Exception {
        try (SSLSocket socket = (SSLSocket) caTrustedFactory.createSocket("localhost", port)) {
            socket.startHandshake();
            assertEquals("OK", healthCheck(socket).getStatus(),
                    "request mode keeps client certificates optional");
        }
    }

    @Test
    void validClientCertificateIsAccepted() throws Exception {
        try (SSLSocket socket = (SSLSocket) mtlsFactory.createSocket("localhost", port)) {
            socket.startHandshake();
            assertEquals("OK", healthCheck(socket).getStatus());
        }
    }

    @Test
    void untrustedClientCertificateIsStillRejected() throws Exception {
        // A self-signed cert whose issuer is not acceptable is simply never
        // presented (the client sends no cert, which request mode allows).
        // The true "presented but invalid" probe is a CA-signed EXPIRED cert:
        // the issuer is acceptable so the client sends it, and the server
        // must fail verification.
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
        }, "a presented but expired certificate must be rejected even in request mode");
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
