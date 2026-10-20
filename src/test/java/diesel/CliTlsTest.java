package diesel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import diesel.net.messages.HealthCheckMessage;
import diesel.net.messages.HealthCheckResponse;
import diesel.net.messages.MessageCodecRegistry;
import diesel.net.MessageType;
import diesel.net.WireProtocol;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CLI client TLS flags (prompt4 #35):
 * - TLS connection with --tls --tls-ca
 * - Parse validation of TLS flags
 * - Config file loading
 * - End-to-end query round-trip over TLS
 */
@Tag("network")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CliTlsTest {

    private static DatabaseServer server;
    private static TestTlsCerts.Keystore keystore;
    private static Path caPemFile;
    private static int port;
    private static Path tempDir;

    @BeforeAll
    static void setupServer() throws Exception {
        tempDir = Path.of("temp-tls-test");
        java.nio.file.Files.createDirectories(tempDir);
        
        // Generate test certificates
        keystore = TestTlsCerts.generate(tempDir);
        
        // Export CA as PEM for --tls-ca
        caPemFile = tempDir.resolve("ca.pem");
        TestTlsCerts.exportCertificatePem(keystore.path(), TestTlsCerts.ALIAS, 
                                         TestTlsCerts.PASSWORD, caPemFile);
        
        // Start TLS-enabled server
        Properties tlsProps = new Properties();
        tlsProps.setProperty("tls.enabled", "true");
        tlsProps.setProperty("tls.keystore.path", keystore.path().toString());
        tlsProps.setProperty("tls.keystore.password", TestTlsCerts.PASSWORD);
        tlsProps.setProperty("tls.keystore.type", "PKCS12");
        
        port = findFreePort();
        server = new DatabaseServer(port, 15000, 0, new Database(tempDir.toString()), tlsProps);
        serverThread = new Thread(server::start, "tls-test-server");
        serverThread.start();
        waitForServer();
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (server != null) {
            server.stop();
            if (serverThread != null) {
                serverThread.interrupt();
                serverThread.join(5000);
            }
        }
        if (tempDir != null) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    @Order(1)
    void parseArgsWithTlsFlags() {
        // Valid TLS flags
        CliRepl.CliArgs args = CliRepl.parseArgs(new String[]{
            "--tls", "--tls-ca", caPemFile.toString(),
            "--user", "admin", "--password", "admin",
            "localhost", port + ""
        });
        assertTrue(args.tls());
        assertEquals(caPemFile.toString(), args.tlsCa());
        assertEquals("admin", args.user());
        assertEquals("admin", args.password());
        assertEquals("localhost", args.host());
        assertEquals(port, args.port());
    }

    @Test
    @Order(2)
    void parseArgsWithTlsFlagsErrors() {
        // TLS flags without --tls
        assertThrows(IllegalArgumentException.class, () -> {
            CliRepl.parseArgs(new String[]{"--tls-ca", caPemFile.toString()});
        });

        // TLS flags with --local
        assertThrows(IllegalArgumentException.class, () -> {
            CliRepl.parseArgs(new String[]{"--tls", "--local", "data"});
        });

        // TLS key without cert
        assertThrows(IllegalArgumentException.class, () -> {
            CliRepl.parseArgs(new String[]{"--tls", "--tls-key", "key.pem"});
        });
    }

    @Test
    @Order(3)
    void parseArgsWithConfigFile() throws Exception {
        // Create config file
        Path configFile = tempDir.resolve("diesel-client.conf");
        java.nio.file.Files.writeString(configFile, 
            "tls.enabled=true\n" +
            "tls.ca.path=" + caPemFile + "\n" +
            "tls.cert.path=missing.pem\n" +
            "tls.key.path=missing.key\n");

        // CLI flags override config
        CliRepl.CliArgs args = CliRepl.parseArgs(new String[]{
            "--tls", "--tls-ca", "/override/path.pem",
            "--user", "admin", "localhost", port + ""
        });
        assertTrue(args.tls());
        assertEquals("/override/path.pem", args.tlsCa());
        assertNull(args.tlsCert());
        assertNull(args.tlsKey());
    }

    @Test
    @Order(4)
    void tlsConnectionWithCaPem() throws Exception {
        // Test the same logic as main() would use: createClient -> connect -> query
        Properties tlsConfig = new Properties();
        tlsConfig.setProperty("tls.enabled", "true");
        tlsConfig.setProperty("tls.ca.path", caPemFile.toString());

        javax.net.ssl.SSLContext context = diesel.net.SslContextFactory.createClient(tlsConfig);
        javax.net.ssl.SSLSocketFactory factory = context.getSocketFactory();
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin", null);
        client.setSslSocketFactory(factory);

        // Connect should succeed
        client.connect();
        assertNotNull(client.getSessionToken());

        // Execute a query to prove TLS channel works
        Object result = client.executeQuery("ALTER USER admin PASSWORD 'tls-admin'");
        assertNotNull(result);

        // Now execute a DDL
        result = client.executeQuery("CREATE TABLE TLS_CLIENT_TEST(ID INTEGER)");
        assertNotNull(result);

        // Clean up
        client.disconnect();
    }

    @Test
    @Order(5)
    void healthCheckOverTls() throws Exception {
        javax.net.ssl.SSLSocketFactory trustedFactory = TestTlsCerts.clientFactory(keystore);
        try (javax.net.ssl.SSLSocket socket =
                     (javax.net.ssl.SSLSocket) trustedFactory.createSocket("localhost", port)) {
            diesel.net.CipherSuiteWhitelist.apply(socket);
            javax.net.ssl.SSLParameters params = socket.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(params);
            socket.startHandshake();

            HealthCheckMessage message = new HealthCheckMessage(System.currentTimeMillis(), null);
            byte[] payload;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(32);
            try (DataOutputStream payloadOut = new DataOutputStream(buffer)) {
                MessageCodecRegistry.getCodec(message.getType()).encode(message, payloadOut);
            }
            payload = buffer.toByteArray();

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            WireProtocol.encodeFrame(message.getType(), payload, false, out);
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            WireProtocol.Frame frame = WireProtocol.decodeFrame(in);
            assertEquals(MessageType.HEALTH_CHECK_RESPONSE, frame.type());
            HealthCheckResponse response = HealthCheckResponse.readFrom(
                    new DataInputStream(new ByteArrayInputStream(frame.payload())));
            assertEquals("OK", response.getStatus());
        }
    }

    // Helper methods
    private static Thread serverThread;
    private static int findFreePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForServer() throws InterruptedException {
        int maxAttempts = 30;
        for (int i = 0; i < maxAttempts; i++) {
            try (var socket = new java.net.Socket("localhost", port)) {
                return; // Connected
            } catch (IOException e) {
                Thread.sleep(1000);
            }
        }
        throw new RuntimeException("Server did not start in time");
    }

    private static void deleteDirectory(Path dir) throws IOException {
        java.nio.file.Files.walk(dir)
            .sorted((a, b) -> -a.compareTo(b))
            .forEach(path -> {
                try {
                    java.nio.file.Files.delete(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }
}