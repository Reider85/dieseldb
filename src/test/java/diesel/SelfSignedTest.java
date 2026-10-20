package diesel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that a client connecting to a self-signed server without --tls-ca
 * fails with a CertificateException in the cause chain (prompt4 #35).
 */
@Tag("network")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SelfSignedTest {

    private static DatabaseServer server;
    private static TestTlsCerts.Keystore keystore;
    private static int port;
    private static Path tempDir;

    @BeforeAll
    static void setupServer() throws Exception {
        tempDir = Path.of("temp-selfsigned-test");
        java.nio.file.Files.createDirectories(tempDir);
        
        // Generate test certificates (self-signed, not trusted by system)
        keystore = TestTlsCerts.generate(tempDir);
        
        // Start TLS-enabled server with self-signed cert
        Properties tlsProps = new Properties();
        tlsProps.setProperty("tls.enabled", "true");
        tlsProps.setProperty("tls.keystore.path", keystore.path().toString());
        tlsProps.setProperty("tls.keystore.password", TestTlsCerts.PASSWORD);
        tlsProps.setProperty("tls.keystore.type", "PKCS12");
        
        port = findFreePort();
        server = new DatabaseServer(port, 15000, 0, new Database(tempDir.toString()), tlsProps);
        serverThread = new Thread(server::start, "selfsigned-test-server");
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
    void selfSignedCertWithoutCaPathFails() {
        // Client tries to connect to self-signed server without --tls-ca
        // Should fail with CertificateException in cause chain
        
        Properties tlsConfig = new Properties();
        tlsConfig.setProperty("tls.enabled", "true");
        // No tls.ca.path → uses system truststore (doesn't trust self-signed)
        
        javax.net.ssl.SSLSocketFactory factory;
        try {
            javax.net.ssl.SSLContext ctx = diesel.net.SslContextFactory.createClient(tlsConfig);
            factory = ctx.getSocketFactory();
        } catch (Exception e) {
            fail("Should be able to create SSLContext, got: " + e);
            return;
        }
        
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin", null);
        client.setSslSocketFactory(factory);

        // Connect should fail with CertificateException in cause chain
        RuntimeException ex = assertThrows(RuntimeException.class, client::connect);
        
        // Check that the cause chain contains a CertificateException or related
        Throwable cause = ex.getCause();
        assertNotNull(cause, "Exception should have a cause");
        
        // Look for CertificateException or SSLHandshakeException with CertificateException cause
        boolean foundCertException = false;
        Throwable current = cause;
        while (current != null) {
            if (current instanceof java.security.cert.CertificateException) {
                foundCertException = true;
                break;
            }
            if (current instanceof javax.net.ssl.SSLHandshakeException) {
                // Check if this SSLHandshakeException has a CertificateException cause
                Throwable sslCause = current.getCause();
                if (sslCause instanceof java.security.cert.CertificateException) {
                    foundCertException = true;
                    break;
                }
            }
            current = current.getCause();
        }
        
        assertTrue(foundCertException, 
            "Expected CertificateException in cause chain, got: " + cause.getClass().getName() + 
            " with message: " + cause.getMessage());
    }

    @Test
    @Order(2)
    void selfSignedWithExplicitCaPathSucceeds() throws Exception {
        // Same as above but with explicit CA path → should succeed
        
        Properties tlsConfig = new Properties();
        tlsConfig.setProperty("tls.enabled", "true");
        tlsConfig.setProperty("tls.ca.path", keystore.path().toString());
        tlsConfig.setProperty("tls.truststore.type", "PKCS12");
        tlsConfig.setProperty("tls.truststore.password", keystore.password());
        
        javax.net.ssl.SSLContext ctx = diesel.net.SslContextFactory.createClient(tlsConfig);
        javax.net.ssl.SSLSocketFactory factory = ctx.getSocketFactory();
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "admin", null);
        client.setSslSocketFactory(factory);

        // Should connect successfully
        client.connect();
        assertNotNull(client.getSessionToken());
        client.disconnect();
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