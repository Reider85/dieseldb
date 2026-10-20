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
 * Tests that a client connecting to a server with an expired certificate
 * fails with a CertificateException in the cause chain (prompt4 #35).
 */
@Tag("network")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExpiredCertTest {

    private static DatabaseServer server;
    private static TestTlsCerts.ExpiredKeystores keystores;
    private static int port;
    private static Path tempDir;

    @BeforeAll
    static void setupServer() throws Exception {
        tempDir = Path.of("temp-expired-test");
        java.nio.file.Files.createDirectories(tempDir);
        
        // Generate expired certificate
        keystores = TestTlsCerts.generateExpired(tempDir);
        
        // Start TLS-enabled server with expired cert
        Properties tlsProps = new Properties();
        tlsProps.setProperty("tls.enabled", "true");
        tlsProps.setProperty("tls.keystore.path", keystores.server().path().toString());
        tlsProps.setProperty("tls.keystore.password", keystores.server().password());
        tlsProps.setProperty("tls.keystore.type", "PKCS12");
        
        port = findFreePort();
        server = new DatabaseServer(port, 15000, 0, new Database(tempDir.toString()), tlsProps);
        serverThread = new Thread(server::start, "expired-test-server");
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
    void expiredCertFails() {
        // Client tries to connect to server with expired cert
        // Should fail with CertificateException in cause chain
        
        // Trust the CA (which is valid) but the server cert is expired
        Properties tlsConfig = new Properties();
        tlsConfig.setProperty("tls.enabled", "true");
        tlsConfig.setProperty("tls.ca.path", keystores.caPem().toString());
        
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
        boolean foundExpiredMessage = false;
        Throwable current = cause;
        while (current != null) {
            if (current instanceof java.security.cert.CertificateException) {
                foundCertException = true;
                if (current.getMessage() != null && 
                    (current.getMessage().contains("expired") || 
                     current.getMessage().contains("not yet valid"))) {
                    foundExpiredMessage = true;
                }
            }
            if (current instanceof javax.net.ssl.SSLHandshakeException) {
                // Check if this SSLHandshakeException has a CertificateException cause
                Throwable sslCause = current.getCause();
                if (sslCause instanceof java.security.cert.CertificateException) {
                    foundCertException = true;
                    if (sslCause.getMessage() != null && 
                        (sslCause.getMessage().contains("expired") || 
                         sslCause.getMessage().contains("not yet valid"))) {
                        foundExpiredMessage = true;
                    }
                }
            }
            current = current.getCause();
        }
        
        assertTrue(foundCertException, 
            "Expected CertificateException in cause chain, got: " + cause.getClass().getName() + 
            " with message: " + cause.getMessage());
        
        // Optionally: check that the failure mentions expiration
        // (This depends on the exact error message from the JDK)
        String message = cause.getMessage();
        if (message != null) {
            assertTrue(message.contains("expired") || message.contains("not yet valid") || 
                      message.contains("validity") || message.contains("certificate"),
                "Error message should indicate certificate expiration issue: " + message);
        }
    }

    @Test
    @Order(2)
    void expiredCertWithSystemTrustFails() {
        // Client uses system truststore (doesn't trust our expired cert at all)
        
        Properties tlsConfig = new Properties();
        tlsConfig.setProperty("tls.enabled", "true");
        // No tls.ca.path → uses system truststore
        
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

        // Should fail with CertificateException (unknown CA)
        RuntimeException ex = assertThrows(RuntimeException.class, client::connect);
        Throwable cause = ex.getCause();
        assertNotNull(cause, "Exception should have a cause");
        
        // This time it should be "unable to find valid certification path to requested target"
        boolean foundCertException = false;
        Throwable current = cause;
        while (current != null) {
            if (current instanceof java.security.cert.CertificateException) {
                foundCertException = true;
                break;
            }
            current = current.getCause();
        }
        
        assertTrue(foundCertException, 
            "Expected CertificateException in cause chain, got: " + cause.getClass().getName());
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