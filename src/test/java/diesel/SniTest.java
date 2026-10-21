package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Comparator;
import java.util.Properties;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt4 #36 acceptance (SNI): the server presents the keystore matching
 * the client's SNI hostname; unknown or absent SNI falls back to the default
 * keystore; a DIESEL_TENANTS row (CREATE TENANT) also drives selection.
 */
@Tag("network")
class SniTest {

    private static Path pkiDir;
    private static TestTlsCerts.SniPki pki;
    private static SSLSocketFactory clientFactory;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeAll
    static void generatePkiOnce() throws Exception {
        pkiDir = Files.createTempDirectory("dieseldb-sni-pki");
        pki = TestTlsCerts.generateSniPki(pkiDir);
        clientFactory = TestTlsCerts.clientFactory(pki.ca());
    }

    @AfterAll
    static void removePkiOnce() {
        deleteRecursively(pkiDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-sni-connection");
        port = freePort();
        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, 0, database, sniConfigProps());
        serverThread = new Thread(server::start, "sni-server");
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

    private static Properties sniConfigProps() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.keystore.path", pki.defaultKs().path().toString());
        props.setProperty("tls.keystore.password", pki.defaultKs().password());
        props.setProperty("tls.sni.tenant1.example.com.keystore.path",
                pki.tenant1().path().toString());
        props.setProperty("tls.sni.tenant1.example.com.keystore.password",
                pki.tenant1().password());
        props.setProperty("tls.sni.tenant2.example.com.keystore.path",
                pki.tenant2().path().toString());
        props.setProperty("tls.sni.tenant2.example.com.keystore.password",
                pki.tenant2().password());
        return props;
    }

    @Test
    void sniTenant1ReceivesTenant1Certificate() throws Exception {
        assertEquals("tenant1.example.com", peerCommonName("tenant1.example.com"));
    }

    @Test
    void sniTenant2ReceivesTenant2Certificate() throws Exception {
        assertEquals("tenant2.example.com", peerCommonName("tenant2.example.com"));
    }

    @Test
    void unknownSniFallsBackToDefaultCertificate() throws Exception {
        assertEquals("localhost", peerCommonName("unknown.example.com"));
    }

    @Test
    void absentSniFallsBackToDefaultCertificate() throws Exception {
        try (Socket raw = new Socket(InetAddress.getByName("localhost"), port);
                SSLSocket ssl = (SSLSocket) clientFactory.createSocket(
                        raw, "localhost", port, true)) {
            ssl.startHandshake();
            assertEquals("localhost", commonNameOf(ssl));
        }
    }

    @Test
    void tenantsTableRowDrivesSelection() throws Exception {
        // Separate server whose only tenant source is DIESEL_TENANTS.
        server.stop();
        serverThread.interrupt();
        database.close();

        database = new Database(tempDir.toString());
        database.executeQuery(
                "CREATE TENANT tabletenant SNI 'tabletenant.example.com' "
                + "KEYSTORE '" + pki.tenant1().path().toString().replace("\\", "\\\\") + "' "
                + "PASSWORD '" + pki.tenant1().password() + "'", null);
        // DatabaseServer.start() reloads tables from disk; persist the row first.
        database.saveTablesToDisk();
        port = freePort();
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.keystore.path", pki.defaultKs().path().toString());
        props.setProperty("tls.keystore.password", pki.defaultKs().password());
        server = new DatabaseServer(port, 15000, 0, database, props);
        serverThread = new Thread(server::start, "sni-table-server");
        serverThread.start();
        waitForServer(port);

        assertEquals("tenant1.example.com", peerCommonName("tabletenant.example.com"),
                "the CREATE TENANT row must select the tenant keystore");
    }

    /** Connects with the given SNI hostname and returns the peer cert CN. */
    private String peerCommonName(String sniHostname) throws Exception {
        try (Socket raw = new Socket("localhost", port);
                SSLSocket ssl = (SSLSocket) clientFactory.createSocket(
                        raw, sniHostname, port, true)) {
            ssl.startHandshake();
            return commonNameOf(ssl);
        }
    }

    private static String commonNameOf(SSLSocket ssl) throws Exception {
        Certificate[] chain = ssl.getSession().getPeerCertificates();
        assertTrue(chain.length > 0, "server must present a certificate");
        String dn = ((X509Certificate) chain[0]).getSubjectX500Principal().getName();
        for (String part : dn.split(",")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("CN=")) {
                return trimmed.substring(3);
            }
        }
        return dn;
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
