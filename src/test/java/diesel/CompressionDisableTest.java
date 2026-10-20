package diesel;

import diesel.net.messages.QueryMessage;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compression opt-out (prompt4.md #29): with
 * {@code compression.threshold.bytes = 0} the handshake negotiates
 * {@code none}, every frame travels uncompressed and the client API keeps
 * working.
 *
 * <p>The threshold is passed as a per-server configuration overlay (the
 * 5-argument {@link DatabaseServer} constructor), never as a JVM system
 * property: test classes run in parallel and other tests set config keys as
 * system properties for their own readers.
 */
@Tag("network")
public class CompressionDisableTest {

    /** Rows seeded for a result that would otherwise definitely be compressed. */
    private static final int ROWS = 300;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        Properties override = new Properties();
        override.setProperty("compression.threshold.bytes", "0");
        tempDir = Files.createTempDirectory("dieseldb-compression-disable");
        port = freePort();
        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, 0, database, override);
        serverThread = new Thread(server::start, "compression-disable-server");
        serverThread.start();
        waitForServer(port);

        database.executeQuery(
                "CREATE TABLE NET_RAW (ID LONG PRIMARY KEY, DATA STRING)", null);
        
        // Reset must-change password for admin to allow queries
        // Note: This needs to be done after the database is fully initialized
        try {
            database.executeQuery("ALTER USER admin PASSWORD 'test-admin-pw'", null);
        } catch (Exception e) {
            // If admin doesn't exist yet, create it first
            database.executeQuery("CREATE USER admin PASSWORD 'test-admin-pw'", null);
        }
        
        List<String> inserts = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            inserts.add("INSERT INTO NET_RAW (ID, DATA) VALUES (" + i + ", '"
                    + payloadFor(i) + "')");
        }
        for (String insert : inserts) {
            database.executeQuery(insert, null);
        }
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
    void handshakeNegotiatesNoneWhenThresholdIsZero() throws Exception {
        try (WireProbe probe = new WireProbe(port)) {
            var response = probe.handshake();
            assertTrue(response.isSuccess(), "the handshake still succeeds with compression off");
            assertEquals(diesel.net.CompressionHandshakeHandler.NONE,
                    response.getChosenAlgorithm(),
                    "a configured threshold of 0 forces the 'none' codec");
            assertEquals(0, response.getActualCompressionThresholdBytes(),
                    "threshold 0 is echoed so the peer never compresses");
            assertEquals(0, response.getActualCompressionLevel(),
                    "no level accompanies a disabled codec");
        }
    }

    @Test
    void largeFrameStaysUncompressed() throws Exception {
        try (WireProbe probe = new WireProbe(port)) {
            probe.handshake();
            probe.auth("admin", "test-admin-pw");
            probe.write(new QueryMessage("SELECT ID, DATA FROM NET_RAW", null));

            diesel.net.WireProtocol.Frame frame = probe.readFrame();
            assertFalse(frame.compressed(),
                    "threshold 0 must keep every frame uncompressed");
            assertTrue(frame.payload().length > 4096,
                    "the payload is large enough that compression would have kicked in, was "
                            + frame.payload().length + " bytes");

            Object value = probe.decodeValue(frame);
            assertTrue(value instanceof List);
            assertEquals(ROWS, ((List<?>) value).size());
        }
    }

    @Test
    void clientServesLargeResultsWithCompressionOff() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "test-admin-pw");
        try {
            client.connect();
            Object result = client.executeQuery("SELECT ID, DATA FROM NET_RAW");
            assertTrue(result instanceof List);
            assertEquals(ROWS, ((List<?>) result).size());

            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) ((List<?>) result).get(0);
            assertEquals(0, ((Number) col(first, "ID")).intValue());
            assertEquals(payloadFor(0), col(first, "DATA"));

            Object ddl = client.executeQuery("CREATE TABLE NET_RAW_COPY (ID LONG PRIMARY KEY)");
            assertTrue(ddl instanceof String);
            assertFalse(((String) ddl).startsWith(ErrorMessages.ERROR_PREFIX),
                    "DDL must not be an error with compression disabled: " + ddl);
        } finally {
            client.disconnect();
        }
    }

    /** 140 chars of mostly repeated text: well above the default threshold. */
    private static String payloadFor(int id) {
        return String.format("payload_%05d_", id)
                + "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
                + "yyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    }

    private static Object col(Map<String, Object> row, String name) {
        Object exact = row.get(name);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        throw new AssertionError("column " + name + " not found in " + row.keySet());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForServer(int targetPort) {
        TestWaitHelper.waitForCondition(() -> {
            try (Socket socket = new Socket("localhost", targetPort)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        }, Duration.ofSeconds(15));
    }

    private static void deleteRecursively(Path root) {
        if (root == null) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Best-effort temp-dir cleanup on Windows.
                }
            });
        } catch (IOException ignored) {
            // Best-effort temp-dir cleanup on Windows.
        }
    }
}
