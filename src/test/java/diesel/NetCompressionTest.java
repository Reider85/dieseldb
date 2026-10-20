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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Network compression over the v2 wire protocol (prompt4.md #29): a large
 * result must travel with {@link diesel.net.WireProtocol#COMPRESSED_FLAG} set,
 * a small one must not, and the {@link DatabaseClient} must round-trip both.
 */
@Tag("network")
public class NetCompressionTest {

    /** Rows seeded into the big table: 1000 x ~119 chars = ~125 KB raw payload, the acceptance-case size (100 KB -> 5-10 KB). */
    private static final int ROWS = 1000;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-net-compression");
        port = freePort();
database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, database);
        serverThread = new Thread(server::start, "net-compression-server");
        serverThread.start();
        waitForServer(port);

        database.executeQuery(
                "CREATE TABLE NET_COMP (ID LONG PRIMARY KEY, DATA STRING)", null);
        
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
            inserts.add("INSERT INTO NET_COMP (ID, DATA) VALUES (" + i + ','
                    + '\'' + payloadFor(i) + '\'' + ')');
        }
        for (String insert : inserts) {
            database.executeQuery(insert, null);
        }
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
        if (database != null) {
            database.close();
        }
        deleteRecursively(tempDir);
    }

    @Test
    void handshakeNegotiatesZstdWithConfiguredThreshold() throws Exception {
        try (WireProbe probe = new WireProbe(port)) {
            var response = probe.handshake();
            assertTrue(response.isSuccess(), "the handshake must succeed");
            assertEquals("ZSTD", response.getChosenAlgorithm(),
                    "the server prefers ZSTD when the client offers it");
            assertEquals(4096, response.getActualCompressionThresholdBytes(),
                    "the configured threshold travels in the response");
            assertTrue(response.getActualCompressionLevel() > 0,
                    "a positive level accompanies a negotiated codec");
        }
    }

    @Test
    void largeSelectFrameIsCompressedOnTheWire() throws Exception {
        try (WireProbe probe = new WireProbe(port)) {
            probe.handshake();
            probe.auth("admin", "test-admin-pw");
            probe.write(new QueryMessage("SELECT ID, DATA FROM NET_COMP", null));

            diesel.net.WireProtocol.Frame frame = probe.readFrame();
            assertEquals(diesel.net.MessageType.QUERY_RESULT, frame.type(),
                    "the answer to a SELECT is a single query-result frame");
            assertTrue(frame.compressed(),
                    "a result far above the threshold must carry the compressed flag");

            Object value = probe.decodeValue(frame);
            assertTrue(value instanceof List, "a SELECT round-trips as a row list");
            List<?> rows = (List<?>) value;
            assertEquals(ROWS, rows.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> first = (Map<String, Object>) rows.get(0);
            assertTrue(((Number) col(first, "ID")).intValue() == 0);
            assertEquals(payloadFor(0), col(first, "DATA"));

            // Acceptance (prompt4 #29): ~100 KB of result compresses into the
            // 5-10 KB band; the test asserts the band's upper bound, smaller is
            // strictly better. The raw payload is ~125 KB (1000 rows x ~119
            // chars + column metadata) before the COMPRESSED_FLAG is applied.
            assertTrue(frame.payload().length <= 10_000,
                    "a ~125 KB result must compress to at most 10 KB, was "
                            + frame.payload().length + " bytes");
        }
    }

    @Test
    void smallSelectFrameIsNotCompressed() throws Exception {
        try (WireProbe probe = new WireProbe(port)) {
            probe.handshake();
            probe.auth("admin", "test-admin-pw");
            probe.write(new QueryMessage("SELECT ID, DATA FROM NET_COMP WHERE ID = 0", null));

            diesel.net.WireProtocol.Frame frame = probe.readFrame();
            assertFalse(frame.compressed(),
                    "a single-row payload below the threshold travels raw");

            Object value = probe.decodeValue(frame);
            assertTrue(value instanceof List);
            assertEquals(1, ((List<?>) value).size());
        }
    }

    @Test
    void clientRoundTripsLargeResult() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "test-admin-pw");
        try {
            client.connect();
            long startNanos = System.nanoTime();
            Object result = client.executeQuery("SELECT ID, DATA FROM NET_COMP");
            long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
            assertTrue(result instanceof List, "the client rebuilds the row list");
            assertEquals(ROWS, ((List<?>) result).size());
            assertTrue(durationMs < 5000,
                    "throughput must not collapse under compression, the ~125 KB round-trip took "
                            + durationMs + " ms");

            @SuppressWarnings("unchecked")
            Map<String, Object> last =
                    (Map<String, Object>) ((List<?>) result).get(ROWS - 1);
            assertTrue(((Number) col(last, "ID")).intValue() == ROWS - 1);
            assertEquals(payloadFor(ROWS - 1), col(last, "DATA"));
        } catch (DieselException e) {
            throw new AssertionError("client round-trip failed: " + e.getMessage(), e);
        } finally {
            client.disconnect();
        }
    }

    @Test
    void aggregateAndStatusResultsRoundTrip() {
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "test-admin-pw");
        try {
            client.connect();
            Object count = client.executeQuery("SELECT COUNT(*) AS CNT FROM NET_COMP");
            assertTrue(count instanceof List);
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) ((List<?>) count).get(0);
            assertEquals(ROWS, ((Number) col(row, "CNT")).longValue());

            Object ddl = client.executeQuery("CREATE TABLE NET_COMP_COPY (ID LONG PRIMARY KEY)");
            assertTrue(ddl instanceof String, "DDL answers with a status String, was: " + ddl);
            assertFalse(((String) ddl).startsWith(ErrorMessages.ERROR_PREFIX),
                    "the DDL must not be an error: " + ddl);
        } finally {
            client.disconnect();
        }
    }

    @Test
    void clientSendsHandshakeBeforeFirstQuery() {
        // The server refuses to serve anything before the handshake: a plain
        // query frame is rejected at decode time, so DatabaseClient must open
        // with COMPRESSION_HANDSHAKE. Connecting and running a query proves it.
        DatabaseClient client = new DatabaseClient("localhost", port, "admin", "test-admin-pw");
        try {
            client.connect();
            assertNotNull(client.executeQuery("SELECT COUNT(*) AS CNT FROM NET_COMP"));
        } finally {
            client.disconnect();
        }
    }

    /** ~119 chars of mostly repeated text: compressible, well above the threshold. */
    private static String payloadFor(int id) {
        return String.format("payload_%05d_", id)
                + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                + "xxxxxxxxxxxxxxxxxxxxxxxxxxx";
    }

    /** Case-insensitive column lookup (DieselDB echoes the alias casing). */
    private static Object col(Map<String, Object> row, String name) {
        Object exact = row.get(name);
        if (exact != null) {
            return exact;
        }
        Object upper = row.get(name.toUpperCase());
        if (upper != null) {
            return upper;
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
