package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Legacy v1 protocol compatibility (prompt4.md #29): a pre-v2 client speaking
 * plain Java Object Serialization must keep working on the dedicated legacy
 * port, the connection must be logged as deprecated, and compression must
 * follow the v1 handshake rules (GZIP above the agreed threshold).
 *
 * <p>The legacy port is ephemeral per test (never the fixed 5440) so parallel
 * suites cannot collide.
 */
@Tag("network")
public class LegacyCompatTest {

    /** Small table: two hand-written rows. */
    private static final int SMALL_ROWS = 2;

    /** Bulk table: large enough to cross the legacy GZIP threshold. */
    private static final int BULK_ROWS = 200;

    private Database database;
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private int legacyPort;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dieseldb-legacy-compat");
        port = freePort();
        legacyPort = freePort();
        database = new Database(tempDir.toString());
        server = new DatabaseServer(port, 15000, legacyPort, database);
        serverThread = new Thread(server::start, "legacy-compat-server");
        serverThread.start();
        waitForServer(port);
        waitForServer(legacyPort);

        database.executeQuery(
                "CREATE TABLE LEGACY_T (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery(
                "INSERT INTO LEGACY_T (ID, NAME) VALUES (1, 'first')", null);
        database.executeQuery(
                "INSERT INTO LEGACY_T (ID, NAME) VALUES (2, 'second')", null);

        database.executeQuery(
                "CREATE TABLE LEGACY_BULK (ID LONG PRIMARY KEY, NAME STRING)", null);
        List<String> inserts = new ArrayList<>(BULK_ROWS);
        for (int i = 0; i < BULK_ROWS; i++) {
            inserts.add("INSERT INTO LEGACY_BULK (ID, NAME) VALUES (" + i
                    + ", 'bulk-row-" + i + "')");
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
    void legacyClientExecutesQueriesOnLegacyPort() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new QueryMessage("SELECT ID, NAME FROM LEGACY_T WHERE ID = 1", null));
            Object first = client.readResult();
            assertTrue(first instanceof List, "a SELECT answers with a row list, was: " + first);
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) ((List<?>) first).get(0);
            assertEquals(1, ((Number) col(row, "ID")).intValue());
            assertEquals("first", col(row, "NAME"));

            client.send(new QueryMessage("SELECT NAME FROM LEGACY_T WHERE ID = 2", null));
            Object second = client.readResult();
            @SuppressWarnings("unchecked")
            Map<String, Object> secondRow = (Map<String, Object>) ((List<?>) second).get(0);
            assertEquals("second", col(secondRow, "NAME"));
        }
    }

    @Test
    void legacyConnectionIsLoggedAsDeprecated() throws Exception {
        Logger logger = Logger.getLogger("diesel.LegacyProtocolAdapter");
        List<LogRecord> captured = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                captured.add(record);
            }

            @Override
            public void flush() {
                // no-op
            }

            @Override
            public void close() {
                // no-op
            }
        };
        logger.addHandler(handler);
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new QueryMessage("SELECT ID FROM LEGACY_T WHERE ID = 1", null));
            assertTrue(client.readResult() instanceof List);
        } finally {
            logger.removeHandler(handler);
        }
        assertTrue(captured.stream().anyMatch(record -> record.getLevel() == Level.WARNING
                        && formatRecord(record).toLowerCase().contains("deprecated")),
                "the adapter must warn that the legacy protocol is deprecated, got "
                        + captured.size() + " record(s)");
    }

    @Test
    void legacyCompressionHandshakeAgreesOnGzip() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new CompressionHandshakeMessage(
                    true, List.of("GZIP", "ZSTD"), 6, 1024));
            Object response = client.readRaw();
            assertTrue(response instanceof CompressionHandshakeResponse,
                    "the handshake is answered with a bare v1 response, got: " + response);
            CompressionHandshakeResponse handshake = (CompressionHandshakeResponse) response;
            assertTrue(handshake.isCompressionEnabled());
            assertEquals("GZIP", handshake.getAgreedAlgorithm(),
                    "the v1 protocol only ever knew GZIP");
            assertEquals(6, handshake.getAgreedCompressionLevel());
            assertEquals(1024, handshake.getAgreedThresholdBytes(),
                    "a positive client threshold is honoured");

            // Below the agreed 1024-byte threshold: raw frame.
            client.send(new QueryMessage("SELECT ID FROM LEGACY_T WHERE ID = 1", null));
            assertTrue(client.readResult() instanceof List);
            assertEquals(0x00, client.lastMarker,
                    "a small result travels uncompressed in the v1 frame");

            // Above it: the same frame layout with the GZIP marker.
            client.send(new QueryMessage("SELECT ID, NAME FROM LEGACY_BULK", null));
            Object bulk = client.readResult();
            assertEquals(0x01, client.lastMarker,
                    "a result above the threshold travels GZIP compressed in the v1 frame");
            assertEquals(BULK_ROWS, ((List<?>) bulk).size());
        }
    }

    @Test
    void legacyPrepareExecuteCloseRoundTrip() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new PrepareMessage(
                    "SELECT NAME FROM LEGACY_T WHERE ID = ?", null));
            Object prepared = client.readRaw();
            assertTrue(prepared instanceof String, "prepare answers with a bare id String");
            String statementId = (String) prepared;

            client.send(new ExecutePreparedMessage(statementId, List.of(2), null));
            Object rows = client.readResult();
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) ((List<?>) rows).get(0);
            assertEquals("second", col(row, "NAME"));

            client.send(new ClosePreparedMessage(statementId, null));
            Object closed = client.readRaw();
            assertEquals("Prepared statement closed", closed,
                    "close-prepared answers with a bare status String in the v1 protocol");
        }
    }

    @Test
    void legacyBatchQueryReturnsBareResultList() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new BatchQueryMessage(List.of(
                    "INSERT INTO LEGACY_T (ID, NAME) VALUES (10, 'b1')",
                    "INSERT INTO LEGACY_T (ID, NAME) VALUES (11, 'b2')",
                    "INSERT INTO LEGACY_T (ID, NAME) VALUES (12, 'b3')"), null));
            Object batch = client.readRaw();
            assertTrue(batch instanceof List, "a batch answers with a bare List, was: " + batch);
            assertEquals(3, ((List<?>) batch).size());

            client.send(new QueryMessage(
                    "SELECT COUNT(*) AS CNT FROM LEGACY_T WHERE ID >= 10", null));
            Object count = client.readResult();
            @SuppressWarnings("unchecked")
            Map<String, Object> countRow = (Map<String, Object>) ((List<?>) count).get(0);
            assertEquals(3, ((Number) col(countRow, "CNT")).longValue());
        }
    }

    @Test
    void legacyErrorArrivesAsFramedErrorString() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new QueryMessage("INVALID SQL SYNTAX", null));
            Object result = client.readResult();
            assertTrue(result instanceof String, "an error is a String, was: " + result);
            assertTrue(((String) result).startsWith(ErrorMessages.ERROR_PREFIX),
                    "unexpected error payload: " + result);
        }
    }

    @Test
    void legacyCursorOpenFetchCloseRoundTrip() throws Exception {
        try (LegacyClient client = LegacyClient.open(legacyPort)) {
            client.send(new OpenCursorMessage(
                    "SELECT ID, NAME FROM LEGACY_BULK ORDER BY ID", 50, null));
            Object cursorIdValue = client.readResult();
            assertTrue(cursorIdValue instanceof String, "open-cursor answers with the cursor id");
            UUID cursorId = UUID.fromString((String) cursorIdValue);

            client.send(new FetchCursorMessage(cursorId, null));
            Object batch1 = client.readResult();
            assertEquals(50, ((List<?>) batch1).size());

            client.send(new FetchCursorMessage(cursorId, null));
            Object batch2 = client.readResult();
            assertEquals(50, ((List<?>) batch2).size());

            client.send(new CloseCursorMessage(cursorId, null));
            Object closed = client.readRaw();
            assertEquals("Cursor closed", closed,
                    "close-cursor answers with a bare status String in the v1 protocol");
        }
    }

    @Test
    void legacyExitClosesTheSession() throws Exception {
        LegacyClient client = LegacyClient.open(legacyPort);
        try {
            client.send(ErrorMessages.EXIT_COMMAND);
            // The adapter tears the session down on EXIT, so the next read sees
            // the peer's EOF instead of another framed answer.
            assertThrows(EOFException.class, client::readRaw,
                    "EXIT must close the legacy connection");
        } finally {
            client.close();
        }
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

    private static String formatRecord(LogRecord record) {
        if (record.getParameters() == null || record.getParameters().length == 0) {
            return String.valueOf(record.getMessage());
        }
        return java.text.MessageFormat.format(record.getMessage(), record.getParameters());
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

    /**
     * A pre-v2 client: plain {@link ObjectOutputStream} /
     * {@link ObjectInputStream} pair speaking the v1 framing rules
     * ({@code [marker][length][bytes]} for query results, bare objects for
     * prepare/close/EXIT answers).
     */
    private static final class LegacyClient implements AutoCloseable {
        final Socket socket;
        final ObjectOutputStream out;
        final ObjectInputStream in;

        /** Marker byte of the last framed result: 0x00 raw, 0x01 GZIP. */
        int lastMarker = -1;

        static LegacyClient open(int port) throws IOException {
            return new LegacyClient(new Socket("localhost", port));
        }

        private LegacyClient(Socket socket) throws IOException {
            this.socket = socket;
            this.socket.setSoTimeout(15000);
            this.out = new ObjectOutputStream(socket.getOutputStream());
            this.out.flush();
            this.in = new ObjectInputStream(socket.getInputStream());
        }

        void send(Object message) throws IOException {
            out.writeObject(message);
            out.flush();
        }

        Object readRaw() throws IOException, ClassNotFoundException {
            return in.readObject();
        }

        Object readResult() throws IOException, ClassNotFoundException {
            int marker = in.read();
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            lastMarker = marker;
            if (marker == 0x01) {
                try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(data));
                     ObjectInputStream ois = new ObjectInputStream(gzis)) {
                    return ois.readObject();
                }
            }
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                return ois.readObject();
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Best-effort teardown.
            }
        }
    }
}
