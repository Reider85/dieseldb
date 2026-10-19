package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("network")
public class OomHandlingTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.executeQuery("CREATE TABLE OOMT_LEFT (ID LONG PRIMARY KEY SEQUENCE(oomt_lseq 1 1), VAL INTEGER)", null);
        database.executeQuery("CREATE TABLE OOMT_RIGHT (ID LONG PRIMARY KEY SEQUENCE(oomt_rseq 1 1), VAL INTEGER)", null);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            try {
                database.dropTable("OOMT_LEFT");
            } catch (TableNotFoundException ignored) {
                // Ignore: table may not have been created
            }
            try {
                database.dropTable("OOMT_RIGHT");
            } catch (TableNotFoundException ignored) {
                // Ignore: table may not have been created
            }
            database.close();
            database = null;
        }
        SelectQuery.loadHashJoinConfig();
    }

    private void insertRecords(int count) {
        for (int i = 1; i <= count; i++) {
            database.executeQuery("INSERT INTO OOMT_LEFT (VAL) VALUES (" + i + ")", null);
            database.executeQuery("INSERT INTO OOMT_RIGHT (VAL) VALUES (" + i + ")", null);
        }
    }

    @Test
    void selectTracksPeakMemoryMetric() {
        insertRecords(100);
        database.executeQuery("SELECT OOMT_LEFT.ID, OOMT_RIGHT.ID FROM OOMT_LEFT CROSS JOIN OOMT_RIGHT", null);
        assertEquals(100 * 100L, SelectQuery.getLastQueryRowCount(),
                "the row-count metric must report the number of produced rows");
        assertTrue(SelectQuery.getLastQueryPeakMemoryBytes() > 0,
                "the peak memory metric must record heap usage above zero");
        long rowsAtPeak = SelectQuery.getLastQueryRowsAtPeak();
        assertTrue(rowsAtPeak > 0 && rowsAtPeak <= 100 * 100L,
                "the rows-at-peak metric must be between 1 and the total row count, was " + rowsAtPeak);
    }

    /** Database that fails every query with an OutOfMemoryError. */
    static class OomThrowingDatabase extends Database {
        @Override
        public Object executeQuery(String query, UUID transactionId) {
            throw new OutOfMemoryError("Java heap space");
        }
    }

    @Test
    void serverRespondsWithOomMessage() throws Exception {
        int port = freePort();
        DatabaseServer server = new DatabaseServer(port, 5000, new OomThrowingDatabase());
        Thread serverThread = new Thread(server::start, "oom-test-server");
        serverThread.start();
        try {
            waitForServer(port);
            Object response = roundTrip(port, "SELECT * FROM USERS");
            assertEquals("Error: Query exceeded memory limit. Consider adding LIMIT or indexes.", response,
                    "the client must receive the friendly OOM message instead of a dropped connection");
        } finally {
            server.stop();
            serverThread.interrupt();
        }
    }

    @Test
    void oomLogsQueryContext() throws Exception {
        Logger logger = Logger.getLogger("diesel.DatabaseServer");
        List<LogRecord> captured = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord logRecord) {
                captured.add(logRecord);
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
        try {
            int port = freePort();
            DatabaseServer server = new DatabaseServer(port, 5000, new OomThrowingDatabase());
            Thread serverThread = new Thread(server::start, "oom-log-test-server");
            serverThread.start();
            try {
                waitForServer(port);
                roundTrip(port, "SELECT * FROM USERS");
            } finally {
                server.stop();
                serverThread.interrupt();
            }
            assertTrue(captured.stream().anyMatch(record -> record.getLevel() == Level.SEVERE
                            && formatRecord(record).contains("OutOfMemoryError while executing query")
                            && formatRecord(record).contains("SELECT * FROM USERS")),
                    "the server must log the query text of the OOM");
            assertTrue(captured.stream().anyMatch(record -> record.getLevel() == Level.SEVERE
                            && record.getMessage() != null
                            && record.getMessage().contains("rows produced=")
                            && record.getMessage().contains("peak memory used=")),
                    "the server must log the row count and peak memory context of the OOM query");
        } finally {
            logger.removeHandler(handler);
        }
    }

    /** Database that throws OutOfMemoryError on executeCursor (cursor open). */
    static class OomCursorOpenDatabase extends Database {
        @Override
        public Cursor executeCursor(String query, int fetchSize, UUID transactionId) {
            throw new OutOfMemoryError("Java heap space");
        }
    }

    @Test
    void serverRespondsWithOomMessageOnCursorOpen() throws Exception {
        int port = freePort();
        DatabaseServer server = new DatabaseServer(port, 5000, new OomCursorOpenDatabase());
        Thread serverThread = new Thread(server::start, "oom-cursor-open-test-server");
        serverThread.start();
        try {
            waitForServer(port);
            try (WireProbe probe = new WireProbe(port, 10000)) {
                probe.handshake();
                probe.auth("admin", "admin");
                probe.write(new diesel.net.messages.OpenCursorMessage("SELECT * FROM USERS", 10, null));
                Object response = probe.readValue();
                assertEquals("Error: Query exceeded memory limit. Consider adding LIMIT or indexes.", response,
                        "the client must receive the friendly OOM message on cursor open");
            }
        } finally {
            server.stop();
            serverThread.interrupt();
        }
    }

    /** Database whose cursor fetch() throws OutOfMemoryError. */
    static class OomCursorFetchDatabase extends Database {
        @Override
        public Cursor executeCursor(String query, int fetchSize, UUID transactionId) {
            Iterator<Map<String, Object>> oomIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Map<String, Object> next() {
                    throw new OutOfMemoryError("Java heap space");
                }
            };
            return new Cursor(UUID.randomUUID(), query, fetchSize, oomIterator);
        }
    }

    @Test
    void serverRespondsWithOomMessageOnCursorFetch() throws Exception {
        int port = freePort();
        DatabaseServer server = new DatabaseServer(port, 5000, new OomCursorFetchDatabase());
        Thread serverThread = new Thread(server::start, "oom-cursor-fetch-test-server");
        serverThread.start();
        try {
            waitForServer(port);
            try (WireProbe probe = new WireProbe(port, 10000)) {
                probe.handshake();
                probe.auth("admin", "admin");
                probe.write(new diesel.net.messages.OpenCursorMessage("SELECT * FROM USERS", 10, null));
                Object cursorIdResponse = probe.readValue();
                assertTrue(cursorIdResponse instanceof String, "server should return a cursor id string");
                UUID cursorId = UUID.fromString((String) cursorIdResponse);

                probe.write(new diesel.net.messages.FetchCursorMessage(cursorId, null));
                Object fetchResponse = probe.readValue();
                assertEquals("Error: Query exceeded memory limit. Consider adding LIMIT or indexes.", fetchResponse,
                        "the client must receive the friendly OOM message on cursor fetch");
            }
        } finally {
            server.stop();
            serverThread.interrupt();
        }
    }

    private static String formatRecord(LogRecord logRecord) {
        if (logRecord.getParameters() == null || logRecord.getParameters().length == 0) {
            return logRecord.getMessage();
        }
        return java.text.MessageFormat.format(logRecord.getMessage(), logRecord.getParameters());
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

    /** Runs one query over the v2 wire protocol and returns the result value. */
    private static Object roundTrip(int port, String query) throws Exception {
        try (WireProbe probe = new WireProbe(port, 10000)) {
            probe.handshake();
            probe.auth("admin", "admin");
            return probe.query(query);
        }
    }
}
