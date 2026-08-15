package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        try {
            database.dropTable("OOMT_LEFT");
        } catch (IllegalArgumentException ignored) {
        }
        try {
            database.dropTable("OOMT_RIGHT");
        } catch (IllegalArgumentException ignored) {
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
            Object response = roundTrip(port, new QueryMessage("SELECT * FROM USERS", null));
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
            public void publish(LogRecord record) {
                captured.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
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
                roundTrip(port, new QueryMessage("SELECT * FROM USERS", null));
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

    private static String formatRecord(LogRecord record) {
        if (record.getParameters() == null || record.getParameters().length == 0) {
            return record.getMessage();
        }
        return java.text.MessageFormat.format(record.getMessage(), record.getParameters());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForServer(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("server did not start within timeout");
    }

    private static Object roundTrip(int port, QueryMessage message) throws Exception {
        try (Socket client = new Socket("localhost", port)) {
            client.setSoTimeout(10000);
            ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
            out.writeObject(message);
            out.flush();
            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            return in.readObject();
        }
    }
}
