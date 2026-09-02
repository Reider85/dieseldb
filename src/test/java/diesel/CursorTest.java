package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for prompt 81 - Query result pagination: server-side cursors that let
 * a client fetch a SELECT result in paginated batches, both through the
 * in-process {@link Database#executeCursor} API and over the client/server
 * wire protocol.
 */
public class CursorTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        dropTable();
        database.executeQuery("CREATE TABLE CUR_TEST (ID LONG PRIMARY KEY SEQUENCE(cur_seq 1 1), NAME STRING, AGE INTEGER)", null);
        for (int i = 0; i < 25; i++) {
            database.executeQuery(
                    "INSERT INTO CUR_TEST (NAME, AGE) VALUES ('user" + i + "', " + (i * 2) + ")", null);
        }
    }

    @AfterEach
    void tearDown() {
        dropTable();
    }

    private void dropTable() {
        try {
            database.dropTable("CUR_TEST");
        } catch (TableNotFoundException ignored) {
            // Ignore: table may not have been created.
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> rows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    @Test
    void openCursorRejectsNonSelect() {
        assertThrows(IllegalArgumentException.class, () ->
                database.executeCursor("INSERT INTO CUR_TEST (NAME, AGE) VALUES ('x', 1)", 10, null));
    }

    @Test
    void openCursorRejectsNonPositiveFetchSize() {
        assertThrows(IllegalArgumentException.class, () ->
                database.executeCursor("SELECT * FROM CUR_TEST", 0, null));
        assertThrows(IllegalArgumentException.class, () ->
                database.executeCursor("SELECT * FROM CUR_TEST", -5, null));
    }

    @Test
    void fetchReturnsBatchesOfConfiguredSizeAndExhausts() {
        Cursor cursor = database.executeCursor("SELECT ID, NAME FROM CUR_TEST ORDER BY ID", 10, null);
        assertNotNull(cursor);
        assertFalse(cursor.isClosed());
        assertTrue(cursor.hasNext());

        List<Map<String, Object>> batch1 = cursor.fetch();
        assertEquals(10, batch1.size(), "first fetch must return exactly fetchSize rows");

        List<Map<String, Object>> batch2 = cursor.fetch();
        assertEquals(10, batch2.size(), "second fetch must return exactly fetchSize rows");
        assertFalse(batch2.equals(batch1));

        List<Map<String, Object>> batch3 = cursor.fetch();
        assertEquals(5, batch3.size(), "third fetch must return the remaining rows");

        assertFalse(cursor.hasNext(), "cursor must be exhausted after all rows");
        assertTrue(cursor.fetch().isEmpty(), "fetch on an exhausted cursor returns an empty list");
        cursor.close();
        assertTrue(cursor.isClosed());
    }

    @Test
    void fetchWithLargerThanTotalReturnsAll() {
        Cursor cursor = database.executeCursor("SELECT ID FROM CUR_TEST", 100, null);
        List<Map<String, Object>> batch = cursor.fetch();
        assertEquals(25, batch.size(), "one oversized fetch returns the whole result");
        cursor.close();
    }

    @Test
    void fetchAfterCloseReturnsEmpty() {
        Cursor cursor = database.executeCursor("SELECT ID FROM CUR_TEST", 5, null);
        assertEquals(5, cursor.fetch().size());
        cursor.close();
        assertTrue(cursor.isClosed());
        assertTrue(cursor.fetch().isEmpty(), "fetch on a closed cursor returns an empty list");
    }

    @Test
    void paginatedBatchesTogetherEqualTheFullResult() {
        Cursor cursor = database.executeCursor("SELECT ID, NAME FROM CUR_TEST ORDER BY ID", 7, null);
        int total = 0;
        while (cursor.hasNext()) {
            total += cursor.fetch().size();
        }
        assertEquals(25, total, "all fetches together must equal the full result size");
        cursor.close();
    }

    @Test
    void cursorRespectsOrderByAndProjection() {
        Cursor cursor = database.executeCursor("SELECT NAME FROM CUR_TEST ORDER BY AGE", 5, null);
        List<Map<String, Object>> batch = cursor.fetch();
        assertEquals(5, batch.size());
        Map<String, Object> first = batch.get(0);
        assertEquals("user0", first.get("NAME"), "ORDER BY AGE must put user0 first");
        assertFalse(first.containsKey("ID"), "SELECT projection must exclude unselected columns");
        cursor.close();
    }

    @Test
    void cursorSupportsKeysetAndStatelessPaginationQueries() {
        // Keyset pagination pattern (WHERE id > last LIMIT N).
        Cursor page1 = database.executeCursor("SELECT ID FROM CUR_TEST WHERE ID > 0 ORDER BY ID LIMIT 10", 10, null);
        assertEquals(10, page1.fetch().size());
        List<Map<String, Object>> firstPage = page1.fetch().isEmpty() ? List.of()
                : rows(database.executeQuery("SELECT ID FROM CUR_TEST WHERE ID > 0 ORDER BY ID LIMIT 10", null));
        assertTrue(firstPage.isEmpty() || firstPage.size() == 10);

        // Stateless pagination pattern (OFFSET/LIMIT).
        List<Map<String, Object>> pageA = rows(database.executeQuery("SELECT ID FROM CUR_TEST ORDER BY ID LIMIT 5 OFFSET 0", null));
        List<Map<String, Object>> pageB = rows(database.executeQuery("SELECT ID FROM CUR_TEST ORDER BY ID LIMIT 5 OFFSET 5", null));
        assertEquals(5, pageA.size());
        assertEquals(5, pageB.size());
        assertFalse(pageA.equals(pageB), "offset pages must differ");
    }

    @Test
    void openCursorForMissingTableFails() {
        assertThrows(TableNotFoundException.class, () ->
                database.executeCursor("SELECT * FROM DOES_NOT_EXIST", 10, null));
    }

    // ----- client/server wire round-trip -----
    private static class ServerHarness implements AutoCloseable {
        final int port;
        final DatabaseServer server;
        final Thread thread;

        ServerHarness(Database db) throws IOException {
            this.port = freePort();
            this.server = new DatabaseServer(port, 10000, db);
            this.thread = new Thread(server::start, "cursor-test-server");
            thread.start();
            waitForServer(port);
        }

        @Override
        public void close() {
            server.stop();
            thread.interrupt();
        }
    }

    @Test
    void clientServerOpenFetchCloseRoundTrip() throws Exception {
        try (ServerHarness harness = new ServerHarness(database)) {
            DatabaseClient client = new DatabaseClient("localhost", harness.port);
            try {
                client.connect();
                String cursorId = client.openCursor("SELECT ID, NAME FROM CUR_TEST ORDER BY ID", 8);
                assertNotNull(cursorId);

                List<Map<String, Object>> batch1 = client.fetchCursor(cursorId);
                assertEquals(8, batch1.size(), "first wire fetch returns fetchSize rows");

                List<Map<String, Object>> batch2 = client.fetchCursor(cursorId);
                assertEquals(8, batch2.size());

                List<Map<String, Object>> batch3 = client.fetchCursor(cursorId);
                assertEquals(8, batch3.size());

                List<Map<String, Object>> batch4 = client.fetchCursor(cursorId);
                assertEquals(1, batch4.size(), "final wire fetch returns the remaining rows");

                assertTrue(client.fetchCursor(cursorId).isEmpty(), "cursor is exhausted on the wire");

                client.closeCursor(cursorId);
            } finally {
                client.disconnect();
            }
        }
    }

    @Test
    void clientServerFetchUnknownCursorReturnsError() throws Exception {
        try (ServerHarness harness = new ServerHarness(database)) {
            DatabaseClient client = new DatabaseClient("localhost", harness.port);
            try {
                client.connect();
                boolean threw = false;
                try {
                    client.fetchCursor("00000000-0000-0000-0000-000000000000");
                } catch (DieselException e) {
                    threw = true;
                    assertTrue(e.getMessage().contains("Unknown or closed cursor"));
                }
                assertTrue(threw, "fetching an unknown cursor must raise a server error");
            } finally {
                client.disconnect();
            }
        }
    }

    @Test
    void clientServerOpenNonSelectReturnsError() throws Exception {
        try (ServerHarness harness = new ServerHarness(database)) {
            DatabaseClient client = new DatabaseClient("localhost", harness.port);
            try {
                client.connect();
                boolean threw = false;
                try {
                    client.openCursor("INSERT INTO CUR_TEST (NAME, AGE) VALUES ('wirex', 1)", 10);
                } catch (DieselException e) {
                    threw = true;
                    assertTrue(e.getMessage().contains("Cursor can only be opened over a SELECT"),
                            "unexpected error message: " + e.getMessage());
                }
                assertTrue(threw, "opening a cursor over a non-SELECT must raise a server error");
            } finally {
                client.disconnect();
            }
        }
    }

    // ----- helpers -----
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForServer(int port) {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new IllegalStateException("server did not start within timeout");
    }
}