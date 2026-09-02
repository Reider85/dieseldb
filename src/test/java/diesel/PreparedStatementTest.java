package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for prompt 79 - Prepared Statements caching: parameter binding at
 * execution time, template-scoped LRU caching of the parsed AST, the Database
 * API, and the client/server wire protocol round-trip.
 */
public class PreparedStatementTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        dropTable();
        database.executeQuery("CREATE TABLE PS_TEST (ID LONG PRIMARY KEY SEQUENCE(ps_seq 1 1), NAME STRING, AGE INTEGER)", null);
        insert("alpha", 25);
        insert("beta", 30);
        insert("gamma", 25);
        insert("delta", 40);
    }

    @AfterEach
    void tearDown() {
        dropTable();
        PreparedStatement.resetGlobalCacheSize();
    }

    private void dropTable() {
        try {
            database.dropTable("PS_TEST");
        } catch (TableNotFoundException ignored) {
            // Ignore: table may not have been created.
        }
    }

    private void insert(String name, int age) {
        database.executeQuery("INSERT INTO PS_TEST (NAME, AGE) VALUES ('" + name + "', " + age + ")", null);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> rows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    @Test
    void preparedSelectWithWhereReturnsMatchingRows() {
        PreparedStatement ps = database.prepareStatement(
                "SELECT * FROM PS_TEST WHERE NAME = ?");
        ps.bindParameters("beta");
        List<Map<String, Object>> result = ps.executeQuery(database, null);
        assertEquals(1, result.size(), "placeholder must be bound to the bound NAME value");
        assertEquals("beta", result.get(0).get("NAME"));
    }

    @Test
    void bindParametersRebindsForEachExecution() {
        PreparedStatement ps = database.prepareStatement(
                "SELECT * FROM PS_TEST WHERE NAME = ?");
        ps.bindParameters("alpha");
        assertEquals("alpha", ps.executeQuery(database, null).get(0).get("NAME"));
        ps.bindParameters("delta");
        assertEquals("delta", ps.executeQuery(database, null).get(0).get("NAME"));
    }

    @Test
    void templateParsedOnceReusedAcrossBindingsInCache() {
        PreparedStatement ps = database.prepareStatement(
                "SELECT * FROM PS_TEST WHERE AGE = ?");
        // First execution parses.
        ps.bindParameters(25);
        assertEquals(2, ps.executeQuery(database, null).size());
        int sizeAfterFirst = ps.getCacheSize();
        assertTrue(sizeAfterFirst >= 1, "template binding must be cached after first execution");
        // Re-executing the same binding must hit the cache (no size growth).
        assertEquals(2, ps.executeQuery(database, null).size());
        assertEquals(sizeAfterFirst, ps.getCacheSize(),
                "an identical binding must reuse the cached parsed AST");
        // A different value is a different binding but stays within the template cache.
        ps.bindParameters(30);
        assertEquals(1, ps.executeQuery(database, null).size());
    }

    @Test
    void preparedInsertExecutesAndPersists() {
        PreparedStatement ps = database.prepareStatement(
                "INSERT INTO PS_TEST (NAME, AGE) VALUES (?, ?)");
        ps.bindParameters("epsilon", 50);
        Object result = ps.executeUpdate(database, null);
        assertNull(result, "INSERT returns null");
        Object count = database.executeQuery("SELECT COUNT(*) FROM PS_TEST", null);
        assertEquals(5L, rows(count).get(0).get("COUNT(*)"), "the prepared INSERT must add one row");
    }

    @Test
    void preparedInsertWithNullValue() {
        PreparedStatement ps = database.prepareStatement(
                "INSERT INTO PS_TEST (NAME, AGE) VALUES (?, ?)");
        ps.bindParameters("zeta", null);
        ps.executeUpdate(database, null);
        Object count = database.executeQuery("SELECT COUNT(*) FROM PS_TEST", null);
        assertEquals(5L, rows(count).get(0).get("COUNT(*)"));
    }

    @Test
    void preparedUpdateAffectsMatchingRows() {
        PreparedStatement ps = database.prepareStatement(
                "UPDATE PS_TEST SET AGE = ? WHERE NAME = ?");
        ps.bindParameters(99, "alpha");
        ps.executeUpdate(database, null);
        PreparedStatement read = database.prepareStatement(
                "SELECT AGE FROM PS_TEST WHERE NAME = ?");
        read.bindParameters("alpha");
        assertEquals(99, ((Number) read.executeQuery(database, null).get(0).get("AGE")).intValue(),
                "the prepared UPDATE must set the bound value");
    }

    @Test
    void executeQueryRejectsNonSelect() {
        PreparedStatement ps = database.prepareStatement(
                "INSERT INTO PS_TEST (NAME, AGE) VALUES (?, ?)");
        ps.bindParameters("eta", 1);
        boolean threw = false;
        try {
            ps.executeQuery(database, null);
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw, "executeQuery must reject a non-SELECT prepared statement");
    }

    @Test
    void cacheClearForgetsEntries() {
        PreparedStatement ps = database.prepareStatement(
                "SELECT * FROM PS_TEST WHERE AGE = ?");
        ps.bindParameters(25);
        ps.executeQuery(database, null);
        assertTrue(ps.getCacheSize() >= 1);
        ps.clearCache();
        assertEquals(0, ps.getCacheSize(), "clearCache must empty the statement cache");
    }

    @Test
    void multiplePlaceholdersInWhere() {
        PreparedStatement ps = database.prepareStatement(
                "SELECT * FROM PS_TEST WHERE AGE = ? AND NAME = ?");
        ps.bindParameters(25, "alpha");
        List<Map<String, Object>> result = ps.executeQuery(database, null);
        assertEquals(1, result.size());
        assertEquals("alpha", result.get(0).get("NAME"));
    }

    // ----- in-process (no wire) prepared SELECT helper for round-trip reuse -----
    private static class ServerHarness implements AutoCloseable {
        final int port;
        final DatabaseServer server;
        final Thread thread;

        ServerHarness(Database db) throws IOException {
            this.port = freePort();
            this.server = new DatabaseServer(port, 10000, db);
            this.thread = new Thread(server::start, "ps-test-server");
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
    void clientServerPrepareExecuteCloseRoundTrip() throws Exception {
        try (ServerHarness harness = new ServerHarness(database)) {
            DatabaseClient client = new DatabaseClient("localhost", harness.port);
            try {
                client.connect();
                client.executeQuery("INSERT INTO PS_TEST (NAME, AGE) VALUES ('server1', 11)");
                client.executeQuery("INSERT INTO PS_TEST (NAME, AGE) VALUES ('server2', 12)");

                String id = client.prepareStatement(
                        "SELECT * FROM PS_TEST WHERE NAME = ?");
                assertNotNull(id);

                Object result = client.executePrepared(id, "server1");
                assertEquals(1, rows(result).size());
                assertEquals("server1", rows(result).get(0).get("NAME"));

                // Reuse the same prepared statement with a different binding.
                Object result2 = client.executePrepared(id, List.of("server2"));
                assertEquals("server2", rows(result2).get(0).get("NAME"));

                // Prepared INSERT through the wire.
                String insertId = client.prepareStatement(
                        "INSERT INTO PS_TEST (NAME, AGE) VALUES (?, ?)");
                client.executePrepared(insertId, List.of("server3", 13));

                PreparedStatement q = database.prepareStatement(
                        "SELECT COUNT(*) FROM PS_TEST WHERE NAME = ?");
                q.bindParameters("server3");
                assertEquals(1L, rows(q.executeQuery(database, null)).get(0).get("COUNT(*)"),
                        "the wired prepared INSERT must persist into the shared database");

                client.closePrepared(id);
                client.closePrepared(insertId);
            } finally {
                client.disconnect();
            }
        }
    }

    @Test
    void clientServerExecutesUnknownStatementReturnsError() throws Exception {
        try (ServerHarness harness = new ServerHarness(database)) {
            DatabaseClient client = new DatabaseClient("localhost", harness.port);
            try {
                client.connect();
                boolean threw = false;
                try {
                    client.executePrepared("does-not-exist", List.of());
                } catch (DieselException e) {
                    threw = true;
                    assertTrue(e.getMessage().contains("Unknown prepared statement"));
                }
                assertTrue(threw, "executing an unknown statement id must raise a server error");
            } finally {
                client.disconnect();
            }
        }
    }

    // ----- helpers (mirror OomHandlingTest) -----
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
