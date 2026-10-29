package diesel.observability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #45: MetricsHttpServer — the /metrics HTTP endpoint
 * (acceptance: a curl-style GET returns Prometheus text with # HELP / # TYPE).
 * <p>
 * Tagged {@code network}: real loopback sockets. Every test binds an ephemeral
 * port ({@code port=0}) so parallel runs never collide, except the deliberate
 * bind-conflict test.
 */
@Tag("network")
public class MetricsHttpServerTest {

    private MetricsHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    // ─── Acceptance: curl :9090/metrics ─────────────────────────────

    @Test
    void getMetricsReturnsPrometheusTextWithHelpAndType() throws Exception {
        String metric = "diesel_test_httpserver_queries";
        MetricsRegistry.getInstance().counter(metric, "type", "SELECT").add(42);

        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start(), "ephemeral bind must succeed");
        assertNotEquals(-1, server.getPort());

        HttpReply reply = get(server.getPort(), "/metrics");

        assertEquals(200, reply.status, reply.body);
        assertTrue(reply.contentType.startsWith("text/plain; version=0.0.4"),
                "unexpected Content-Type: " + reply.contentType);
        assertTrue(reply.body.contains("# HELP " + metric + " "), reply.body);
        assertTrue(reply.body.contains("# TYPE " + metric + " counter"), reply.body);
        assertTrue(reply.body.contains(metric + "{type=\"SELECT\"} 42"), reply.body);
        assertTrue(server.getHandledRequests() >= 1);
    }

    // ─── Protocol behaviour ─────────────────────────────────────────

    @Test
    void unknownPathReturns404() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start());

        HttpReply reply = get(server.getPort(), "/health");

        assertEquals(404, reply.status, reply.body);
    }

    @Test
    void subPathOfMetricsReturns404() throws Exception {
        // The JDK server routes by context prefix: /metrics/extra lands in our
        // handler and must be rejected, not served as valid exposition.
        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start());

        HttpReply reply = get(server.getPort(), "/metrics/extra");

        assertEquals(404, reply.status, reply.body);
    }

    @Test
    void nonGetReturns405WithAllowHeader() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start());

        HttpURLConnection conn = open(server.getPort(), "/metrics");
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            out.write(new byte[0]);
        }
        assertEquals(405, conn.getResponseCode());
        assertEquals("GET", conn.getHeaderField("Allow"));
        conn.disconnect();
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    @Test
    void stopReleasesThePort() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start());
        int port = server.getPort();
        // Warm it up once so we know the port was genuinely live.
        assertEquals(200, get(port, "/metrics").status);

        server.stop();
        assertFalse(server.isRunning());
        assertEquals(-1, server.getPort());
        // The port must be immediately reusable — DatabaseServer restarts in
        // tests and a lingering 9090 would break the next run.
        try (ServerSocket probe = new ServerSocket()) {
            probe.bind(new java.net.InetSocketAddress("127.0.0.1", port));
            assertEquals(port, probe.getLocalPort());
        }
        // stop() is idempotent.
        server.stop();
    }

    @Test
    void startIsIdempotentAndDoubleStopSafe() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0);
        assertTrue(server.start());
        int port = server.getPort();
        assertTrue(server.start(), "second start() must be a no-op");
        assertEquals(port, server.getPort(), "second start() must not rebind");
        server.stop();
        server.stop();
    }

    @Test
    void bindConflictIsNotFatalAndReturnsFalse() throws Exception {
        // Occupy a port, then ask the metrics server for it: it must decline
        // gracefully — observability must never keep the DB from starting.
        try (ServerSocket blocker = new ServerSocket()) {
            blocker.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
            int busyPort = blocker.getLocalPort();

            MetricsHttpServer conflicted = new MetricsHttpServer("127.0.0.1", busyPort);
            assertFalse(conflicted.start(), "bind on a busy port must fail");
            assertFalse(conflicted.isRunning());
            assertEquals(-1, conflicted.getPort());
            conflicted.stop(); // no-op, must not throw
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static HttpURLConnection open(int port, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection)
                URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);
        return conn;
    }

    private static HttpReply get(int port, String path) throws IOException {
        HttpURLConnection conn = open(port, path);
        conn.setRequestMethod("GET");
        int status = conn.getResponseCode();
        String contentType = conn.getContentType() == null ? "" : conn.getContentType();
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                status < 400 ? conn.getInputStream() : conn.getErrorStream(),
                StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line).append('\n');
            }
        } finally {
            conn.disconnect();
        }
        return new HttpReply(status, contentType, body.toString());
    }

    private static final class HttpReply {
        final int status;
        final String contentType;
        final String body;

        HttpReply(int status, String contentType, String body) {
            this.status = status;
            this.contentType = contentType;
            this.body = body;
        }
    }
}
