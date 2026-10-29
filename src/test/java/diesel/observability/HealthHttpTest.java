package diesel.observability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #46: /health, /health/live and /health/ready over HTTP.
 * <p>
 * Acceptance: "curl http://localhost:9090/health returns UP/DOWN". Health
 * checks are built from suppliers so the DOWN path is exercised without a real
 * Database. Tagged {@code network}: real loopback sockets, ephemeral ports.
 */
@Tag("network")
public class HealthHttpTest {

    private MetricsHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    private static HealthCheck healthyCheck() {
        return new HealthCheck(
                new LivenessCheck(() -> true, true, () -> 50.0, 5.0),
                new ReadinessCheck(() -> false, () -> 5, 0),
                () -> 0, () -> 1024, () -> 0, System.nanoTime());
    }

    private int get(String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI
                .create("http://127.0.0.1:" + server.getPort() + path)
                .toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        int code = conn.getResponseCode();
        conn.disconnect();
        return code;
    }

    private String getBody(String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI
                .create("http://127.0.0.1:" + server.getPort() + path)
                .toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        int code = conn.getResponseCode();
        java.io.InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        StringBuilder sb = new StringBuilder();
        if (is != null) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    sb.append(line);
                }
            }
        }
        conn.disconnect();
        return sb.toString();
    }

    private void assertJsonStatus(String path, int expectedCode, String expectedStatus)
            throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI
                .create("http://127.0.0.1:" + server.getPort() + path)
                .toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        int code = conn.getResponseCode();
        assertEquals(expectedCode, code, path + " status code");
        assertEquals("application/json", conn.getHeaderField("Content-Type"));
        java.io.InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        StringBuilder sb = new StringBuilder();
        if (is != null) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    sb.append(line);
                }
            }
        }
        conn.disconnect();
        assertTrue(sb.toString().contains("\"status\":\"" + expectedStatus + "\""),
                path + " body must contain status " + expectedStatus + ": " + sb);
    }

    // ─── Acceptance: curl /health returns UP/DOWN ──────────────────

    @Test
    void getHealthReturnsUpWithRuntimeFigures() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0, healthyCheck());
        assertTrue(server.start(), "ephemeral bind must succeed");

        assertJsonStatus("/health", 200, "UP");
        String body = getBody("/health");
        assertTrue(body.contains("\"uptime\":"), "combined body must carry uptime: " + body);
        assertTrue(body.contains("\"activeTx\":"), "combined body must carry activeTx: " + body);
        assertTrue(body.contains("\"diskFree\":"), "combined body must carry diskFree: " + body);
        assertTrue(body.contains("\"walLag\":"), "combined body must carry walLag: " + body);
        assertTrue(body.contains("\"checks\":{\"live\":\"UP\",\"ready\":\"UP\"}"),
                "combined body must carry both sub-checks: " + body);
    }

    @Test
    void getHealthReturns503WhenDown() throws Exception {
        // Dead WAL writer → liveness DOWN → combined DOWN → HTTP 503.
        HealthCheck down = new HealthCheck(
                new LivenessCheck(() -> false, true, () -> 50.0, 5.0),
                new ReadinessCheck(() -> false, () -> 5, 0),
                () -> 0, () -> 1024, () -> 0, System.nanoTime());
        server = new MetricsHttpServer("127.0.0.1", 0, down);
        assertTrue(server.start());

        assertJsonStatus("/health", 503, "DOWN");
        String body = getBody("/health");
        assertTrue(body.contains("wal-writer-dead"),
                "DOWN body must carry the failure reason: " + body);
    }

    // ─── Liveness/readiness isolation ──────────────────────────────

    @Test
    void liveAndReadyAreIndependentViews() throws Exception {
        // Liveness DOWN (dead writer), readiness UP (recovery done, warm pool).
        HealthCheck mixed = new HealthCheck(
                new LivenessCheck(() -> false, true, () -> 50.0, 5.0),
                new ReadinessCheck(() -> false, () -> 5, 0),
                () -> 0, () -> 1024, () -> 0, System.nanoTime());
        server = new MetricsHttpServer("127.0.0.1", 0, mixed);
        assertTrue(server.start());

        assertJsonStatus("/health/live", 503, "DOWN");
        assertJsonStatus("/health/ready", 200, "UP");
        assertJsonStatus("/health", 503, "DOWN");
    }

    @Test
    void readinessDownDuringRecovery() throws Exception {
        // Acceptance: during recovery startup → /health/ready=DOWN.
        HealthCheck recovering = new HealthCheck(
                new LivenessCheck(() -> true, true, () -> 50.0, 5.0),
                new ReadinessCheck(() -> true, () -> 0, 0),
                () -> 0, () -> 1024, () -> 0, System.nanoTime());
        server = new MetricsHttpServer("127.0.0.1", 0, recovering);
        assertTrue(server.start());

        assertJsonStatus("/health/ready", 503, "DOWN");
        assertJsonStatus("/health/live", 200, "UP");
    }

    // ─── HTTP surface ──────────────────────────────────────────────

    @Test
    void postIsRejectedWith405() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0, healthyCheck());
        assertTrue(server.start());

        HttpURLConnection conn = (HttpURLConnection) URI
                .create("http://127.0.0.1:" + server.getPort() + "/health")
                .toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            out.write(new byte[0]);
        }
        assertEquals(405, conn.getResponseCode());
        assertEquals("GET", conn.getHeaderField("Allow"));
        conn.disconnect();
    }

    @Test
    void unknownHealthSubPathIs404() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0, healthyCheck());
        assertTrue(server.start());
        assertEquals(404, get("/health/unknown"));
    }

    @Test
    void metricsStillServedAlongsideHealth() throws Exception {
        server = new MetricsHttpServer("127.0.0.1", 0, healthyCheck());
        assertTrue(server.start());
        assertEquals(200, get("/metrics"));
        assertEquals(200, get("/health"));
        assertEquals(200, get("/health/live"));
        assertEquals(200, get("/health/ready"));
    }
}
