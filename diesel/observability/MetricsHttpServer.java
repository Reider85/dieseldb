package diesel.observability;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimal HTTP server exposing the {@link MetricsRegistry} at
 * {@code GET /metrics} in the Prometheus text exposition format
 * (prompt4 #45, R3-012 step 2/3).
 *
 * <p>Uses the JDK's built-in {@link HttpServer} (module {@code jdk.httpserver},
 * part of the JDK — not an external dependency, keeping with the project's
 * minimal-dependency style) instead of hand-rolling HTTP/1.1 over a
 * {@code ServerSocket}: the exposition endpoint benefits from correct
 * keep-alive, header and 404 handling for free.
 *
 * <p><b>Failure semantics.</b> A bind failure (port already taken) is logged
 * as a WARNING and leaves the server stopped — metrics must never abort
 * database startup. {@link #stop()} is idempotent, so {@code DatabaseServer}
 * can call it unconditionally.
 */
public final class MetricsHttpServer {

    private static final Logger LOGGER = Logger.getLogger(MetricsHttpServer.class.getName());

    /** The Prometheus scrape endpoint this server serves. */
    public static final String METRICS_PATH = "/metrics";

    private final String host;
    private final int port;
    private final HealthCheck healthCheck;
    private final AtomicInteger handledRequests = new AtomicInteger();

    private volatile HttpServer server;
    private volatile boolean running;

    /**
     * Creates a metrics-only server bound to the given host and port.
     *
     * @param host the bind address, e.g. {@code 0.0.0.0} to accept remote scrapes
     * @param port the TCP port; {@code 0} binds an ephemeral port (tests)
     */
    public MetricsHttpServer(String host, int port) {
        this(host, port, null);
    }

    /**
     * Creates a server exposing {@code /metrics} and, when a health check is
     * supplied, the {@code /health}, {@code /health/live} and
     * {@code /health/ready} endpoints (prompt4 #46).
     *
     * @param host        the bind address, e.g. {@code 0.0.0.0}
     * @param port        the TCP port; {@code 0} binds an ephemeral port (tests)
     * @param healthCheck the health orchestrator, or null to serve metrics only
     */
    public MetricsHttpServer(String host, int port, HealthCheck healthCheck) {
        this.host = host == null || host.isEmpty() ? "0.0.0.0" : host;
        this.port = port;
        this.healthCheck = healthCheck;
    }

    /**
     * Binds and starts serving {@code /metrics}. Non-blocking.
     *
     * @return true when the server is up; false when the bind failed (already
     *         logged — callers treat this as non-fatal)
     */
    public synchronized boolean start() {
        if (running) {
            return true;
        }
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(host, port), 0);
            http.createContext(METRICS_PATH, this::handle);
            if (healthCheck != null) {
                // Prefix context: /health also captures /health/live and
                // /health/ready (longest-prefix match in the JDK server).
                http.createContext(HealthCheck.HEALTH_PATH, this::handleHealth);
            }
            // Single daemon thread: scrapes are infrequent (default every 15s)
            // and the exporter is lock-free, so a pool would be pure overhead.
            // Daemon so an unclosed server can never block JVM exit.
            AtomicInteger threadNo = new AtomicInteger();
            http.setExecutor(Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable,
                        "diesel-metrics-http-" + threadNo.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }));
            http.start();
            server = http;
            running = true;
            LOGGER.log(Level.INFO, "Metrics endpoint listening on http://{0}:{1}{2}",
                    new Object[]{host, getPort(), METRICS_PATH});
            return true;
        } catch (IOException e) {
            // Non-fatal by design: an unavailable metrics port must not keep
            // the database from starting (the endpoint is observability-only).
            LOGGER.log(Level.WARNING,
                    "Metrics endpoint could not bind {0}:{1}, continuing without /metrics: {2}",
                    new Object[]{host, port, e.getMessage()});
            return false;
        }
    }

    /**
     * Stops the server and releases the port. Idempotent.
     */
    public synchronized void stop() {
        HttpServer local = server;
        if (local == null) {
            return;
        }
        running = false;
        server = null;
        local.stop(0);
        LOGGER.log(Level.INFO, "Metrics endpoint stopped after {0} request(s)",
                handledRequests.get());
    }

    /**
     * Returns the actually bound port (useful after binding ephemeral port 0),
     * or {@code -1} when the server is not running.
     *
     * @return the bound port, or -1
     */
    public int getPort() {
        HttpServer local = server;
        return local == null ? -1 : local.getAddress().getPort();
    }

    /**
     * Returns whether the server is currently accepting requests.
     *
     * @return true when running
     */
    public boolean isRunning() {
        return running;
    }

    /** Returns the number of HTTP requests served since construction. */
    public int getHandledRequests() {
        return handledRequests.get();
    }

    // ─── Request handling ───────────────────────────────────────────

    private void handle(HttpExchange exchange) throws IOException {
        try {
            handledRequests.incrementAndGet();
            String method = exchange.getRequestMethod();
            if (!"GET".equalsIgnoreCase(method)) {
                exchange.getResponseHeaders().set("Allow", "GET");
                write(exchange, 405, "Method Not Allowed\n", PrometheusExporter.CONTENT_TYPE);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (!METRICS_PATH.equals(path)) {
                // The JDK server routes by context prefix, so /metrics/xyz
                // lands here too — answer 404 explicitly.
                write(exchange, 404, "Not Found\n", PrometheusExporter.CONTENT_TYPE);
                return;
            }
            write(exchange, 200, PrometheusExporter.render(), PrometheusExporter.CONTENT_TYPE);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to render /metrics", e);
            write(exchange, 500, "Internal Server Error\n", PrometheusExporter.CONTENT_TYPE);
        } finally {
            exchange.close();
        }
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        try {
            handledRequests.incrementAndGet();
            String method = exchange.getRequestMethod();
            if (!"GET".equalsIgnoreCase(method)) {
                exchange.getResponseHeaders().set("Allow", "GET");
                write(exchange, 405, "Method Not Allowed\n", HealthCheck.CONTENT_TYPE);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            HealthResult result;
            switch (path) {
                case HealthCheck.HEALTH_PATH -> result = healthCheck.combined();
                case HealthCheck.LIVE_PATH -> result = healthCheck.live();
                case HealthCheck.READY_PATH -> result = healthCheck.ready();
                default -> {
                    // The JDK server routes by context prefix, so /health/xyz
                    // lands here too — answer 404 explicitly.
                    write(exchange, 404, "Not Found\n", HealthCheck.CONTENT_TYPE);
                    return;
                }
            }
            // 200 when UP, 503 when DOWN: Kubernetes native probes consume the
            // HTTP status code directly; the JSON body carries diagnostic detail.
            write(exchange, result.up() ? 200 : 503, result.body(), HealthCheck.CONTENT_TYPE);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to render health endpoint", e);
            write(exchange, 500, "Internal Server Error\n", HealthCheck.CONTENT_TYPE);
        } finally {
            exchange.close();
        }
    }

    private static void write(HttpExchange exchange, int status, String body, String contentType)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
