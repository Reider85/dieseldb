package diesel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

/**
 * Test-only bootstrap helper for the diesel-jdbc module: starts an in-process
 * {@link LibpqServer} against a private data directory.
 *
 * <p>Lives in package {@code diesel} on purpose - {@code Database} is
 * package-private, so only same-package code (same classloader at test time)
 * can construct it.
 */
public final class LibpqTestServer implements AutoCloseable {

    private final Database database;
    private final LibpqServer server;
    private final int port;

    private LibpqTestServer(Database database, LibpqServer server, int port) {
        this.database = database;
        this.server = server;
        this.port = port;
    }

    /**
     * Starts a server: port 5432 when free (acceptance URL), otherwise an
     * ephemeral port. Auth method "password" (server accepts any credentials).
     */
    public static LibpqTestServer start(Path dataDir) throws Exception {
        int port;
        try (ServerSocket probe = new ServerSocket()) {
            probe.setReuseAddress(true);
            probe.bind(new InetSocketAddress("localhost", 5432));
            port = 5432;
        } catch (Exception portTaken) {
            try (ServerSocket ephemeral = new ServerSocket(0)) {
                port = ephemeral.getLocalPort();
            }
        }

        Database database = new Database(dataDir.resolve("libpq-test-db").toString());
        LibpqServer server = new LibpqServer(database, port, "password");
        Thread starter = new Thread(() -> {
            try {
                server.start();
            } catch (Exception ignore) {
                // stop() closes the accept loop; bind failures surface via awaitListening
            }
        }, "libpq-test-server");
        starter.setDaemon(true);
        starter.start();
        awaitListening(port);
        return new LibpqTestServer(database, server, port);
    }

    public int port() {
        return port;
    }

    public String jdbcUrl() {
        return "jdbc:diesel://localhost:" + port + "/testdb";
    }

    private static void awaitListening(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("localhost", port), 200);
                return;
            } catch (Exception retry) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("LibpqServer did not listen on port " + port);
    }

    @Override
    public void close() {
        server.stop();
        // Database.close() hard rule: stop the persist-flusher daemon or
        // @TempDir cleanup fails on Windows.
        database.close();
    }
}
