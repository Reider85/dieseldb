package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SocketTimeoutTest {
    private static final Logger LOGGER = Logger.getLogger(SocketTimeoutTest.class.getName());
    private static final int SOCKET_TIMEOUT_MS = 3000;

    private DatabaseServer server;
    private Thread serverThread;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            port = tempSocket.getLocalPort();
        }
        server = new DatabaseServer(port, SOCKET_TIMEOUT_MS);
        serverThread = new Thread(() -> server.start(), "test-server");
        serverThread.start();
        waitForServerReady(port);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

    private void waitForServerReady(int port) {
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        fail("Server did not start within timeout");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void idleConnectionIsClosedAfterSocketTimeout() throws Exception {
        long start = System.currentTimeMillis();
        try (Socket client = new Socket("localhost", port)) {
            // Client read timeout longer than the server one, so the client observes
            // the server closing the connection instead of timing out itself.
            client.setSoTimeout(SOCKET_TIMEOUT_MS + 5000);

            ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
            out.writeObject(new QueryMessage("SET AUTOCOMMIT = ON", null));
            out.flush();

            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            int marker = in.read();
            int dataLength = in.readInt();
            byte[] data = new byte[dataLength];
            in.readFully(data);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
            Object response = ois.readObject();
            LOGGER.log(Level.INFO, "Received response to round-trip query: " + response);
            assertFalse(response instanceof String && ((String) response).startsWith("Error:"),
                    "Round-trip query should be answered without an error");

            // The server blocks in in.readObject() with a socket timeout. After
            // SOCKET_TIMEOUT_MS of idle time the server times out, breaks the loop
            // and closes the connection, which makes this read fail with IOException.
            assertThrows(IOException.class, in::readObject,
                    "Idle connection should be closed by the server after the socket timeout");
        }
        long elapsed = System.currentTimeMillis() - start;
        LOGGER.log(Level.INFO, "Socket timeout fired after {0} ms (configured {1} ms)",
                new Object[]{elapsed, SOCKET_TIMEOUT_MS});
        assertTrue(elapsed >= SOCKET_TIMEOUT_MS, "Timeout should not fire before the configured socket timeout");
    }
}
