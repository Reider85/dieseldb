package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ServerConnectionLimitTest {
    private static final Logger LOGGER = Logger.getLogger(ServerConnectionLimitTest.class.getName());
    private DatabaseServer server;
    private Thread serverThread;
    private int port;
    private List<Socket> clientSockets = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        try (ServerSocket tempSocket = new ServerSocket(0)) {
            port = tempSocket.getLocalPort();
        }
        server = new DatabaseServer(port, 30000);
        serverThread = new Thread(() -> server.start(), "test-server");
        serverThread.start();
        waitForServerReady(port);
    }

    @AfterEach
    void tearDown() {
        for (Socket s : clientSockets) {
            try { s.close(); } catch (IOException ignored) { /* socket already closed */ }
        }
        clientSockets.clear();
        
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
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
        fail("Server did not start within timeout");
    }

    @Test
    @Timeout(60)
    void moreThanPoolPlusQueueConnectionsAreRejected() throws Exception {
        int poolSize = 100;
        int queueCapacity = 100;
        int maxAccepted = poolSize + queueCapacity;
        
        LOGGER.log(Level.INFO, "Opening " + maxAccepted + " idle connections (pool=" + poolSize + ", queue=" + queueCapacity + ")");
        
        for (int i = 0; i < maxAccepted; i++) {
            Socket client = new Socket("localhost", port);
            clientSockets.add(client);
            if (i % 20 == 0) {
                Thread.sleep(20);
            }
        }
        
        Thread.sleep(1000);
        LOGGER.log(Level.INFO, "All " + clientSockets.size() + " connections established, now testing rejection");
        
        int rejected = 0;
        int probeCount = 5;
        for (int i = 0; i < probeCount; i++) {
            Socket probe = null;
            try {
                probe = new Socket("localhost", port);
                probe.setSoTimeout(2000);
                
                ObjectOutputStream out = new ObjectOutputStream(probe.getOutputStream());
                out.writeObject(new QueryMessage("SET AUTOCOMMIT = ON", null));
                out.flush();
                
                ObjectInputStream in = new ObjectInputStream(probe.getInputStream());
                Object response = in.readObject();
                
                LOGGER.log(Level.INFO, "Probe " + i + " got response (unexpected): " + response);
            } catch (IOException | ClassNotFoundException e) {
                rejected++;
                LOGGER.log(Level.INFO, "Probe " + i + " rejected as expected: " + e.getClass().getSimpleName());
            } finally {
                if (probe != null) {
                    try { probe.close(); } catch (IOException ignored) { /* socket already closed */ }
                }
            }
        }
        
        LOGGER.log(Level.INFO, "Rejected " + rejected + " out of " + probeCount + " probe connections");
        assertTrue(rejected > 0, "At least some connections beyond pool+queue capacity should be rejected");
        assertEquals(maxAccepted, clientSockets.size(), "All initial connections should be accepted");
    }
}