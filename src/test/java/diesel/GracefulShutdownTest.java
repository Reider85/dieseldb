package diesel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class GracefulShutdownTest {
    private static final Logger LOGGER = Logger.getLogger(GracefulShutdownTest.class.getName());

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void serverTerminatesCleanlyOnSigterm() throws Exception {
        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
        int port = freePort();
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (isWindows ? "java.exe" : "java");
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, "diesel.DatabaseServer", String.valueOf(port));
        pb.redirectErrorStream(true);
        Process process = pb.start();

        List<String> outputLines = new ArrayList<>();
        Thread outputPump = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (outputLines) {
                        outputLines.add(line);
                    }
                }
            } catch (IOException ignored) {
                // Process stream closed while the server is shutting down
            }
        }, "graceful-shutdown-output-pump");
        outputPump.setDaemon(true);
        outputPump.start();

        try {
            waitForServerReady(port, outputLines);
            LOGGER.log(Level.INFO, "Server process started on port {0}, opening a client connection", port);

            try (Socket client = new Socket("localhost", port)) {
                LOGGER.log(Level.INFO, "Client connected, sending SIGTERM via process.destroy()");
                process.destroy();

                assertTrue(process.waitFor(30, TimeUnit.SECONDS),
                        "Server process should terminate within 30 seconds after SIGTERM");

                if (isWindows) {
                    LOGGER.log(Level.INFO, "Windows: Process.destroy() is forceful and does not run JVM shutdown hooks; only termination is verified");
                } else {
                    assertEquals(0, process.exitValue(), "Server should exit with status 0 after graceful shutdown");
                }
            }
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
        outputPump.join(5000);

        if (!isWindows) {
            String log = String.join("\n", outputLines);
            assertTrue(log.contains("Database server stopped"),
                    "Shutdown hook should stop the server gracefully. Server output:\n" + log);
        }
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void waitForServerReady(int port, List<String> outputLines) {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("localhost", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        String log;
        synchronized (outputLines) {
            log = String.join("\n", outputLines);
        }
        fail("Server process did not start within timeout. Server output:\n" + log);
    }
}
