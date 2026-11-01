package com.diesel.drivers;

import diesel.LibpqTestServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 57 acceptance (GoTest): the Go driver in
 * {@code drivers/go/diesel-go} passes its {@code go test} suite against an
 * in-process DieselDB libpq server.
 *
 * <p>Requires {@code go} on the PATH - skipped via JUnit assumptions
 * otherwise. Executes {@code go test ./...} with the port passed through the
 * {@code DIESEL_PORT} environment variable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GoTest {

    private static final long TEST_TIMEOUT_SECONDS = 300;

    @TempDir
    static Path tempDir;

    private LibpqTestServer server;

    @BeforeAll
    void startServer() throws Exception {
        server = LibpqTestServer.start(tempDir);
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    private Map<String, String> driverEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("DIESEL_HOST", "localhost");
        env.put("DIESEL_PORT", String.valueOf(server.port()));
        env.put("DIESEL_USER", "testuser");
        env.put("DIESEL_PASSWORD", "testpass");
        env.put("DIESEL_DB", "testdb");
        return env;
    }

    @Test
    void goDriverPassesCrudSuite() throws Exception {
        String go = DriverTestSupport.findExecutableProbing("version", "go");
        Assumptions.assumeTrue(go != null,
                "go not found on PATH - skipping Go driver suite");

        Path moduleDir = DriverTestSupport.findRepoRoot()
                .resolve("drivers").resolve("go").resolve("diesel-go");
        assertTrue(Files.isRegularFile(moduleDir.resolve("go.mod")),
                "Go driver go.mod missing at " + moduleDir);

        DriverTestSupport.Result result = DriverTestSupport.run(
                moduleDir, driverEnv(), TEST_TIMEOUT_SECONDS,
                go, "test", "./...");

        assertTrue(!result.timedOut(),
                "Go driver test suite timed out after "
                        + TEST_TIMEOUT_SECONDS + "s. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 4000));
        assertEquals(0, result.exitCode(),
                "Go driver test suite failed. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 8000));
    }
}
