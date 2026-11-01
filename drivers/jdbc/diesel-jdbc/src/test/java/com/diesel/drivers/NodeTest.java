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
 * Prompt 56 acceptance (NodeTest): the TypeScript driver in
 * {@code drivers/nodejs/diesel-node} passes its node:test suite against an
 * in-process DieselDB libpq server.
 *
 * <p>Requires {@code node} and {@code npm} on the PATH - skipped via JUnit
 * assumptions otherwise. Installs the TypeScript devDependency on first run
 * ({@code npm install}) and executes {@code npm test} (build + node --test).
 * The port is passed through the {@code DIESEL_PORT} environment variable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NodeTest {

    private static final long INSTALL_TIMEOUT_SECONDS = 300;
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
    void nodeDriverPassesCrudSuite() throws Exception {
        String node = DriverTestSupport.findExecutable("node");
        Assumptions.assumeTrue(node != null,
                "node not found on PATH - skipping Node driver suite");
        String npm = DriverTestSupport.findExecutable(
                DriverTestSupport.npmCommand());
        Assumptions.assumeTrue(npm != null,
                "npm not found on PATH - skipping Node driver suite");

        Path moduleDir = DriverTestSupport.findRepoRoot()
                .resolve("drivers").resolve("nodejs").resolve("diesel-node");
        assertTrue(Files.isDirectory(moduleDir.resolve("src")),
                "Node driver sources missing at " + moduleDir);

        if (!Files.isDirectory(moduleDir.resolve("node_modules")
                .resolve("typescript"))) {
            DriverTestSupport.Result install = DriverTestSupport.run(
                    moduleDir, driverEnv(), INSTALL_TIMEOUT_SECONDS,
                    npm, "install");
            assertTrue(!install.timedOut(),
                    "npm install timed out. Output tail:\n"
                            + DriverTestSupport.tail(install.output(), 4000));
            assertEquals(0, install.exitCode(),
                    "npm install failed. Output tail:\n"
                            + DriverTestSupport.tail(install.output(), 8000));
        }

        DriverTestSupport.Result result = DriverTestSupport.run(
                moduleDir, driverEnv(), TEST_TIMEOUT_SECONDS, npm, "test");

        assertTrue(!result.timedOut(),
                "Node driver test suite timed out after "
                        + TEST_TIMEOUT_SECONDS + "s. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 4000));
        assertEquals(0, result.exitCode(),
                "Node driver test suite failed. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 8000));
    }
}
