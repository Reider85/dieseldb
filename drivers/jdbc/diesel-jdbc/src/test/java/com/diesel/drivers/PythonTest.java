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
 * Prompt 56 acceptance (PythonTest): the pure-Python driver in
 * {@code drivers/python/diesel-python} passes its unittest suite against an
 * in-process DieselDB libpq server.
 *
 * <p>Requires {@code python} (or {@code python3}/{@code py}) on the PATH -
 * skipped via JUnit assumptions otherwise. The port is passed through the
 * {@code DIESEL_PORT} environment variable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PythonTest {

    private static final long TEST_TIMEOUT_SECONDS = 180;

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

    private static String findPython() {
        String override = System.getenv("DIESEL_PYTHON");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        return DriverTestSupport.findExecutable("python", "python3", "py");
    }

    private Map<String, String> driverEnv(Path moduleDir) {
        Map<String, String> env = new HashMap<>();
        env.put("PYTHONPATH", moduleDir.toString());
        env.put("DIESEL_HOST", "localhost");
        env.put("DIESEL_PORT", String.valueOf(server.port()));
        env.put("DIESEL_USER", "testuser");
        env.put("DIESEL_PASSWORD", "testpass");
        env.put("DIESEL_DB", "testdb");
        return env;
    }

    @Test
    void pythonDriverPassesCrudSuite() throws Exception {
        String python = findPython();
        Assumptions.assumeTrue(python != null,
                "python not found on PATH - skipping Python driver suite");

        Path moduleDir = DriverTestSupport.findRepoRoot()
                .resolve("drivers").resolve("python").resolve("diesel-python");
        assertTrue(Files.isDirectory(moduleDir.resolve("diesel")),
                "Python driver package missing at " + moduleDir);

        DriverTestSupport.Result result = DriverTestSupport.run(
                moduleDir, driverEnv(moduleDir), TEST_TIMEOUT_SECONDS,
                python, "-m", "unittest", "discover", "-s", "tests", "-t", ".",
                "-v");

        assertTrue(!result.timedOut(),
                "Python driver test suite timed out after "
                        + TEST_TIMEOUT_SECONDS + "s. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 4000));
        assertEquals(0, result.exitCode(),
                "Python driver test suite failed. Output tail:\n"
                        + DriverTestSupport.tail(result.output(), 8000));
    }
}
