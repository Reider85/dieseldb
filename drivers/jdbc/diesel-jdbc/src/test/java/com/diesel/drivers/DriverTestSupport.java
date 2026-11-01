package com.diesel.drivers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared helpers for the Python/Node driver acceptance tests: locating the
 * repository root and running language-toolchain subprocesses with captured
 * output and hard timeouts (anti-hang guard).
 */
final class DriverTestSupport {

    private DriverTestSupport() {
    }

    /** Result of a subprocess run. */
    record Result(int exitCode, String output, boolean timedOut) {
    }

    /**
     * Walks up from the working directory (the diesel-jdbc module basedir
     * under surefire) looking for {@code drivers/python/diesel-python}.
     */
    static Path findRepoRoot() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        for (int i = 0; i < 8 && dir != null; i++) {
            if (Files.isDirectory(dir.resolve("drivers")
                    .resolve("python").resolve("diesel-python"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException(
                "Repository root not found above " + System.getProperty("user.dir"));
    }

    /**
     * Returns the first candidate executable that answers {@code --version}
     * with exit code 0 within 10 seconds, or {@code null} if none does.
     */
    static String findExecutable(String... candidates) {
        return findExecutableProbing("--version", candidates);
    }

    /**
     * Like {@link #findExecutable} but probes with an arbitrary subcommand:
     * Go answers {@code go version} and exits 2 on {@code go --version}.
     */
    static String findExecutableProbing(String probe, String... candidates) {
        for (String candidate : candidates) {
            try {
                ProcessBuilder pb = new ProcessBuilder(candidate, probe);
                pb.redirectErrorStream(true);
                Process proc = pb.start();
                drain(proc.getInputStream());
                if (proc.waitFor(10, TimeUnit.SECONDS) && proc.exitValue() == 0) {
                    return candidate;
                }
                proc.destroyForcibly();
            } catch (IOException notFound) {
                // try next candidate
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    /**
     * Runs a command, collecting merged stdout/stderr. Fails fast on timeout
     * (kills the process tree) - never leaves a subprocess hanging.
     */
    static Result run(Path cwd, Map<String, String> extraEnv,
                      long timeoutSeconds, String... command)
            throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(cwd.toFile());
        pb.redirectErrorStream(true);
        if (extraEnv != null) {
            pb.environment().putAll(extraEnv);
        }
        Process proc = pb.start();

        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        Thread reader = new Thread(() -> {
            try (InputStream in = proc.getInputStream()) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) >= 0) {
                    synchronized (collected) {
                        collected.write(buf, 0, n);
                    }
                }
            } catch (IOException ignore) {
                // stream closed when the process died
            }
        }, "driver-test-subprocess-out");
        reader.setDaemon(true);
        reader.start();

        boolean finished = proc.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            reader.join(5_000);
            String output = collected.toString(StandardCharsets.UTF_8);
            return new Result(-1, output, true);
        }
        reader.join(10_000);
        String output = collected.toString(StandardCharsets.UTF_8);
        return new Result(proc.exitValue(), output, false);
    }

    private static void drain(InputStream in) {
        Thread t = new Thread(() -> {
            try (InputStream stream = in) {
                byte[] buf = new byte[1024];
                while (stream.read(buf) >= 0) {
                    // discard
                }
            } catch (IOException ignore) {
                // process exited
            }
        }, "driver-test-version-probe");
        t.setDaemon(true);
        t.start();
    }

    /** Windows needs {@code npm.cmd}; POSIX uses {@code npm}. */
    static String npmCommand() {
        return isWindows() ? "npm.cmd" : "npm";
    }

    static boolean isWindows() {
        return System.getProperty("os.name", "")
                .toLowerCase(Locale.ROOT).contains("win");
    }

    /** Tail of subprocess output for failure messages (keeps logs readable). */
    static String tail(String output, int maxChars) {
        if (output == null) {
            return "";
        }
        String normalized = output.replace("\r\n", "\n");
        if (normalized.length() <= maxChars) {
            return normalized;
        }
        return "..." + normalized.substring(normalized.length() - maxChars);
    }
}
