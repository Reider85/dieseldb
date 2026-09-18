package diesel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import diesel.storage.AtomicFileWriter;
import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies crash-safe atomic file persistence (Prompt 30): saves go through a
 * temp+fsync+atomic-rename helper ({@link AtomicFileWriter}) so an interrupted
 * write never truncates the previous valid file, and a leftover {@code .tmp}
 * next to a missing target is reported as a WARNING on load.
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class AtomicFileWriteTest {

    @TempDir
    Path tempDir;

    private String prevCsvCodec;

    @BeforeEach
    void saveCodec() {
        prevCsvCodec = System.getProperty("csv.compression.codec");
        System.setProperty("csv.compression.codec", "none");
    }

    @AfterEach
    void restoreCodec() {
        if (prevCsvCodec == null) {
            System.clearProperty("csv.compression.codec");
        } else {
            System.setProperty("csv.compression.codec", prevCsvCodec);
        }
    }

    private static final List<String> SCHEMA = List.of("NAME", "AGE", "CITY");
    private static final Map<String, Class<?>> TYPES;

    static {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("CITY", String.class);
        TYPES = Map.copyOf(t);
    }

    private static Map<String, Object> row(String name, int age, String city) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("NAME", name);
        r.put("AGE", age);
        r.put("CITY", city);
        return r;
    }

    /** Value whose {@code toString()} explodes mid-write, simulating an I/O failure. */
    private static final class Exploding {
        @Override
        public String toString() {
            throw new IllegalStateException("boom");
        }
    }

    // ─── Crash resilience: CSV/TSV save interruption ─────────────────

    @Test
    void csvInterruptedSaveKeepsPreviousVersionAndCleansTemp() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("T", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row("Alice", 25, "Minsk"));
        storage.saveToFile("T");

        Path csv = tempDir.resolve("T.csv");
        byte[] v1 = Files.readAllBytes(csv);

        // Insert a row that fails while being converted to text mid-write.
        Map<String, Object> bad = row("Bob", 30, "Kyiv");
        bad.put("NAME", new Exploding());
        storage.insert(bad);

        assertThrows(RuntimeException.class, () -> storage.saveToFile("T"));

        // The previous valid version is untouched and no temp file lingers.
        assertArrayEquals(v1, Files.readAllBytes(csv));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(csv)));
        storage.close();
    }

    @Test
    void tsvInterruptedSaveKeepsPreviousVersionAndCleansTemp() throws Exception {
        TsvRowStorage storage = new TsvRowStorage("T", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row("Alice", 25, "Minsk"));
        storage.saveToFile("T");

        Path tsv = tempDir.resolve("T.tsv");
        byte[] v1 = Files.readAllBytes(tsv);

        Map<String, Object> bad = row("Bob", 30, "Kyiv");
        bad.put("NAME", new Exploding());
        storage.insert(bad);

        assertThrows(RuntimeException.class, () -> storage.saveToFile("T"));

        assertArrayEquals(v1, Files.readAllBytes(tsv));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(tsv)));
        storage.close();
    }

    @Test
    void successfulSaveLeavesNoTempFilesAndRoundTrips() throws Exception {
        System.setProperty("csv.table.mirror", "on");
        try {
            successfulSaveLeavesNoTempFilesAndRoundTripsInner();
        } finally {
            System.clearProperty("csv.table.mirror");
        }
    }

    private void successfulSaveLeavesNoTempFilesAndRoundTripsInner() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("T", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(row("Alice", 25, "Minsk"));
        storage.saveToFile("T");

        Path csv = tempDir.resolve("T.csv");
        assertTrue(Files.exists(csv));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(csv)));
        storage.close();

        CsvRowStorage loaded = new CsvRowStorage("T", SCHEMA, TYPES);
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("T");
        assertEquals(1, loaded.scan().size());
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
        loaded.close();
    }

    // ─── AtomicFileWriter helper semantics ──────────────────────────

    @Test
    void uncommittedTextWriterDiscardsAndKeepsTarget() throws Exception {
        Path target = tempDir.resolve("doc.txt");
        try (AtomicFileWriter afw = AtomicFileWriter.openText(target)) {
            afw.bufferedWriter().write("version one\n");
            afw.commit();
        }
        String v1 = Files.readString(target);

        try (AtomicFileWriter afw = AtomicFileWriter.openText(target)) {
            afw.bufferedWriter().write("a much longer version two that never commits\n");
            // no commit() -> close() must discard
        }

        assertEquals(v1, Files.readString(target));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(target)));
    }

    @Test
    void uncommittedBinaryWriterDiscardsAndKeepsTarget() throws Exception {
        Path target = tempDir.resolve("blob.bin");
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
            afw.outputStream().write("version-one-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            afw.commit();
        }
        byte[] v1 = Files.readAllBytes(target);

        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
            afw.outputStream().write("partial garbage that never commits".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            // no commit() -> close() must discard
        }

        assertArrayEquals(v1, Files.readAllBytes(target));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(target)));
    }

    @Test
    void committedBinaryWriterReplacesTarget() throws Exception {
        Path target = tempDir.resolve("blob2.bin");
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
            afw.outputStream().write("committed".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            afw.commit();
        }
        assertArrayEquals("committed".getBytes(java.nio.charset.StandardCharsets.UTF_8), Files.readAllBytes(target));
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(target)));
    }

    // ─── Concurrent commits to one target ───────────────────────────

    @Test
    void concurrentCommitsToSameTargetNeverLoseTheTempFile() throws Exception {
        Path target = tempDir.resolve("shared.bin");
        byte[] payload = new byte[4096];
        new java.util.Random(42).nextBytes(payload);

        int workers = 4;
        int rounds = 150;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        for (int w = 0; w < workers; w++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                    for (int r = 0; r < rounds; r++) {
                        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
                            afw.outputStream().write(payload);
                            afw.outputStream().flush();
                            afw.commit();
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }, "atomic-commit-writer-" + w);
            threads.add(t);
            t.start();
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        for (Thread t : threads) {
            t.join(120_000);
        }

        assertTrue(errors.isEmpty(),
                "no concurrent commit may fail with the shared .tmp abducted: " + errors);
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(target)));
        assertArrayEquals(payload, Files.readAllBytes(target));
    }

    // ─── Orphan temp detection on load ──────────────────────────────

    @Test
    void loadWarnsWhenTargetMissingButTempExists() throws Exception {
        Path csv = tempDir.resolve("T.csv");
        Files.createFile(AtomicFileWriter.tmpPath(csv));

        try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.AtomicFileWriter")) {
            CsvRowStorage storage = new CsvRowStorage("T", SCHEMA, TYPES);
            storage.setDataDir(tempDir.toString());
            storage.open();
            storage.loadFromFile("T"); // target missing -> must not throw
            assertTrue(storage.scan().isEmpty());
            storage.close();

            List<ILoggingEvent> warnings = capture.eventsMatching(Level.WARN, "Interrupted write");
            assertFalse(warnings.isEmpty(),
                    "expected a WARNING about the orphaned temp file, got: " + capture.events());
        }
    }
}