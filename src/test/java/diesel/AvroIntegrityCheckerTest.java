package diesel;

import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroIntegrityChecker;
import diesel.storage.avro.AvroIntegrityChecker.BlockIntegrityResult;
import diesel.storage.avro.AvroIntegrityChecker.IntegrityReport;
import diesel.storage.avro.AvroIntegrityChecker.IntegrityStats;
import diesel.storage.avro.AvroReadIterator;
import diesel.storage.avro.AvroTypeMapper;
import diesel.StorageType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AvroIntegrityChecker} (Prompt 83).
 *
 * <p>Covers: config resolution (sysprop / config-file / defaults / invalid),
 * clean-file validation, header-only / empty files, single corrupt block
 * detection, bit-rot via byte-flip, truncated payloads, CRC mismatch with
 * in-memory cache, sidecar round-trip, failOnMismatch behaviour, thread
 * safety, directory scanAll, and IntegrityStats.</p>
 */
@Tag("storage")
@StorageType("avro")
class AvroIntegrityCheckerTest {

    @TempDir
    Path tempDir;

    private static final String[] PROP_KEYS = {
            AvroIntegrityChecker.CHECK_ON_READ_KEY,
            AvroIntegrityChecker.CHECK_ON_OPEN_KEY,
            AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY,
            AvroIntegrityChecker.STORE_SIDECAR_KEY,
            "avro.integrity.config.file"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveProps() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreProps() {
        for (String key : PROP_KEYS) {
            String value = prevProps.get(key);
            if (value != null) {
                System.setProperty(key, value);
            } else {
                System.clearProperty(key);
            }
        }
    }

    // ─── helpers ────────────────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User-" + id);
        m.put("AGE", (int) (id % 100));
        return m;
    }

    private static Map<String, Class<?>> types(Schema schema) {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        for (Schema.Field f : schema.getFields()) {
            m.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
        }
        return m;
    }

    private static long countRows(File f) throws IOException {
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols(), types(reader.getSchema()))) {
            long n = 0;
            while (it.hasNext()) {
                it.next();
                n++;
            }
            return n;
        }
    }

    /** Flips bit 0 of the byte at {@code offset}. */
    private static void corruptByte(File f, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 1);
        }
    }

    private File writeMultiBlock(int rows) throws IOException {
        System.setProperty("avro.block.sync.interval", "256");
        File f = new File(tempDir.toFile(), "multi-" + rows + ".avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (int i = 0; i < rows; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private File writeEmpty() throws IOException {
        System.setProperty("avro.block.sync.interval", "256");
        File f = new File(tempDir.toFile(), "empty.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private File writeSingleBlock(int rows) throws IOException {
        System.setProperty("avro.block.sync.interval", "104857600");
        File f = new File(tempDir.toFile(), "single-" + rows + ".avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (int i = 0; i < rows; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void resolveDefaults() {
        System.clearProperty(AvroIntegrityChecker.CHECK_ON_READ_KEY);
        System.clearProperty(AvroIntegrityChecker.CHECK_ON_OPEN_KEY);
        System.clearProperty(AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY);
        System.clearProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        assertTrue(c.checkOnRead());
        assertFalse(c.checkOnOpen());
        assertFalse(c.failOnMismatch());
        assertFalse(c.storeSidecar());
    }

    @Test
    void resolveSysprops() {
        System.setProperty(AvroIntegrityChecker.CHECK_ON_READ_KEY, "false");
        System.setProperty(AvroIntegrityChecker.CHECK_ON_OPEN_KEY, "true");
        System.setProperty(AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY, "true");
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        assertFalse(c.checkOnRead());
        assertTrue(c.checkOnOpen());
        assertTrue(c.failOnMismatch());
        assertTrue(c.storeSidecar());
    }

    @Test
    void resolveConfigFile() throws IOException {
        for (String key : PROP_KEYS) System.clearProperty(key);
        File cfg = new File(tempDir.toFile(), "test-config.properties");
        Files.writeString(cfg.toPath(),
                AvroIntegrityChecker.CHECK_ON_READ_KEY + "=false\n"
                        + AvroIntegrityChecker.CHECK_ON_OPEN_KEY + "=true\n"
                        + AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY + "=true\n"
                        + AvroIntegrityChecker.STORE_SIDECAR_KEY + "=true\n");
        System.setProperty("avro.integrity.config.file", cfg.getAbsolutePath());
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        assertFalse(c.checkOnRead());
        assertTrue(c.checkOnOpen());
        assertTrue(c.failOnMismatch());
        assertTrue(c.storeSidecar());
    }

    @Test
    void resolveInvalidConfigFile() {
        System.setProperty("avro.integrity.config.file", "/nonexistent/path.properties");
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        assertTrue(c.checkOnRead());
        assertFalse(c.failOnMismatch());
    }

    // ─── Clean file ─────────────────────────────────────────────────

    @Test
    void cleanFileAllValid() throws IOException {
        File f = writeMultiBlock(200);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        assertTrue(r.fullyValid());
        assertTrue(r.blocksChecked() >= 1);
        assertEquals(r.blocksChecked(), r.blocksValid());
        assertEquals(0, r.blocksCorrupted());
        assertTrue(r.problems().isEmpty());
        assertTrue(r.totalCompressedBytes() > 0);
        assertTrue(r.totalDecompressedBytes() > 0);
    }

    @Test
    void emptyFileFullyValid() throws IOException {
        File f = writeEmpty();
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        assertTrue(r.fullyValid());
        assertEquals(0, r.blocksChecked());
    }

    @Test
    void singleBlockAllValid() throws IOException {
        File f = writeSingleBlock(100);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        assertTrue(r.fullyValid());
        assertEquals(1, r.blocksChecked());
    }

    // ─── Corruption detection ───────────────────────────────────────

    @Test
    void bitRotDetected() throws IOException {
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport clean = c.validateFile(f);
        assertTrue(clean.fullyValid());
        assertTrue(clean.blocksChecked() >= 2);

        BlockIntegrityResult first = clean.blocks().get(0);
        long dataStart = first.contentEndOffset() - 32;
        corruptByte(f, dataStart);

        c.resetStats();
        IntegrityReport dirty = c.validateFile(f);
        assertFalse(dirty.fullyValid());
        assertTrue(dirty.blocksCorrupted() >= 1);
    }

    @Test
    void truncBlockDetected() throws IOException {
        File f = writeMultiBlock(200);
        long len = f.length();
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(len - 4);
        }
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        assertFalse(r.fullyValid());
        assertTrue(r.blocksCorrupted() >= 1 || !r.problems().isEmpty());
    }

    // ─── CRC mismatch with sidecar ─────────────────────────────────

    @Test
    void sidecarRoundTrip() throws IOException {
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        assertTrue(r.fullyValid());

        File sidecar = AvroIntegrityChecker.sidecarFile(f);
        assertTrue(sidecar.exists());
        assertTrue(sidecar.getName().endsWith(AvroIntegrityChecker.SIDECAR_SUFFIX));

        Map<Integer, Long> cached = c.readSidecar(f);
        assertFalse(cached.isEmpty());
        assertEquals((int) r.blocksChecked(), cached.size());
    }

    @Test
    void sidecarDetectsRotAcrossScans() throws IOException {
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        File f = writeMultiBlock(500);

        AvroIntegrityChecker c1 = AvroIntegrityChecker.resolve();
        IntegrityReport r1 = c1.validateFile(f);
        assertTrue(r1.fullyValid());

        BlockIntegrityResult first = r1.blocks().get(0);
        long dataStart = first.contentEndOffset() - 32;
        corruptByte(f, dataStart);

        AvroIntegrityChecker c2 = AvroIntegrityChecker.resolve();
        IntegrityReport r2 = c2.validateFile(f);
        assertFalse(r2.fullyValid());
    }

    // ─── failOnMismatch ─────────────────────────────────────────────

    @Test
    void failOnMismatchThrows() throws IOException {
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        System.setProperty(AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY, "true");
        File f = writeMultiBlock(200);

        AvroIntegrityChecker base = AvroIntegrityChecker.resolve();
        IntegrityReport clean = base.validateFile(f);
        assertTrue(clean.fullyValid());

        BlockIntegrityResult first = clean.blocks().get(0);
        long dataStart = first.contentEndOffset() - 32;
        corruptByte(f, dataStart);

        AvroIntegrityChecker strict = AvroIntegrityChecker.resolve();
        assertThrows(IOException.class, () -> strict.validateFile(f));
    }

    @Test
    void failOnMismatchOffRecordsOnly() throws IOException {
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        System.setProperty(AvroIntegrityChecker.FAIL_ON_MISMATCH_KEY, "false");
        File f = writeMultiBlock(200);

        AvroIntegrityChecker base = AvroIntegrityChecker.resolve();
        IntegrityReport clean = base.validateFile(f);
        assertTrue(clean.fullyValid());

        BlockIntegrityResult first = clean.blocks().get(0);
        long dataStart = first.contentEndOffset() - 32;
        corruptByte(f, dataStart);

        AvroIntegrityChecker lenient = AvroIntegrityChecker.resolve();
        IntegrityReport dirty = lenient.validateFile(f);
        assertFalse(dirty.fullyValid());
    }

    // ─── Stats ──────────────────────────────────────────────────────

    @Test
    void statsAccumulate() throws IOException {
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        c.validateFile(f);
        c.validateFile(f);
        IntegrityStats.Snapshot s = c.stats().snapshot();
        assertEquals(2, s.checks());
        assertEquals(2, s.filesValidated());
        assertTrue(s.blocksValidated() >= 2);
        assertTrue(s.bytesValidated() > 0);
    }

    @Test
    void statsReset() throws IOException {
        File f = writeMultiBlock(200);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        c.validateFile(f);
        c.resetStats();
        IntegrityStats.Snapshot s = c.stats().snapshot();
        assertEquals(0, s.checks());
        assertEquals(0, s.filesValidated());
        assertEquals(0, s.blocksValidated());
        assertEquals(0, s.bytesValidated());
    }

    @Test
    void statsCrcMismatchCounted() throws IOException {
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport clean = c.validateFile(f);
        assertTrue(clean.fullyValid());

        BlockIntegrityResult first = clean.blocks().get(0);
        long dataStart = first.contentEndOffset() - 32;
        corruptByte(f, dataStart);

        c.validateFile(f);
        IntegrityStats.Snapshot s = c.stats().snapshot();
        assertTrue(s.crcMismatches() >= 1 || s.blocksCorrupted() >= 1);
    }

    // ─── computeCrc ─────────────────────────────────────────────────

    @Test
    void computeCrcDeterministic() {
        byte[] data = "hello world avro integrity".getBytes(StandardCharsets.UTF_8);
        long a = AvroIntegrityChecker.computeCrc(data);
        long b = AvroIntegrityChecker.computeCrc(data);
        assertEquals(a, b);
    }

    @Test
    void computeCrcDiffersForDifferentData() {
        byte[] a = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[] b = "bbb".getBytes(StandardCharsets.UTF_8);
        assertFalse(AvroIntegrityChecker.computeCrc(a) == AvroIntegrityChecker.computeCrc(b));
    }

    // ─── validateBlock ──────────────────────────────────────────────

    @Test
    void validateBlockReturnsCorrectIndex() throws IOException {
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        BlockIntegrityResult r = c.validateBlock(f, 0);
        assertNotNull(r);
        assertEquals(0, r.blockIndex());
        assertTrue(r.valid());
    }

    @Test
    void validateBlockOutOfRangeReturnsNull() throws IOException {
        File f = writeSingleBlock(10);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        BlockIntegrityResult r = c.validateBlock(f, 99);
        assertNull(r);
    }

    @Test
    void validateBlockNegativeThrows() {
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        File f = new File(tempDir.toFile(), "nonexistent.avro");
        assertThrows(IllegalArgumentException.class, () -> c.validateBlock(f, -1));
    }

    // ─── directory scan ─────────────────────────────────────────────

    @Test
    void scanAllFindsValidFiles() throws IOException {
        writeMultiBlock(200);
        writeMultiBlock(300);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        var dirReport = c.scanAll(tempDir.toFile());
        assertTrue(dirReport.allValid());
        assertEquals(2, dirReport.files().size());
        assertTrue(dirReport.totalBlocks() >= 2);
    }

    @Test
    void scanAllDetectsCorruptFile() throws IOException {
        System.setProperty(AvroIntegrityChecker.STORE_SIDECAR_KEY, "true");
        File good = writeMultiBlock(200);
        File bad = writeMultiBlock(300);

        AvroIntegrityChecker base = AvroIntegrityChecker.resolve();
        IntegrityReport br = base.validateFile(bad);
        assertTrue(br.fullyValid());
        BlockIntegrityResult first = br.blocks().get(0);
        corruptByte(bad, first.contentEndOffset() - 32);

        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        var dirReport = c.scanAll(tempDir.toFile());
        assertFalse(dirReport.allValid());
        assertTrue(dirReport.filesCorrupt() >= 1);
    }

    // ─── sidecarFile ────────────────────────────────────────────────

    @Test
    void sidecarFilePath() {
        File f = new File("/data/test.avro");
        File sc = AvroIntegrityChecker.sidecarFile(f);
        assertTrue(sc.getPath().endsWith(AvroIntegrityChecker.SIDECAR_SUFFIX));
        assertTrue(sc.getPath().contains("test.avro"));
    }

    // ─── toString ───────────────────────────────────────────────────

    @Test
    void toStringNotNull() {
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        assertNotNull(c.toString());
        assertTrue(c.toString().contains("AvroIntegrityChecker"));
    }

    // ─── report summary ─────────────────────────────────────────────

    @Test
    void reportSummaryFormat() throws IOException {
        File f = writeMultiBlock(200);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport r = c.validateFile(f);
        String s = r.summary();
        assertTrue(s.contains("blocks="));
        assertTrue(s.contains("valid="));
        assertTrue(s.contains("fullyValid="));
    }

    // ─── thread safety ──────────────────────────────────────────────

    @Test
    void concurrentValidationSafe() throws Exception {
        File f = writeMultiBlock(500);
        AvroIntegrityChecker c = AvroIntegrityChecker.resolve();
        IntegrityReport baseline = c.validateFile(f);
        assertTrue(baseline.fullyValid());

        int threads = 4;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threads);
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(false);

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        IntegrityReport r = c.validateFile(f);
                        if (!r.fullyValid()) fail.set(true);
                    }
                } catch (Exception e) {
                    fail.set(true);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await(30, java.util.concurrent.TimeUnit.SECONDS);
        assertFalse(fail.get());
    }
}
