package diesel;

import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroReadIterator;
import diesel.storage.avro.AvroSyncMarkerManager;
import diesel.storage.avro.AvroSyncMarkerManager.IntegrityResult;
import diesel.storage.avro.AvroSyncMarkerManager.RecoveryResult;
import diesel.storage.avro.AvroSyncMarkerManager.SyncMarkerInfo;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 77 AVRO sync marker manager tests:
 * random marker generation, integrity validation, crash recovery,
 * marker scanning and config resolution.
 */
@Tag("storage")
@StorageType("avro")
class AvroSyncMarkerManagerTest {

    private static final String[] PROP_KEYS = {
            "avro.syncmarker.validate.strict",
            "avro.syncmarker.recovery.backup",
            "avro.syncmarker.config.file",
            "avro.block.sync.interval"
    };

    @TempDir
    Path tempDir;

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

    // ─── Test data helpers ─────────────────────────────────────────

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

    /** Writes a multi-block file (small sync interval) and returns it. */
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

    /** Writes a single-block file with the default (64 MB) sync interval. */
    private File writeSingleBlock(int rows) throws IOException {
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

    /** Corrupts a single byte of the marker at {@code offset} (flips bit 0). */
    private static void corruptByte(File f, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 1);
        }
    }

    private static void truncate(File f, long length) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(length);
        }
    }

    private static SyncMarkerInfo lastBlockMarker(List<SyncMarkerInfo> markers) {
        return markers.stream()
                .filter(m -> m.blockIndex() >= 0)
                .reduce((a, b) -> b)
                .orElseThrow();
    }

    // ─── Generation ────────────────────────────────────────────────

    @Test
    void generateSyncMarkerIs16Bytes() {
        byte[] m = AvroSyncMarkerManager.generateSyncMarker();
        assertEquals(AvroSyncMarkerManager.SYNC_SIZE, m.length);
    }

    @Test
    void generateSyncMarkerIsRandom() {
        byte[] a = AvroSyncMarkerManager.generateSyncMarker();
        byte[] b = AvroSyncMarkerManager.generateSyncMarker();
        assertFalse(Arrays.equals(a, b), "two markers must differ");
    }

    @Test
    void generateSyncMarkerWithCustomRandom() {
        SecureRandom rng = new SecureRandom() {
            @Override
            public void nextBytes(byte[] bytes) {
                Arrays.fill(bytes, (byte) 0x5A);
            }
        };
        byte[] m = AvroSyncMarkerManager.generateSyncMarker(rng);
        byte[] expected = new byte[AvroSyncMarkerManager.SYNC_SIZE];
        Arrays.fill(expected, (byte) 0x5A);
        assertArrayEquals(expected, m, "custom random source must fill the marker");
    }

    @Test
    void generateSyncMarkerRejectsNullRandom() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroSyncMarkerManager.generateSyncMarker(null));
    }

    // ─── Integrity validation ──────────────────────────────────────

    @Test
    void validateHealthySingleBlock() throws IOException {
        File f = writeSingleBlock(50);
        IntegrityResult r = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        assertTrue(r.valid(), "healthy single-block file must validate: " + r.errors());
        assertTrue(r.errors().isEmpty());
        assertEquals(1, r.totalBlocks());
        assertEquals(-1, r.truncationOffset());
        assertTrue(r.totalPayloadBytes() > 0);
    }

    @Test
    void validateHealthyMultiBlock() throws IOException {
        File f = writeMultiBlock(2000);
        IntegrityResult r = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        assertTrue(r.valid(), "healthy multi-block file must validate: " + r.errors());
        assertTrue(r.totalBlocks() >= 2, "expected multiple blocks, got " + r.totalBlocks());
        assertEquals(-1, r.truncationOffset());
        // header marker + one marker per block (no flush marker for our writer)
        assertEquals(r.totalBlocks() + 1, r.markers().size(),
                "markers = header + one per block: " + r.markers());
    }

    @Test
    void validateHeaderOnlyFile() throws IOException {
        File f = writeMultiBlock(0);
        IntegrityResult r = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        assertTrue(r.valid(), r.errors().toString());
        assertEquals(0, r.totalBlocks());
    }

    @Test
    void validateCorruptedSyncMarkerFails() throws IOException {
        File f = writeMultiBlock(2000);
        List<SyncMarkerInfo> markers = AvroSyncMarkerManager.resolve().scanAllMarkers(f);
        SyncMarkerInfo blockMarker = markers.stream()
                .filter(m -> m.blockIndex() >= 0)
                .findFirst().orElseThrow();
        long offset = blockMarker.fileOffset();
        assertTrue(AvroSyncMarkerManager.isSyncMarkerValid(f, offset));
        corruptByte(f, offset);
        assertFalse(AvroSyncMarkerManager.isSyncMarkerValid(f, offset));

        IntegrityResult r = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        assertFalse(r.valid(), "corrupted marker must invalidate the file");
        assertTrue(r.errors().stream().anyMatch(e -> e.contains("mismatch")),
                "errors must mention the mismatch: " + r.errors());
        assertTrue(r.truncationOffset() >= 0);
    }

    @Test
    void validateTruncatedFileStrictThrows() throws IOException {
        File f = writeMultiBlock(2000);
        List<SyncMarkerInfo> markers = AvroSyncMarkerManager.resolve().scanAllMarkers(f);
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        truncate(f, lastBlockSync + 3); // middle of the last block's sync marker

        AvroSyncMarkerManager strict = new AvroSyncMarkerManager(true, true);
        assertThrows(IOException.class, () -> strict.validateIntegrity(f));
    }

    @Test
    void validateTruncatedFileLenientTreatsAsRecoverable() throws IOException {
        File f = writeMultiBlock(2000);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        List<SyncMarkerInfo> markers = healthy.markers();
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        long secondToLastSync = markers.stream()
                .filter(m -> m.blockIndex() == healthy.totalBlocks() - 2)
                .map(SyncMarkerInfo::fileOffset)
                .findFirst().orElseThrow();
        truncate(f, lastBlockSync + 3);

        AvroSyncMarkerManager lenient = new AvroSyncMarkerManager(false, true);
        IntegrityResult r = lenient.validateIntegrity(f);
        assertTrue(r.valid(), "lenient mode treats a truncated tail as recoverable: " + r.errors());
        assertTrue(r.errors().stream().anyMatch(e -> e.startsWith("Truncated")));
        assertEquals(secondToLastSync + AvroSyncMarkerManager.SYNC_SIZE, r.truncationOffset());
        assertEquals(healthy.totalBlocks() - 1, r.totalBlocks());
    }

    @Test
    void validateMissingFileThrows() {
        AvroSyncMarkerManager mgr = new AvroSyncMarkerManager(true, true);
        assertThrows(IOException.class,
                () -> mgr.validateIntegrity(new File(tempDir.toFile(), "nope.avro")));
    }

    @Test
    void validateEmptyFileThrows() throws IOException {
        File f = new File(tempDir.toFile(), "empty.avro");
        assertTrue(f.createNewFile());
        AvroSyncMarkerManager mgr = new AvroSyncMarkerManager(true, true);
        assertThrows(IOException.class, () -> mgr.validateIntegrity(f));
    }

    // ─── Crash recovery ────────────────────────────────────────────

    @Test
    void recoverHealthyFileIsNoOp() throws IOException {
        File f = writeMultiBlock(500);
        long before = f.length();
        RecoveryResult r = AvroSyncMarkerManager.resolve().recoverToLastValidBlock(f);
        assertFalse(r.truncated());
        assertEquals(-1, r.truncatedAt());
        assertFalse(r.backupCreated());
        assertEquals(before, f.length(), "healthy file must not be modified");
        assertEquals(500, countRows(f));
        assertNotNull(r.lastValidMarker());
        assertEquals(AvroSyncMarkerManager.SYNC_SIZE, r.lastValidMarker().length);
    }

    @Test
    void recoverTruncatedTail() throws IOException {
        File f = writeMultiBlock(2000);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        List<SyncMarkerInfo> markers = healthy.markers();
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        long secondToLastSync = markers.stream()
                .filter(m -> m.blockIndex() == healthy.totalBlocks() - 2)
                .map(SyncMarkerInfo::fileOffset)
                .findFirst().orElseThrow();
        long expectedEnd = secondToLastSync + AvroSyncMarkerManager.SYNC_SIZE;
        long originalLen = f.length();
        truncate(f, lastBlockSync + 3);

        RecoveryResult r = AvroSyncMarkerManager.resolve().recoverToLastValidBlock(f);
        assertTrue(r.truncated());
        assertEquals(expectedEnd, r.truncatedAt());
        assertTrue(r.backupCreated());
        assertEquals(healthy.totalBlocks() - 1, r.blocksRecovered());
        assertTrue(f.length() < originalLen);
    }

    @Test
    void recoverTruncatedTailPreservesRecords() throws IOException {
        File f = writeMultiBlock(2000);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        List<SyncMarkerInfo> markers = healthy.markers();
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        truncate(f, lastBlockSync + 3);

        RecoveryResult r = AvroSyncMarkerManager.resolve().recoverToLastValidBlock(f);
        assertTrue(r.truncated());
        assertEquals(healthy.totalBlocks() - 1, r.blocksRecovered());
        assertEquals(r.recordsRecovered(), countRows(f),
                "recovered rows == preserved records");
    }

    @Test
    void recoverCorruptedSyncMarkerDropsTheBlock() throws IOException {
        File f = writeMultiBlock(2000);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        List<SyncMarkerInfo> markers = healthy.markers();
        // corrupt the sync marker of block index 2
        SyncMarkerInfo corrupted = markers.stream()
                .filter(m -> m.blockIndex() == 2)
                .findFirst().orElseThrow();
        corruptByte(f, corrupted.fileOffset());

        RecoveryResult r = AvroSyncMarkerManager.resolve().recoverToLastValidBlock(f);
        assertTrue(r.truncated());
        assertEquals(2, r.blocksRecovered(), "blocks 0 and 1 survive, block 2 is dropped");
        long expectedEnd = markers.stream()
                .filter(m -> m.blockIndex() == 1)
                .map(SyncMarkerInfo::fileOffset)
                .findFirst().orElseThrow() + AvroSyncMarkerManager.SYNC_SIZE;
        assertEquals(expectedEnd, r.truncatedAt());
        assertEquals(r.recordsRecovered(), countRows(f));
        assertTrue(r.recordsRecovered() < 2000, "corrupted block's rows are dropped");
        assertTrue(r.recordsRecovered() > 0);
    }

    @Test
    void recoveryWritesBackupFile() throws IOException {
        File f = writeMultiBlock(500);
        List<SyncMarkerInfo> markers = AvroSyncMarkerManager.resolve().scanAllMarkers(f);
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        long crashedLen = lastBlockSync + 3;
        truncate(f, crashedLen);

        AvroSyncMarkerManager mgr = new AvroSyncMarkerManager(true, true);
        RecoveryResult r = mgr.recoverToLastValidBlock(f);
        assertTrue(r.backupCreated());
        File bak = new File(f.getParentFile(), f.getName() + ".bak");
        assertTrue(bak.isFile(), "backup file must exist");
        assertEquals(crashedLen, bak.length(), "backup holds the crashed file's tail bytes");
        assertTrue(f.length() < bak.length(), "recovery must shorten the file below the backup");
    }

    @Test
    void recoveryBackupDisabled() throws IOException {
        File f = writeMultiBlock(500);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        List<SyncMarkerInfo> markers = healthy.markers();
        long lastBlockSync = lastBlockMarker(markers).fileOffset();
        truncate(f, lastBlockSync + 3);

        AvroSyncMarkerManager mgr = new AvroSyncMarkerManager(true, false);
        RecoveryResult r = mgr.recoverToLastValidBlock(f);
        assertTrue(r.truncated());
        assertFalse(r.backupCreated());
        assertFalse(new File(f.getParentFile(), f.getName() + ".bak").exists(),
                "no backup must be written when disabled");
    }

    // ─── Marker scanning ───────────────────────────────────────────

    @Test
    void scanAllMarkersHealthyMultiBlock() throws IOException {
        File f = writeMultiBlock(2000);
        List<SyncMarkerInfo> markers = AvroSyncMarkerManager.resolve().scanAllMarkers(f);
        assertFalse(markers.isEmpty());
        assertEquals(1, markers.stream().filter(m -> m.blockIndex() == -1).count(),
                "exactly one marker (the header) has blockIndex -1");
        long prev = -1;
        for (SyncMarkerInfo m : markers) {
            assertTrue(m.fileOffset() > prev, "markers must appear in file order");
            assertEquals(AvroSyncMarkerManager.SYNC_SIZE, m.marker().length);
            prev = m.fileOffset();
        }
        int blockMarkers = (int) markers.stream().filter(m -> m.blockIndex() >= 0).count();
        assertTrue(blockMarkers >= 2, "multi-block file must expose >= 2 block markers");
    }

    @Test
    void countBlocksMatchesIntegrity() throws IOException {
        File f = writeMultiBlock(2000);
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        int blocks = mgr.countBlocks(f);
        IntegrityResult r = mgr.validateIntegrity(f);
        assertEquals(r.totalBlocks(), blocks);
        assertTrue(blocks >= 2);
    }

    @Test
    void headerMarkerLocatedAtHeaderEnd() throws IOException {
        File f = writeSingleBlock(10);
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        List<SyncMarkerInfo> markers = mgr.scanAllMarkers(f);
        long headerEnd;
        try (AvroDataFileReader probe = new AvroDataFileReader(f)) {
            headerEnd = probe.getPosition();
        }
        SyncMarkerInfo header = markers.stream()
                .filter(m -> m.blockIndex() == -1)
                .findFirst().orElseThrow();
        assertEquals(headerEnd - AvroSyncMarkerManager.SYNC_SIZE, header.fileOffset());
    }

    @Test
    void isSyncMarkerValidDetectsSingleBlockFile() throws IOException {
        File f = writeSingleBlock(10);
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        List<SyncMarkerInfo> markers = mgr.scanAllMarkers(f);
        SyncMarkerInfo blockMarker = markers.stream()
                .filter(m -> m.blockIndex() >= 0)
                .findFirst().orElseThrow();
        assertTrue(AvroSyncMarkerManager.isSyncMarkerValid(f, blockMarker.fileOffset()));
        corruptByte(f, blockMarker.fileOffset());
        assertFalse(AvroSyncMarkerManager.isSyncMarkerValid(f, blockMarker.fileOffset()));
    }

    // ─── Config resolution ─────────────────────────────────────────

    @Test
    void configDefaults() {
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        assertEquals(AvroSyncMarkerManager.DEFAULT_VALIDATE_STRICT, mgr.validateStrict());
        assertEquals(AvroSyncMarkerManager.DEFAULT_RECOVERY_BACKUP, mgr.createBackup());
        assertEquals("avro.syncmarker.validate.strict", AvroSyncMarkerManager.VALIDATE_STRICT_KEY);
        assertEquals("avro.syncmarker.recovery.backup", AvroSyncMarkerManager.RECOVERY_BACKUP_KEY);
    }

    @Test
    void configSyspropOverridesDefault() {
        System.setProperty("avro.syncmarker.validate.strict", "false");
        System.setProperty("avro.syncmarker.recovery.backup", "false");
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        assertFalse(mgr.validateStrict());
        assertFalse(mgr.createBackup());
    }

    @Test
    void configFileOverride() throws Exception {
        Path config = tempDir.resolve("syncmarker-test.properties");
        Properties props = new Properties();
        props.setProperty("avro.syncmarker.recovery.backup", "false");
        try (OutputStream out = new FileOutputStream(config.toFile())) {
            props.store(out, "test");
        }
        System.setProperty("avro.syncmarker.config.file", config.toString());
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        assertFalse(mgr.createBackup());
        assertTrue(mgr.validateStrict(), "default when absent from file stays true");
    }

    @Test
    void configFileOverrideGivesWayToSysprop() throws Exception {
        Path config = tempDir.resolve("syncmarker-test.properties");
        Properties props = new Properties();
        props.setProperty("avro.syncmarker.recovery.backup", "true");
        try (OutputStream out = new FileOutputStream(config.toFile())) {
            props.store(out, "test");
        }
        System.setProperty("avro.syncmarker.config.file", config.toString());
        System.setProperty("avro.syncmarker.recovery.backup", "false");
        AvroSyncMarkerManager mgr = AvroSyncMarkerManager.resolve();
        assertFalse(mgr.createBackup(), "sysprop wins over config.properties");
    }
}