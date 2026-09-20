package diesel;

import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroInputFormatCompat;
import diesel.storage.avro.AvroReadIterator;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 70 AVRO split compatibility tests: sync-marker-aligned split
 * computation, non-overlapping full-file coverage, per-split reads with and
 * without projection pushdown, structural analysis, and merge of trailing tiny
 * splits.
 */
@Tag("storage")
@StorageType("avro")
class AvroInputFormatCompatTest {

    private static final String SPLIT_SIZE_KEY = "avro.split.size";
    private static final String MIN_SIZE_KEY = "avro.split.min.size";
    private static final String SYNC_KEY = "avro.block.sync.interval";

    @TempDir
    Path tempDir;

    private String prevSplit;
    private String prevMin;
    private String prevSync;

    @BeforeEach
    void saveProps() {
        prevSplit = System.getProperty(SPLIT_SIZE_KEY);
        prevMin = System.getProperty(MIN_SIZE_KEY);
        prevSync = System.getProperty(SYNC_KEY);
    }

    @AfterEach
    void restoreProps() {
        restore(SPLIT_SIZE_KEY, prevSplit);
        restore(MIN_SIZE_KEY, prevMin);
        restore(SYNC_KEY, prevSync);
    }

    private static void restore(String key, String value) {
        if (value != null) {
            System.setProperty(key, value);
        } else {
            System.clearProperty(key);
        }
    }

    // ─── Test data helpers ─────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        m.put("ACTIVE", Boolean.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User-" + id);
        m.put("AGE", (int) (id % 100));
        m.put("ACTIVE", id % 2 == 0);
        return m;
    }

    /** Writes a multi-block file (small sync interval, so many blocks). */
    private File writeMultiBlock(int rows) throws IOException {
        return writeMultiBlock(rows, null);
    }

    /** Writes a multi-block file with an explicit codec ({@code null} = no codec). */
    private File writeMultiBlock(int rows, String codecName) throws IOException {
        return writeMultiBlock(rows, codecName, 512);
    }

    private File writeMultiBlock(int rows, String codecName, int syncInterval) throws IOException {
        System.setProperty(SYNC_KEY, Integer.toString(syncInterval));
        File f = new File(tempDir.toFile(), "multi-" + rows + "-" + syncInterval + ".avro");
        var codec = codecName == null ? null : AvroCodecFactory.factory(codecName, -1);
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f, codec);
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

    /** Writes a single-block file (default 64 MB sync interval). */
    private File writeSingleBlock(int rows) throws IOException {
        System.clearProperty(SYNC_KEY);
        File f = new File(tempDir.toFile(), "single.avro");
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

    private static Map<String, Class<?>> typesOf(Schema schema) {
        Map<String, Class<?>> m = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Schema.Field f : schema.getFields()) {
            m.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
        }
        return m;
    }

    private static List<Object[]> sequentialRead(File f, List<String> cols) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols, typesOf(reader.getSchema()))) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    private static List<Object[]> sequentialProjected(File f, List<String> cols) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (AvroDataFileReader reader = new AvroDataFileReader(f, cols);
             AvroReadIterator it = new AvroReadIterator(reader, cols, typesOf(reader.getSchema()))) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    private static void assertRowsEqual(List<Object[]> expected, List<Object[]> actual) {
        assertEquals(expected.size(), actual.size(), "row count");
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "row " + i);
        }
    }

    private static List<Object[]> readAllSplits(File f, List<AvroInputFormatCompat.SplitDescriptor> splits)
            throws IOException {
        List<Object[]> all = new ArrayList<>();
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            all.addAll(AvroInputFormatCompat.readSplit(f, s));
        }
        return all;
    }

    // ─── Split correctness ─────────────────────────────────────────

    @Test
    void singleBlockFileYieldsOneSplit() throws IOException {
        File f = writeSingleBlock(50);
        List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);
        assertEquals(1, splits.size(), "a single-block file must yield exactly one split");
        AvroInputFormatCompat.SplitDescriptor s = splits.get(0);
        assertEquals(0, s.firstBlock());
        assertEquals(1, s.endBlock());
        assertEquals(50, s.recordCount());
        assertTrue(s.length() > 0);
        assertRowsEqual(sequentialRead(f, cols()), AvroInputFormatCompat.readSplit(f, s));
    }

    @Test
    void splitsCoverWholeFileExactly() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        System.setProperty(MIN_SIZE_KEY, "128");
        File f = writeMultiBlock(700);

        List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);
        assertTrue(splits.size() > 1, "a multi-block file must be cut into several splits");

        assertTrue(AvroInputFormatCompat.validateSplits(f, splits),
                "splits must tile the file without gaps or overlaps");

        long totalRecords = 0;
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            assertTrue(s.recordCount() > 0, "every split must contain at least one record");
            totalRecords += s.recordCount();
        }
        assertEquals(700, totalRecords, "split record counts must sum to the file's total");

        // every split start must coincide with a block boundary
        AvroInputFormatCompat.FileLayout layout = AvroInputFormatCompat.analyzeFile(f);
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            boolean aligned = layout.blocks().stream()
                    .anyMatch(b -> b.headerPos() == s.start());
            assertTrue(aligned, "split start " + s.start() + " must align on a sync-marker boundary");
        }

        assertRowsEqual(sequentialRead(f, cols()), readAllSplits(f, splits));
    }

    @Test
    void smallerTargetProducesMoreSplits() throws IOException {
        File f = writeMultiBlock(700);

        System.setProperty(MIN_SIZE_KEY, "128");
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        List<AvroInputFormatCompat.SplitDescriptor> small = AvroInputFormatCompat.computeSplits(f);

        System.setProperty(SPLIT_SIZE_KEY, "8192");
        List<AvroInputFormatCompat.SplitDescriptor> large = AvroInputFormatCompat.computeSplits(f);

        assertTrue(small.size() > large.size(),
                "a smaller target split size must yield more splits");

        long total = 0;
        for (AvroInputFormatCompat.SplitDescriptor s : small) {
            total += s.recordCount();
        }
        assertEquals(700, total, "split size must not change the record coverage");
    }

    @Test
    void lastSplitAbsorbsRemainder() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "4096");
        System.setProperty(MIN_SIZE_KEY, "128");
        File f = writeMultiBlock(700);
        List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);

        AvroInputFormatCompat.SplitDescriptor last = splits.get(splits.size() - 1);
        AvroInputFormatCompat.FileLayout layout = AvroInputFormatCompat.analyzeFile(f);
        // the last split always ends at the last block's trailing sync marker
        AvroInputFormatCompat.BlockRange lastBlock = layout.blocks().get(layout.blocks().size() - 1);
        assertEquals(lastBlock.blockEnd(), last.end(), "the final split must abut the end of the data section");

        long records = 0;
        for (int b = last.firstBlock(); b < last.endBlock(); b++) {
            records += layout.blocks().get(b).recordCount();
        }
        assertEquals(records, last.recordCount(), "the last split must carry all its blocks' records");
    }

    @Test
    void trailingTinySplitMergedIntoPrevious() throws IOException {
        System.setProperty(MIN_SIZE_KEY, "1"); // no merging: keep every target split
        File f = writeMultiBlock(400, null, 512);
        List<AvroInputFormatCompat.SplitDescriptor> unmerged = withTargetSize(f, 2048);

        assertTrue(unmerged.size() > 1, "without a minimum the file must stay split");
        long unmergedTotal = sumRecords(unmerged);

        System.setProperty(MIN_SIZE_KEY, Long.toString(unmerged.get(unmerged.size() - 1).length() + 1));
        List<AvroInputFormatCompat.SplitDescriptor> merged = withTargetSize(f, 2048);

        assertTrue(merged.size() < unmerged.size(),
                "a trailing split below the minimum must merge into the previous split");
        assertEquals(unmergedTotal, sumRecords(merged), "merging must not drop records");
        assertTrue(AvroInputFormatCompat.validateSplits(f, merged));
    }

    private static List<AvroInputFormatCompat.SplitDescriptor> withTargetSize(File f, long target)
            throws IOException {
        return AvroInputFormatCompat.computeSplits(f, target, resolveLong(MIN_SIZE_KEY, 1));
    }

    private static long resolveLong(String key, long dflt) {
        String v = System.getProperty(key);
        return v != null ? Long.parseLong(v.trim()) : dflt;
    }

    private static long sumRecords(List<AvroInputFormatCompat.SplitDescriptor> splits) {
        long n = 0;
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            n += s.recordCount();
        }
        return n;
    }

    // ─── Reading ───────────────────────────────────────────────────

    @Test
    void readSplitEqualsSequentialSlice() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        System.setProperty(MIN_SIZE_KEY, "128");
        File f = writeMultiBlock(700);
        List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);
        List<Object[]> sequential = sequentialRead(f, cols());

        long offset = 0;
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            List<Object[]> actual = AvroInputFormatCompat.readSplit(f, s);
            assertEquals(s.recordCount(), actual.size(), "split must return exactly its declared records");
            for (int i = 0; i < actual.size(); i++) {
                assertArrayEquals(sequential.get((int) (offset + i)), actual.get(i),
                        "row " + (offset + i) + " in split starting at block " + s.firstBlock());
            }
            offset += s.recordCount();
        }
        assertEquals(700, offset);
    }

    @Test
    void projectionPushdownPerSplit() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        System.setProperty(MIN_SIZE_KEY, "128");
        File f = writeMultiBlock(300);
        List<String> proj = List.of("NAME", "ID");
        List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);

        List<Object[]> expected = sequentialProjected(f, proj);
        List<Object[]> actual = new ArrayList<>();
        for (AvroInputFormatCompat.SplitDescriptor s : splits) {
            actual.addAll(AvroInputFormatCompat.readSplit(f, s, proj));
        }
        assertRowsEqual(expected, actual);
    }

    @Test
    void readAllEqualsSequential() throws IOException {
        File f = writeMultiBlock(200);
        assertRowsEqual(sequentialRead(f, cols()), AvroInputFormatCompat.readAll(f));
    }

    // ─── Edge cases ────────────────────────────────────────────────

    @Test
    void emptyFileYieldsNoSplits() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        File f = new File(tempDir.toFile(), "header.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        w.close(); // zero rows -> header + trailing sync only
        assertTrue(AvroInputFormatCompat.computeSplits(f).isEmpty());
        assertTrue(AvroInputFormatCompat.validateSplits(f, List.of()),
                "an empty split set must validate against a header-only file");
        assertTrue(AvroInputFormatCompat.readAll(f).isEmpty());
    }

    @Test
    void trailingSyncOnlyRegionNotTreatedAsBlock() throws IOException {
        File f = writeSingleBlock(30);
        AvroInputFormatCompat.FileLayout layout = AvroInputFormatCompat.analyzeFile(f);
        assertEquals(1, layout.blocks().size(),
                "the trailing FLUSH sync marker must not be scanned as a block");
        assertEquals(30, layout.totalRecords());
        assertTrue(layout.fileSize() > layout.headerSize());
    }

    @Test
    void analyzeFileLayoutExposesStructure() throws IOException {
        File f = writeMultiBlock(250, "deflate");
        AvroInputFormatCompat.FileLayout layout = AvroInputFormatCompat.analyzeFile(f);

        assertEquals(250, layout.totalRecords());
        assertEquals("deflate", layout.codecName());
        assertNotNull(layout.schema());
        assertEquals(16, layout.syncMarker().length);
        assertTrue(layout.blocks().size() > 1, "small sync interval must produce multiple blocks");

        long payload = 0;
        for (AvroInputFormatCompat.BlockRange b : layout.blocks()) {
            assertTrue(b.headerPos() >= layout.headerSize(), "block headers must follow the header");
            assertTrue(b.blockEnd() > b.headerPos());
            assertTrue(b.recordCount() > 0);
            payload += b.payloadSize();
        }
        assertEquals(layout.totalPayloadBytes(), payload);
    }

    @Test
    void splitsWorkAcrossCodecs() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "4096");
        System.setProperty(MIN_SIZE_KEY, "128");
        for (String codec : List.of("deflate", "snappy")) {
            File f = writeMultiBlock(400, codec);
            List<AvroInputFormatCompat.SplitDescriptor> splits = AvroInputFormatCompat.computeSplits(f);
            assertTrue(AvroInputFormatCompat.validateSplits(f, splits), codec + " splits must tile the file");
            assertRowsEqual(sequentialRead(f, cols()), readAllSplits(f, splits), codec);
        }
    }

    private static void assertRowsEqual(List<Object[]> expected, List<Object[]> actual, String what) {
        assertEquals(expected.size(), actual.size(), what + ": row count");
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), what + ": row " + i);
        }
    }

    @Test
    void corruptOrMissingFileThrows() throws IOException {
        File missing = new File(tempDir.toFile(), "missing.avro");
        assertThrows(IOException.class, () -> AvroInputFormatCompat.analyzeFile(missing));

        System.setProperty(SPLIT_SIZE_KEY, "1024");
        File f = writeMultiBlock(200);
        long len = f.length();
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "rw")) {
            raf.setLength(len / 2); // truncate inside the data blocks
        }
        assertThrows(IOException.class, () -> AvroInputFormatCompat.computeSplits(f),
                "a truncated Avro file must be reported as corrupt during the block scan");
    }

    @Test
    void nonAvroFileRejected() throws IOException {
        File notAvro = new File(tempDir.toFile(), "not-avro.bin");
        java.nio.file.Files.writeString(notAvro.toPath(), "this is not avro data");
        assertThrows(IOException.class, () -> AvroInputFormatCompat.analyzeFile(notAvro));
    }

    // ─── Configuration ─────────────────────────────────────────────

    @Test
    void configResolutionDefaultsAndSysprop() throws IOException {
        System.clearProperty(SPLIT_SIZE_KEY);
        System.clearProperty(MIN_SIZE_KEY);
        assertEquals(AvroInputFormatCompat.DEFAULT_SPLIT_SIZE, AvroInputFormatCompat.resolveSplitSize());
        assertEquals(AvroInputFormatCompat.DEFAULT_MIN_SPLIT_SIZE, AvroInputFormatCompat.resolveMinSplitSize());

        System.setProperty(SPLIT_SIZE_KEY, "1234");
        System.setProperty(MIN_SIZE_KEY, "567");
        assertEquals(1234, AvroInputFormatCompat.resolveSplitSize());
        assertEquals(567, AvroInputFormatCompat.resolveMinSplitSize());
    }

    @Test
    void generatedSplitsAreIndependentObjects() throws IOException {
        System.setProperty(SPLIT_SIZE_KEY, "1024");
        System.setProperty(MIN_SIZE_KEY, "128");
        File f = writeMultiBlock(300);
        List<AvroInputFormatCompat.SplitDescriptor> a = AvroInputFormatCompat.computeSplits(f);
        List<AvroInputFormatCompat.SplitDescriptor> b = AvroInputFormatCompat.computeSplits(f);
        assertEquals(a, b, "repeated split computation must be deterministic");
        assertRowsEqual(sequentialRead(f, cols()), readAllSplits(f, a));
    }
}