package diesel;

import diesel.storage.avro.AvroBlockConfig;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroParallelReader;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 69 AVRO parallel reading tests: block index scan, weighted
 * load balancing between blocks of different sizes, deterministic merge
 * across thread counts, projection pushdown, and the sequential fallback
 * below the {@code avro.parallel.read.threshold}.
 */
@Tag("storage")
@StorageType("avro")
class AvroParallelReaderTest {

    private static final String THRESHOLD_KEY = "avro.parallel.read.threshold";

    @TempDir
    Path tempDir;

    private String prevThreshold;
    private String prevSync;

    @BeforeEach
    void saveProps() {
        prevThreshold = System.getProperty(THRESHOLD_KEY);
        prevSync = System.getProperty("avro.block.sync.interval");
    }

    @AfterEach
    void restoreProps() {
        restore(THRESHOLD_KEY, prevThreshold);
        restore("avro.block.sync.interval", prevSync);
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

    /** Writes a multi-block file (small sync interval) and returns it. */
    private File writeMultiBlock(int rows) throws IOException {
        System.setProperty("avro.block.sync.interval", "512");
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

    private static Map<String, Class<?>> typesOf(Schema schema) {
        Map<String, Class<?>> m = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Schema.Field f : schema.getFields()) {
            m.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
        }
        return m;
    }

    /** Sequential reference read through the existing streaming reader. */
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

    /** Sequential reference read with projection. */
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

    // ─── Parallel correctness ──────────────────────────────────────

    @Test
    void parallelReadEqualsSequential() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(500);

        try (AvroParallelReader reader = new AvroParallelReader(f)) {
            assertTrue(reader.isParallel(), "multi-block file above threshold should read in parallel");
            assertTrue(reader.blockCount() > 1);
            assertEquals(500, reader.rowCount());
            assertRowsEqual(sequentialRead(f, cols()), reader.readAll());
        }
    }

    @Test
    void deterministicAcrossThreadCounts() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(500);

        List<Object[]> r1;
        List<Object[]> r2;
        List<Object[]> r4;
        try (AvroParallelReader a = new AvroParallelReader(f, 1);
             AvroParallelReader b = new AvroParallelReader(f, 2);
             AvroParallelReader c = new AvroParallelReader(f, 4)) {
            r1 = a.readAll();
            r2 = b.readAll();
            r4 = c.readAll();
        }
        assertRowsEqual(r1, r2);
        assertRowsEqual(r1, r4);
    }

    @Test
    void projectionPushdownMatchesSequential() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(500);
        List<String> proj = List.of("NAME", "ID");

        try (AvroParallelReader reader = new AvroParallelReader(f)) {
            assertRowsEqual(sequentialProjected(f, proj), reader.readProjected(proj));
        }
    }

    @Test
    void singleBlockFileNeverParallel() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        System.clearProperty("avro.block.sync.interval");
        File f = new File(tempDir.toFile(), "single.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (int i = 0; i < 50; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }

        try (AvroParallelReader reader = new AvroParallelReader(f)) {
            assertFalse(reader.isParallel(), "a single-block file must fall back to sequential reading");
            assertEquals(1, reader.blockCount());
            assertEquals(1, reader.getPartitionCount());
            assertRowsEqual(sequentialRead(f, cols()), reader.readAll());
        }
    }

    @Test
    void belowThresholdFallsBackToSequential() throws IOException {
        // no sysprop -> config.properties avro.parallel.read.threshold (10000)
        System.clearProperty(THRESHOLD_KEY);
        File f = writeMultiBlock(100); // 100 rows < 10000, still multi-block

        try (AvroParallelReader reader = new AvroParallelReader(f)) {
            assertEquals(10000, reader.getThreshold());
            assertFalse(reader.isParallel(), "row count below threshold must fall back to sequential reading");
            assertEquals(1, reader.getPartitionCount());
            assertRowsEqual(sequentialRead(f, cols()), reader.readAll());
        }
    }

    @Test
    void headerOnlyFileReturnsEmpty() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = new File(tempDir.toFile(), "header.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        w.close(); // zero rows -> header + trailing sync only

        try (AvroParallelReader reader = new AvroParallelReader(f)) {
            assertEquals(0, reader.blockCount());
            assertEquals(0, reader.rowCount());
            assertFalse(reader.isParallel());
            assertTrue(reader.readAll().isEmpty());
        }
    }

    // ─── Load balancing ────────────────────────────────────────────

    @Test
    void partitionLoadsReflectPayloadWeights() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(500);

        try (AvroParallelReader reader = new AvroParallelReader(f, 4)) {
            assertTrue(reader.isParallel());
            assertEquals(reader.getPartitionCount(), reader.getPartitionLoads().size());
            long sum = 0;
            for (long load : reader.getPartitionLoads()) {
                assertTrue(load > 0, "every partition must have positive load");
                sum += load;
            }
            assertEquals(reader.getTotalPayloadBytes(), sum);
        }
    }

    @Test
    void buildPartitionsBalancesSkewedBlockSizes() {
        List<AvroParallelReader.BlockEntry> blocks = List.of(
                new AvroParallelReader.BlockEntry(0, 10, 512),
                new AvroParallelReader.BlockEntry(1, 10, 512),
                new AvroParallelReader.BlockEntry(2, 10, 512),
                new AvroParallelReader.BlockEntry(3, 50, 9216),
                new AvroParallelReader.BlockEntry(4, 50, 9216),
                new AvroParallelReader.BlockEntry(5, 10, 512),
                new AvroParallelReader.BlockEntry(6, 10, 512));

        List<int[]> partitions = AvroParallelReader.buildPartitions(blocks, 3);
        assertEquals(3, partitions.size(), "three workers must yield three non-empty partitions");
        long total = blocks.stream().mapToLong(b -> b.payloadSize).sum();
        long maxBlock = blocks.stream().mapToLong(b -> b.payloadSize).max().orElseThrow();

        // coverage: partitions are contiguous and cover every block exactly once
        int prev = 0;
        for (int[] p : partitions) {
            assertEquals(prev, p[0], "partitions must be contiguous and non-overlapping");
            assertTrue(p[1] > p[0], "every partition must own at least one block");
            prev = p[1];
        }
        assertEquals(blocks.size(), prev, "partitions must cover all blocks");

        long maxLoad = 0;
        for (int[] p : partitions) {
            long load = 0;
            for (int i = p[0]; i < p[1]; i++) {
                load += blocks.get(i).payloadSize;
            }
            maxLoad = Math.max(maxLoad, load);
        }
        // algorithmic bound: no partition exceeds the ideal share + the heaviest block
        assertTrue(maxLoad <= total / partitions.size() + maxBlock,
                "partition load must stay within the ideal share plus the heaviest block");
        assertTrue(maxLoad < total, "no partition may claim the whole file");
    }

    @Test
    void buildPartitionsCoversSingleWorkerAndSingleBlock() {
        List<AvroParallelReader.BlockEntry> one = List.of(
                new AvroParallelReader.BlockEntry(10, 3, 100));
        List<int[]> p1 = AvroParallelReader.buildPartitions(one, 4);
        assertEquals(1, p1.size());
        assertArrayEquals(new int[]{0, 1}, p1.get(0));

        List<int[]> p2 = AvroParallelReader.buildPartitions(List.of(), 4);
        assertTrue(p2.isEmpty());
    }

    // ─── Failure handling ──────────────────────────────────────────

    @Test
    void corruptFileThrows() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(200);
        long len = f.length();
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "rw")) {
            raf.setLength(len / 2); // truncate inside the data blocks
        }
        assertThrows(IOException.class, () -> new AvroParallelReader(f),
                "a truncated Avro file must be reported as corrupt during the block scan");
    }

    @Test
    void closeIsIdempotentAndBlocksReads() throws IOException {
        System.setProperty(THRESHOLD_KEY, "1");
        File f = writeMultiBlock(50);

        AvroParallelReader reader = new AvroParallelReader(f);
        assertNotNull(reader.readAll());
        reader.close();
        reader.close(); // second close is a no-op
        assertThrows(IOException.class, reader::readAll, "reads after close must be rejected");
    }
}