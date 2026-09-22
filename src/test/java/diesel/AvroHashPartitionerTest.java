package diesel;

import diesel.storage.avro.AvroHashPartitioner;
import diesel.storage.avro.AvroRowStorage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@StorageType("avro")
class AvroHashPartitionerTest {

    @TempDir
    Path tempDir;

    private AvroRowStorage storage;
    private static final String TABLE_NAME = "test_table";

    @BeforeEach
    void setUp() {
        storage = new AvroRowStorage(TABLE_NAME,
                Arrays.asList("id", "name", "event_date", "amount"),
                createColumnTypes());
        storage.setDataDir(tempDir.resolve("data").toString());
    }

    @AfterEach
    void tearDown() {
        try {
            if (Files.exists(tempDir)) {
                Files.walk(tempDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                // Ignore cleanup errors
                            }
                        });
            }
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    private Map<String, Class<?>> createColumnTypes() {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("id", Integer.class);
        types.put("name", String.class);
        types.put("event_date", LocalDate.class);
        types.put("amount", BigDecimal.class);
        return types;
    }

    private AvroHashPartitioner.HashPartitionConfig createEnabledConfig() {
        return new AvroHashPartitioner.HashPartitionConfig(
                true, "id", 4, AvroHashPartitioner.HashFunction.SIMPLE, 0);
    }

    private AvroHashPartitioner.HashPartitionConfig createDisabledConfig() {
        return new AvroHashPartitioner.HashPartitionConfig(
                false, "id", 4, AvroHashPartitioner.HashFunction.SIMPLE, 0);
    }

    private AvroHashPartitioner createEnabled(int count) {
        return new AvroHashPartitioner(storage,
                new AvroHashPartitioner.HashPartitionConfig(
                        true, "id", count, AvroHashPartitioner.HashFunction.SIMPLE, 0));
    }

    // ─── Config tests ───────────────────────────────────────────────

    @Test
    void testConfigDefaults() {
        AvroHashPartitioner.HashPartitionConfig config = createEnabledConfig();

        assertTrue(config.isEnabled());
        assertEquals("id", config.getPartitionColumn());
        assertEquals(4, config.getNumPartitions());
        assertEquals(AvroHashPartitioner.HashFunction.SIMPLE, config.getHashFunction());
        assertEquals(0, config.getSeed());
    }

    @Test
    void testConfigDisabled() {
        AvroHashPartitioner.HashPartitionConfig config = createDisabledConfig();
        assertFalse(config.isEnabled());
    }

    @Test
    void testConfigNumPartitionsClamped() {
        AvroHashPartitioner.HashPartitionConfig config = new AvroHashPartitioner.HashPartitionConfig(
                true, "id", 0, AvroHashPartitioner.HashFunction.SIMPLE, 0);
        assertEquals(1, config.getNumPartitions());
    }

    // ─── Hash function tests ────────────────────────────────────────

    @Test
    void testComputeHashDeterministicPerFunction() {
        for (AvroHashPartitioner.HashFunction function : AvroHashPartitioner.HashFunction.values()) {
            AvroHashPartitioner partitioner = new AvroHashPartitioner(storage,
                    new AvroHashPartitioner.HashPartitionConfig(
                            true, "id", 4, function, 7));
            int h1 = partitioner.computeHash("hello");
            int h2 = partitioner.computeHash("hello");
            assertEquals(h1, h2, "function " + function + " must be deterministic");
            assertTrue(h1 >= 0, "function " + function + " must be non-negative");
        }
    }

    @Test
    void testComputeHashNullReturnsZero() {
        AvroHashPartitioner partitioner = new AvroHashPartitioner(storage, createEnabledConfig());
        assertEquals(0, partitioner.computeHash(null));
    }

    @Test
    void testComputeHashSeedChangesHash() {
        AvroHashPartitioner p0 = new AvroHashPartitioner(storage,
                new AvroHashPartitioner.HashPartitionConfig(true, "id", 4,
                        AvroHashPartitioner.HashFunction.SIMPLE, 0));
        AvroHashPartitioner p1 = new AvroHashPartitioner(storage,
                new AvroHashPartitioner.HashPartitionConfig(true, "id", 4,
                        AvroHashPartitioner.HashFunction.SIMPLE, 123));
        if (p0.computeHash("sample") == p1.computeHash("sample")) {
            // Collision with this seed offset is possible but astronomically unlikely;
            // verify at least one seed differs across a small corpus.
            boolean anyDifferent = false;
            for (int i = 0; i < 1000; i++) {
                if (p0.computeHash("value-" + i) != p1.computeHash("value-" + i)) {
                    anyDifferent = true;
                    break;
                }
            }
            assertTrue(anyDifferent, "seed must perturb the hashes");
        }
    }

    @Test
    void testHashFunctionsDistributeEvenlyAcrossPartitions() {
        AvroHashPartitioner partitioner = new AvroHashPartitioner(storage, createEnabledConfig());
        int[] counts = new int[4];
        for (int i = 0; i < 2000; i++) {
            int index = partitioner.getPartitionIndex("key-" + i);
            counts[index]++;
        }
        for (int count : counts) {
            assertTrue(count > 300 && count < 700,
                    "expected near-uniform distribution, bucket had " + count);
        }
    }

    // ─── Partition index tests ──────────────────────────────────────

    @Test
    void testPartitionIndexRange() {
        AvroHashPartitioner partitioner = createEnabled(4);
        for (int i = 0; i < 1000; i++) {
            int index = partitioner.getPartitionIndex("value-" + i);
            assertTrue(index >= 0 && index < 4, "index out of range: " + index);
        }
    }

    @Test
    void testPartitionIndexDeterminism() {
        AvroHashPartitioner partitioner = createEnabled(4);
        assertEquals(partitioner.getPartitionIndex("same"),
                partitioner.getPartitionIndex("same"));
    }

    @Test
    void testPartitionIndexForCount() {
        AvroHashPartitioner partitioner = createEnabled(4);
        for (int i = 0; i < 100; i++) {
            int index = partitioner.getPartitionIndexForCount("value-" + i, 8);
            assertTrue(index >= 0 && index < 8);
        }
    }

    // ─── Directory tests ────────────────────────────────────────────

    @Test
    void testGetPartitionDirPath() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        Object value = 42;
        Path dir = partitioner.getPartitionDir(TABLE_NAME, value);
        assertNotNull(dir);
        int expected = partitioner.getPartitionIndex(value);
        assertEquals("part=" + expected, dir.getFileName().toString());
    }

    @Test
    void testGetPartitionDirNullValue() {
        AvroHashPartitioner partitioner = createEnabled(4);
        assertNull(partitioner.getPartitionDir(TABLE_NAME, null));
    }

    @Test
    void testGetPartitionDirDisabled() {
        AvroHashPartitioner partitioner = new AvroHashPartitioner(storage, createDisabledConfig());
        assertNull(partitioner.getPartitionDir(TABLE_NAME, 42));
    }

    @Test
    void testEnsurePartitionExistsCreatesDirectory() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        partitioner.ensurePartitionExists(TABLE_NAME, 42);

        Path dir = partitioner.getPartitionDir(TABLE_NAME, 42);
        assertNotNull(dir);
        assertTrue(Files.exists(dir));
        assertTrue(Files.isDirectory(dir));
    }

    @Test
    void testEnsurePartitionExistsDoesNotCreateWhenDisabled() throws Exception {
        AvroHashPartitioner partitioner = new AvroHashPartitioner(storage, createDisabledConfig());
        partitioner.ensurePartitionExists(TABLE_NAME, 42);
        assertNull(partitioner.getPartitionDir(TABLE_NAME, 42));
    }

    @Test
    void testPartitionExists() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        assertFalse(partitioner.partitionExists(TABLE_NAME, 42));

        partitioner.ensurePartitionExists(TABLE_NAME, 42);
        assertTrue(partitioner.partitionExists(TABLE_NAME, 42));
    }

    @Test
    void testGetPartitionForRowComputeOnly() {
        AvroHashPartitioner partitioner = createEnabled(4);
        Path dir = partitioner.getPartitionForRow(TABLE_NAME, 42);
        assertNotNull(dir);
        assertFalse(Files.exists(dir)); // compute-only, not on disk yet
    }

    // ─── Partition listing tests ────────────────────────────────────

    @Test
    void testListPartitionsEmpty() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertTrue(partitions.isEmpty());
    }

    @Test
    void testListPartitionsAfterCreate() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        partitioner.ensurePartitionExists(TABLE_NAME, 1);
        partitioner.ensurePartitionExists(TABLE_NAME, 2);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(2, partitions.size());
    }

    @Test
    void testListPartitionIndexes() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        for (int i = 0; i < 10; i++) {
            partitioner.ensurePartitionExists(TABLE_NAME, i);
        }

        List<Integer> indexes = partitioner.listPartitionIndexes(TABLE_NAME);
        assertFalse(indexes.isEmpty());
        // all indexes must be in range and unique
        Set<Integer> unique = new HashSet<>(indexes);
        assertEquals(indexes.size(), unique.size());
        for (Integer index : indexes) {
            assertTrue(index >= 0 && index < 4);
        }
        // sorted ascending
        List<Integer> sorted = new ArrayList<>(indexes);
        Collections.sort(sorted);
        assertEquals(sorted, indexes);
    }

    @Test
    void testListPartitionsSkipsNonPartitionDirs() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        Path base = getBaseDir();
        Files.createDirectories(base.resolve(TABLE_NAME));
        Files.createDirectories(base.resolve(TABLE_NAME).resolve("not_a_partition"));
        partitioner.ensurePartitionExists(TABLE_NAME, 5);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(1, partitions.size());
    }

    // ─── Rebalance tests ────────────────────────────────────────────

    @Test
    void testRebalanceNoOpWhenCountUnchanged() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        seedAllPartitions(TABLE_NAME, 4, 100);

        AvroHashPartitioner.RebalanceReport report = partitioner.rebalance(TABLE_NAME, 4);
        assertTrue(report.success());
        assertEquals(4, report.newCount());
        assertEquals(0, report.rowsMoved());
        assertEquals(0, report.partitionsCreated());
        assertEquals(0, report.partitionsRemoved());
    }

    @Test
    void testRebalanceDisabledReturnsNoOp() throws Exception {
        AvroHashPartitioner partitioner = new AvroHashPartitioner(storage, createDisabledConfig());
        AvroHashPartitioner.RebalanceReport report = partitioner.rebalance(TABLE_NAME, 4);
        assertTrue(report.success());
        assertEquals(0, report.rowsMoved());
    }

    @Test
    void testRebalanceRedistributesRowsToNewCount() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        int rows = 500;
        seedAllPartitions(TABLE_NAME, 4, rows);

        AvroHashPartitioner.RebalanceReport report = partitioner.rebalance(TABLE_NAME, 8);
        assertTrue(report.success(), "errors: " + report.errors());
        assertEquals(4, report.oldCount());
        assertEquals(8, report.newCount());
        assertEquals(rows, report.rowsMoved());
        assertTrue(report.partitionsCreated() >= 1);

        // Old partitions must be gone
        List<Path> remaining = partitioner.listPartitions(TABLE_NAME);
        List<Integer> indexes = partitioner.listPartitionIndexes(TABLE_NAME);
        for (Integer index : indexes) {
            assertTrue(index >= 0 && index < 8, "index out of new range: " + index);
        }
        assertFalse(remaining.isEmpty());

        // Total rows preserved across new layout
        long totalRead = countRowsAcrossPartitions(TABLE_NAME, indexes);
        assertEquals(rows, totalRead);
    }

    @Test
    void testRebalanceReportFields() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        seedAllPartitions(TABLE_NAME, 4, 50);

        AvroHashPartitioner.RebalanceReport report = partitioner.rebalance(TABLE_NAME, 2);
        assertEquals(4, report.oldCount());
        assertEquals(2, report.newCount());
        assertTrue(report.rowsMoved() > 0);
        assertTrue(report.partitionsCreated() > 0);
        assertTrue(report.partitionsRemoved() > 0);
        assertTrue(report.success());
        assertTrue(report.errors().isEmpty());
    }

    // ─── Storage integration tests ──────────────────────────────────

    @Test
    void testStorageInsertAndHashPartition() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);

        // Seed 100 rows into the part=* layout via per-partition storages
        seedAllPartitions(TABLE_NAME, 4, 100);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertFalse(partitions.isEmpty());
        // all rows placed into some partition
        List<Integer> indexes = partitioner.listPartitionIndexes(TABLE_NAME);
        long total = countRowsAcrossPartitions(TABLE_NAME, indexes);
        assertEquals(100, total);
    }

    @Test
    void testMultipleInsertsFillAllPartitions() throws Exception {
        AvroHashPartitioner partitioner = createEnabled(4);
        seedAllPartitions(TABLE_NAME, 4, 1000);

        List<Integer> indexes = partitioner.listPartitionIndexes(TABLE_NAME);
        assertEquals(4, indexes.size(), "expected all 4 partitions to be populated");
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private Map<String, Object> row(int id, String name) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", id);
        map.put("name", name);
        map.put("event_date", java.time.LocalDate.of(2023, 1, 1));
        map.put("amount", BigDecimal.valueOf(10 + id));
        return map;
    }

    /**
     * Writes {@code count} rows distributed across {@code numPartitions}
     * partition directories using per-partition storage instances, mirroring
     * the producer layout the partitioner reads from.
     */
    private void seedAllPartitions(String tableName, int numPartitions, int count) throws IOException {
        Map<Integer, List<Map<String, Object>>> seeded = new HashMap<>();
        AvroHashPartitioner probe = createEnabled(numPartitions);
        for (int i = 0; i < count; i++) {
            Map<String, Object> r = row(i, "name" + i);
            int index = probe.getPartitionIndex(i);
            seeded.computeIfAbsent(index, k -> new ArrayList<>()).add(r);
        }
        for (Map.Entry<Integer, List<Map<String, Object>>> e : seeded.entrySet()) {
            Path partitionDir = getBaseDir().resolve(tableName).resolve("part=" + e.getKey());
            if (!Files.exists(partitionDir)) {
                Files.createDirectories(partitionDir);
            }
            AvroRowStorage partitionStorage = new AvroRowStorage(tableName,
                    Arrays.asList("id", "name", "event_date", "amount"),
                    createColumnTypes());
            partitionStorage.setDataDir(partitionDir.toString());
            partitionStorage.setRows(e.getValue());
            partitionStorage.saveToFile(tableName);
        }
    }

    private long countRowsAcrossPartitions(String tableName, List<Integer> indexes) throws IOException {
        long total = 0;
        Path base = getBaseDir();
        for (Integer index : indexes) {
            Path partitionDir = base.resolve(tableName).resolve("part=" + index);
            if (Files.exists(partitionDir)) {
                AvroRowStorage partitionStorage = new AvroRowStorage(tableName,
                        Arrays.asList("id", "name", "event_date", "amount"),
                        createColumnTypes());
                partitionStorage.setDataDir(partitionDir.toString());
                partitionStorage.loadFromFile(tableName);
                total += partitionStorage.scan().size();
            }
        }
        return total;
    }

    private Path getBaseDir() {
        return tempDir.resolve("data").getParent();
    }
}