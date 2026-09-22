package diesel;

import diesel.storage.avro.AvroRangePartitioner;
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
class AvroRangePartitionerTest {

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

    private AvroRangePartitioner.RangePartitionConfig createEnabledConfig() {
        return new AvroRangePartitioner.RangePartitionConfig(
                true, "id", 0.0, 9999.0, 1000.0);
    }

    private AvroRangePartitioner.RangePartitionConfig createDisabledConfig() {
        return new AvroRangePartitioner.RangePartitionConfig(
                false, "id", 0.0, 9999.0, 1000.0);
    }

    private AvroRangePartitioner createEnabled() {
        return new AvroRangePartitioner(storage, createEnabledConfig());
    }

    // ─── Config tests ───────────────────────────────────────────────

    @Test
    void testConfigDefaults() {
        AvroRangePartitioner.RangePartitionConfig config = createEnabledConfig();

        assertTrue(config.isEnabled());
        assertEquals("id", config.getPartitionColumn());
        assertEquals(0.0, config.getLowerBound());
        assertEquals(9999.0, config.getUpperBound());
        assertEquals(1000.0, config.getRangeSize());
    }

    @Test
    void testConfigDisabled() {
        AvroRangePartitioner.RangePartitionConfig config = createDisabledConfig();
        assertFalse(config.isEnabled());
    }

    @Test
    void testConfigNullColumnDefaultsToEmpty() {
        AvroRangePartitioner.RangePartitionConfig config =
                new AvroRangePartitioner.RangePartitionConfig(true, null, 0.0, 100.0, 10.0);
        assertEquals("", config.getPartitionColumn());
    }

    @Test
    void testConfigNullBoundsAutoDetect() {
        AvroRangePartitioner.RangePartitionConfig config =
                new AvroRangePartitioner.RangePartitionConfig(true, "id", null, null, 0);
        assertNull(config.getLowerBound());
        assertNull(config.getUpperBound());
        assertEquals(0, config.getRangeSize());
    }

    // ─── Boundary initialization tests ──────────────────────────────

    @Test
    void testAutoBoundariesGenerated() {
        AvroRangePartitioner partitioner = createEnabled();
        List<AvroRangePartitioner.RangeBoundary> boundaries = partitioner.getBoundaries();

        // 0..9999 with size 1000 => 10 boundaries
        assertEquals(10, boundaries.size());
        assertEquals(0.0, boundaries.get(0).lowerInclusive());
        assertEquals(999.0, boundaries.get(0).upperInclusive());
        assertEquals(9000.0, boundaries.get(9).lowerInclusive());
        assertEquals(9999.0, boundaries.get(9).upperInclusive());
    }

    @Test
    void testManualBoundariesWhenNoAutoDetect() {
        AvroRangePartitioner.RangePartitionConfig config =
                new AvroRangePartitioner.RangePartitionConfig(true, "id", null, null, 0);
        AvroRangePartitioner partitioner = new AvroRangePartitioner(storage, config);
        assertTrue(partitioner.getBoundaries().isEmpty());

        partitioner.addBoundary(0, 499);
        partitioner.addBoundary(500, 999);

        assertEquals(2, partitioner.getBoundaries().size());
        assertEquals(0, partitioner.getBoundaries().get(0).index());
        assertEquals(1, partitioner.getBoundaries().get(1).index());
    }

    @Test
    void testSetBoundariesReplacesExisting() {
        AvroRangePartitioner partitioner = createEnabled();
        assertEquals(10, partitioner.getBoundaries().size());

        partitioner.setBoundaries(List.of(
                new AvroRangePartitioner.RangeBoundary(0, 0, 100),
                new AvroRangePartitioner.RangeBoundary(1, 101, 200)));
        assertEquals(2, partitioner.getBoundaries().size());
    }

    @Test
    void testAddBoundaryRejectsInvalidRange() {
        AvroRangePartitioner.RangePartitionConfig config =
                new AvroRangePartitioner.RangePartitionConfig(true, "id", null, null, 0);
        AvroRangePartitioner partitioner = new AvroRangePartitioner(storage, config);

        assertThrows(IllegalArgumentException.class,
                () -> partitioner.addBoundary(500, 100));
    }

    // ─── Range resolution tests ─────────────────────────────────────

    @Test
    void testResolveRangeForValueWithinBounds() {
        AvroRangePartitioner partitioner = createEnabled();

        AvroRangePartitioner.RangeBoundary b = partitioner.resolveRangeForValue(0);
        assertNotNull(b);
        assertEquals(0.0, b.lowerInclusive());
        assertEquals(999.0, b.upperInclusive());

        b = partitioner.resolveRangeForValue(1500);
        assertNotNull(b);
        assertEquals(1000.0, b.lowerInclusive());
        assertEquals(1999.0, b.upperInclusive());
    }

    @Test
    void testResolveRangeForValueBoundaryValues() {
        AvroRangePartitioner partitioner = createEnabled();

        // Value exactly on lower boundary of first range
        assertNotNull(partitioner.resolveRangeForValue(0));
        // Value exactly on upper boundary of first range
        assertNotNull(partitioner.resolveRangeForValue(999));
        // Value exactly on lower boundary of second range
        assertNotNull(partitioner.resolveRangeForValue(1000));
        // Value exactly on upper bound of overall range
        assertNotNull(partitioner.resolveRangeForValue(9999));
    }

    @Test
    void testResolveRangeForValueOutsideAllBounds() {
        AvroRangePartitioner partitioner = createEnabled();
        assertNull(partitioner.resolveRangeForValue(-1));
        assertNull(partitioner.resolveRangeForValue(10000));
    }

    @Test
    void testResolveRangeForNullReturnsNull() {
        AvroRangePartitioner partitioner = createEnabled();
        assertNull(partitioner.resolveRangeForValue(null));
    }

    @Test
    void testResolveRangeForNonNumericReturnsNull() {
        AvroRangePartitioner partitioner = createEnabled();
        assertNull(partitioner.resolveRangeForValue("abc"));
        assertNull(partitioner.resolveRangeForValue(new Object()));
    }

    @Test
    void testResolveRangeForNumericString() {
        AvroRangePartitioner partitioner = createEnabled();
        AvroRangePartitioner.RangeBoundary b = partitioner.resolveRangeForValue("1500");
        assertNotNull(b);
        assertEquals(1000.0, b.lowerInclusive());
    }

    @Test
    void testResolveRangeForVariousNumericTypes() {
        AvroRangePartitioner partitioner = createEnabled();

        assertNotNull(partitioner.resolveRangeForValue(500L));
        assertNotNull(partitioner.resolveRangeForValue(500.5));
        assertNotNull(partitioner.resolveRangeForValue(new BigDecimal("500")));
        assertNotNull(partitioner.resolveRangeForValue((short) 500));
        assertNotNull(partitioner.resolveRangeForValue((byte) 50));
    }

    // ─── Directory tests ────────────────────────────────────────────

    @Test
    void testGetPartitionDirPath() {
        AvroRangePartitioner partitioner = createEnabled();
        Path dir = partitioner.getPartitionDir(TABLE_NAME, 1500);
        assertNotNull(dir);
        assertEquals("range=1000-1999", dir.getFileName().toString());
    }

    @Test
    void testGetPartitionDirNullValue() {
        AvroRangePartitioner partitioner = createEnabled();
        assertNull(partitioner.getPartitionDir(TABLE_NAME, null));
    }

    @Test
    void testGetPartitionDirOutOfRangeValue() {
        AvroRangePartitioner partitioner = createEnabled();
        assertNull(partitioner.getPartitionDir(TABLE_NAME, 10000));
    }

    @Test
    void testGetPartitionDirDisabled() {
        AvroRangePartitioner partitioner = new AvroRangePartitioner(storage, createDisabledConfig());
        assertNull(partitioner.getPartitionDir(TABLE_NAME, 42));
    }

    @Test
    void testEnsurePartitionExistsCreatesDirectory() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);

        Path dir = partitioner.getPartitionDir(TABLE_NAME, 1500);
        assertNotNull(dir);
        assertTrue(Files.exists(dir));
        assertTrue(Files.isDirectory(dir));
    }

    @Test
    void testEnsurePartitionExistsDoesNotCreateWhenDisabled() throws Exception {
        AvroRangePartitioner partitioner = new AvroRangePartitioner(storage, createDisabledConfig());
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);
        assertNull(partitioner.getPartitionDir(TABLE_NAME, 1500));
    }

    @Test
    void testEnsurePartitionExistsDoesNotCreateForNonNumeric() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, "abc");
        assertNull(partitioner.getPartitionDir(TABLE_NAME, "abc"));
    }

    @Test
    void testPartitionExists() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        assertFalse(partitioner.partitionExists(TABLE_NAME, 1500));

        partitioner.ensurePartitionExists(TABLE_NAME, 1500);
        assertTrue(partitioner.partitionExists(TABLE_NAME, 1500));
    }

    @Test
    void testGetPartitionForRowComputeOnly() {
        AvroRangePartitioner partitioner = createEnabled();
        Path dir = partitioner.getPartitionForRow(TABLE_NAME, 1500);
        assertNotNull(dir);
        assertFalse(Files.exists(dir));
    }

    // ─── Partition listing tests ────────────────────────────────────

    @Test
    void testListPartitionsEmpty() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertTrue(partitions.isEmpty());
    }

    @Test
    void testListPartitionsAfterCreate() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);
        partitioner.ensurePartitionExists(TABLE_NAME, 9500);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(3, partitions.size());
    }

    @Test
    void testListPartitionsSkipsNonPartitionDirs() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        Path tableDir = getBaseDir().resolve(TABLE_NAME);
        Files.createDirectories(tableDir);
        Files.createDirectories(tableDir.resolve("not_a_partition"));
        partitioner.ensurePartitionExists(TABLE_NAME, 500);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(1, partitions.size());
    }

    @Test
    void testListRangeBoundaries() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);

        List<AvroRangePartitioner.RangeBoundary> boundaries =
                partitioner.listRangeBoundaries(TABLE_NAME);
        assertEquals(2, boundaries.size());
    }

    // ─── Query pruning tests ────────────────────────────────────────

    @Test
    void testGetPartitionsForValueRangeOverlap() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);  // range 1000-1999
        partitioner.ensurePartitionExists(TABLE_NAME, 2500);  // range 2000-2999

        // Query [1500, 2800] overlaps ranges 1000-1999 and 2000-2999
        List<Path> result = partitioner.getPartitionsForValueRange(
                TABLE_NAME, 1500, 2800);
        assertEquals(2, result.size());
    }

    @Test
    void testGetPartitionsForValueRangeNoOverlap() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999

        List<Path> result = partitioner.getPartitionsForValueRange(
                TABLE_NAME, 5000, 6000);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetPartitionsForValueRangeExactBoundary() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999

        // Query [999, 999] touches only range 0-999
        List<Path> result = partitioner.getPartitionsForValueRange(
                TABLE_NAME, 999, 999);
        assertEquals(1, result.size());

        // Query [1000, 1000] touches only range 1000-1999 (if it exists)
        result = partitioner.getPartitionsForValueRange(TABLE_NAME, 1000, 1000);
        assertTrue(result.isEmpty()); // range 1000-1999 was not created
    }

    @Test
    void testGetPartitionsForValueRangeDisabled() throws Exception {
        AvroRangePartitioner partitioner =
                new AvroRangePartitioner(storage, createDisabledConfig());
        List<Path> result = partitioner.getPartitionsForValueRange(
                TABLE_NAME, 0, 10000);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetPartitionsForValueRangeReversedBounds() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);  // range 1000-1999

        // Reversed bounds should be swapped internally
        List<Path> result = partitioner.getPartitionsForValueRange(
                TABLE_NAME, 2800, 1500);
        assertEquals(1, result.size());
    }

    @Test
    void testPrunePartitionsOutsideRange() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);  // range 1000-1999
        partitioner.ensurePartitionExists(TABLE_NAME, 2500);  // range 2000-2999

        assertEquals(3, partitioner.listPartitions(TABLE_NAME).size());

        // Keep only [1200, 2700] => keeps 1000-1999 and 2000-2999
        partitioner.prunePartitionsOutsideRange(TABLE_NAME, 1200, 2700);

        List<Path> remaining = partitioner.listPartitions(TABLE_NAME);
        assertEquals(2, remaining.size());
        assertFalse(remaining.stream().anyMatch(
                p -> p.getFileName().toString().equals("range=0-999")));
    }

    // ─── Dynamic range adjustment (split) tests ─────────────────────

    @Test
    void testSplitRangeCreatesSubRanges() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999

        AvroRangePartitioner.RangeSplitReport report =
                partitioner.splitRange(TABLE_NAME, 0, 2);

        assertTrue(report.success(), "errors: " + report.errors());
        assertEquals(10, report.oldRangeCount());
        // Old range 0-999 removed, two sub-ranges added => 11
        assertEquals(11, report.newRangeCount());

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertTrue(partitions.stream().anyMatch(
                p -> p.getFileName().toString().contains("range=0-499")));
        assertTrue(partitions.stream().anyMatch(
                p -> p.getFileName().toString().contains("range=500-999")));
    }

    @Test
    void testSplitRangeInvalidCount() {
        AvroRangePartitioner partitioner = createEnabled();
        AvroRangePartitioner.RangeSplitReport report =
                partitioner.splitRange(TABLE_NAME, 0, 1);
        assertFalse(report.success());
        assertFalse(report.errors().isEmpty());
    }

    @Test
    void testSplitRangeInvalidIndex() {
        AvroRangePartitioner partitioner = createEnabled();
        AvroRangePartitioner.RangeSplitReport report =
                partitioner.splitRange(TABLE_NAME, 999, 2);
        assertFalse(report.success());
    }

    @Test
    void testSplitRangeDisabledReturnsNoOp() {
        AvroRangePartitioner partitioner =
                new AvroRangePartitioner(storage, createDisabledConfig());
        AvroRangePartitioner.RangeSplitReport report =
                partitioner.splitRange(TABLE_NAME, 0, 2);
        assertTrue(report.success());
        assertEquals(0, report.rowsMoved());
    }

    // ─── Dynamic range adjustment (merge) tests ─────────────────────

    @Test
    void testMergeRangesCombinesBoundaries() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        partitioner.ensurePartitionExists(TABLE_NAME, 100);   // range 0-999
        partitioner.ensurePartitionExists(TABLE_NAME, 1500);  // range 1000-1999

        assertEquals(10, partitioner.getBoundaries().size());

        AvroRangePartitioner.RangeSplitReport report =
                partitioner.mergeRanges(TABLE_NAME, 0, 1);

        assertTrue(report.success(), "errors: " + report.errors());
        // Two ranges merged into one => 9 boundaries
        assertEquals(9, report.newRangeCount());
    }

    @Test
    void testMergeRangesInvalidIndex() {
        AvroRangePartitioner partitioner = createEnabled();
        AvroRangePartitioner.RangeSplitReport report =
                partitioner.mergeRanges(TABLE_NAME, 0, 999);
        assertFalse(report.success());
        assertFalse(report.errors().isEmpty());
    }

    @Test
    void testMergeRangesDisabledReturnsNoOp() {
        AvroRangePartitioner partitioner =
                new AvroRangePartitioner(storage, createDisabledConfig());
        AvroRangePartitioner.RangeSplitReport report =
                partitioner.mergeRanges(TABLE_NAME, 0, 1);
        assertTrue(report.success());
        assertEquals(0, report.rowsMoved());
    }

    // ─── Partition introspection tests ──────────────────────────────

    @Test
    void testDescribePartitionsEmpty() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        List<AvroRangePartitioner.PartitionRangeInfo> infos =
                partitioner.describePartitions(TABLE_NAME);
        assertTrue(infos.isEmpty());
    }

    @Test
    void testDescribePartitionsWithRows() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        seedRangePartition(TABLE_NAME, "range=1000-1999",
                Arrays.asList(row(1000), row(1500), row(1999)));
        seedRangePartition(TABLE_NAME, "range=0-999",
                Arrays.asList(row(100), row(200)));

        List<AvroRangePartitioner.PartitionRangeInfo> infos =
                partitioner.describePartitions(TABLE_NAME);
        assertEquals(2, infos.size());
        // sorted by index: range 0-999 has index 0
        assertEquals(0, infos.get(0).rangeIndex());
        assertEquals(0.0, infos.get(0).lowerBound());
        assertEquals(999.0, infos.get(0).upperBound());
        assertEquals(2, infos.get(0).rowCount());
        assertEquals(1000.0, infos.get(1).lowerBound());
        assertEquals(3, infos.get(1).rowCount());
    }

    // ─── Storage integration tests ──────────────────────────────────

    @Test
    void testStorageInsertAndRangePartition() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        seedRangeRows(partitioner, TABLE_NAME, 500);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertFalse(partitions.isEmpty());

        // Count rows across all range partitions
        long total = 0;
        for (Path p : partitions) {
            AvroRowStorage partStorage = new AvroRowStorage(TABLE_NAME,
                    Arrays.asList("id", "name", "event_date", "amount"),
                    createColumnTypes());
            partStorage.setDataDir(p.toString());
            partStorage.loadFromFile(TABLE_NAME);
            total += partStorage.scan().size();
        }
        assertEquals(500, total);
    }

    @Test
    void testMultipleRangesPopulated() throws Exception {
        AvroRangePartitioner partitioner = createEnabled();
        // 5000 rows: id 0..4999 => ranges 0-999 .. 4000-4999 => 5 ranges
        seedRangeRows(partitioner, TABLE_NAME, 5000);

        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(5, partitions.size());
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private Map<String, Object> row(int id) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", id);
        map.put("name", "name" + id);
        map.put("event_date", LocalDate.of(2023, 1, 1));
        map.put("amount", BigDecimal.valueOf(10 + id));
        return map;
    }

    /**
     * Seeds rows into range partition directories using per-partition
     * storage instances, mirroring the producer layout the partitioner
     * reads from.
     */
    private void seedRangeRows(AvroRangePartitioner partitioner, String tableName,
                               int count) throws IOException {
        Map<String, List<Map<String, Object>>> seeded = new HashMap<>();
        for (int i = 0; i < count; i++) {
            Path dir = partitioner.getPartitionDir(tableName, i);
            if (dir == null) {
                continue;
            }
            String dirName = dir.getFileName().toString();
            seeded.computeIfAbsent(dirName, k -> new ArrayList<>()).add(row(i));
        }
        for (Map.Entry<String, List<Map<String, Object>>> e : seeded.entrySet()) {
            Path partitionDir = getBaseDir().resolve(tableName).resolve(e.getKey());
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

    private void seedRangePartition(String tableName, String dirName,
                                    List<Map<String, Object>> rows) throws IOException {
        Path partitionDir = getBaseDir().resolve(tableName).resolve(dirName);
        if (!Files.exists(partitionDir)) {
            Files.createDirectories(partitionDir);
        }
        AvroRowStorage partitionStorage = new AvroRowStorage(tableName,
                Arrays.asList("id", "name", "event_date", "amount"),
                createColumnTypes());
        partitionStorage.setDataDir(partitionDir.toString());
        partitionStorage.setRows(rows);
        partitionStorage.saveToFile(tableName);
    }

    private Path getBaseDir() {
        return tempDir.resolve("data").getParent();
    }
}
