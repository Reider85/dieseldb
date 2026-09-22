package diesel;

import diesel.storage.avro.AvroDatePartitioner;
import diesel.storage.avro.AvroRowStorage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@StorageType("avro")
class AvroDatePartitionerTest {

    @TempDir
    Path tempDir;

    private AvroRowStorage storage;
    private static final String TABLE_NAME = "test_table";

    @BeforeEach
    void setUp() {
        // Create storage instance
        storage = new AvroRowStorage(TABLE_NAME, 
            Arrays.asList("id", "name", "event_date", "amount"),
            createColumnTypes());
        storage.setDataDir(tempDir.resolve("data").toString());
    }

    private Map<String, Class<?>> createColumnTypes() {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("id", Integer.class);
        types.put("name", String.class);
        types.put("event_date", LocalDate.class);
        types.put("amount", BigDecimal.class);
        return types;
    }

    @AfterEach
    void tearDown() {
        // Clean up
        try {
            Files.walk(tempDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(path -> {
                     try {
                         Files.delete(path);
                     } catch (IOException e) {
                         // Ignore cleanup errors
                     }
                 });
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    @Test
    void testPartitionConfigDefaults() {
        AvroDatePartitioner.PartitionConfig config = new AvroDatePartitioner.PartitionConfig(
            false, "event_date", AvroDatePartitioner.Granularity.DAY, ZoneId.systemDefault()
        );
        
        assertFalse(config.isEnabled());
        assertEquals("event_date", config.getPartitionColumn());
        assertEquals(AvroDatePartitioner.Granularity.DAY, config.getGranularity());
        assertNotNull(config.getZoneId());
    }

    @Test
    void testPartitionConfigEnabled() {
        AvroDatePartitioner.PartitionConfig config = new AvroDatePartitioner.PartitionConfig(
            true, "event_date", AvroDatePartitioner.Granularity.MONTH, ZoneId.of("UTC")
        );
        
        assertTrue(config.isEnabled());
        assertEquals("event_date", config.getPartitionColumn());
        assertEquals(AvroDatePartitioner.Granularity.MONTH, config.getGranularity());
        assertEquals(ZoneId.of("UTC"), config.getZoneId());
    }

    @Test
    void testEnsurePartitionExistsCreatesDirectory() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        
        partitioner.ensurePartitionExists(TABLE_NAME, testDate);
        
        Path partitionDir = partitioner.getPartitionDirectory(TABLE_NAME, testDate);
        assertNotNull(partitionDir);
        assertTrue(Files.exists(partitionDir));
        assertTrue(Files.isDirectory(partitionDir));
    }

    @Test
    void testEnsurePartitionExistsDoesNotCreateWhenDisabled() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createDisabledConfig());
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        
        partitioner.ensurePartitionExists(TABLE_NAME, testDate);
        
        Path partitionDir = partitioner.getPartitionDirectory(TABLE_NAME, testDate);
        assertNull(partitionDir);
    }

    @Test
    void testResolvePartitionDirWithLocalDate() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDate);
        assertNotNull(partitionDir);
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testResolvePartitionDirWithLocalDateTime() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDateTime testDateTime = LocalDateTime.of(2023, 12, 1, 14, 30);
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDateTime);
        assertNotNull(partitionDir);
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testResolvePartitionDirWithString() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        String testDateStr = "2023-12-01";
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDateStr);
        assertNotNull(partitionDir);
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testResolvePartitionDirWithEpochDay() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        Integer epochDay = (int) LocalDate.of(2023, 12, 1).toEpochDay();
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, epochDay);
        assertNotNull(partitionDir);
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testResolvePartitionDirWithEpochMillis() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        Long epochMillis = LocalDate.of(2023, 12, 1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, epochMillis);
        assertNotNull(partitionDir);
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testResolvePartitionDirWithNullValue() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, null);
        assertNull(partitionDir);
    }

    @Test
    void testResolvePartitionDirWithUnsupportedType() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, new Object());
        assertNull(partitionDir);
    }

    @Test
    void testGranularityDay() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, 
            new AvroDatePartitioner.PartitionConfig(true, "event_date", AvroDatePartitioner.Granularity.DAY, ZoneId.systemDefault()));
        
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDate);
        
        assertNotNull(partitionDir);
        assertEquals("dt=2023-12-01", partitionDir.getFileName().toString());
    }

    @Test
    void testGranularityMonth() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, 
            new AvroDatePartitioner.PartitionConfig(true, "event_date", AvroDatePartitioner.Granularity.MONTH, ZoneId.systemDefault()));
        
        LocalDate testDate = LocalDate.of(2023, 12, 15);
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDate);
        
        assertNotNull(partitionDir);
        assertEquals("dt=2023-12", partitionDir.getFileName().toString());
    }

    @Test
    void testGranularityYear() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, 
            new AvroDatePartitioner.PartitionConfig(true, "event_date", AvroDatePartitioner.Granularity.YEAR, ZoneId.systemDefault()));
        
        LocalDate testDate = LocalDate.of(2023, 12, 15);
        Path partitionDir = partitioner.resolvePartitionDir(TABLE_NAME, testDate);
        
        assertNotNull(partitionDir);
        assertEquals("dt=2023", partitionDir.getFileName().toString());
    }

    @Test
    void testListPartitionsWhenNoPartitionsExist() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertTrue(partitions.isEmpty());
    }

    @Test
    void testListPartitionsWhenPartitionsExist() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        // Create some partitions
        LocalDate date1 = LocalDate.of(2023, 12, 1);
        LocalDate date2 = LocalDate.of(2023, 12, 2);
        
        partitioner.ensurePartitionExists(TABLE_NAME, date1);
        partitioner.ensurePartitionExists(TABLE_NAME, date2);
        
        List<Path> partitions = partitioner.listPartitions(TABLE_NAME);
        assertEquals(2, partitions.size());
        
        assertTrue(partitions.stream().anyMatch(p -> p.getFileName().toString().equals("dt=2023-12-01")));
        assertTrue(partitions.stream().anyMatch(p -> p.getFileName().toString().equals("dt=2023-12-02")));
    }

    @Test
    void testListPartitionDates() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        // Create some partitions
        LocalDate date1 = LocalDate.of(2023, 12, 1);
        LocalDate date2 = LocalDate.of(2023, 12, 2);
        
        partitioner.ensurePartitionExists(TABLE_NAME, date1);
        partitioner.ensurePartitionExists(TABLE_NAME, date2);
        
        List<LocalDate> dates = partitioner.listPartitionDates(TABLE_NAME);
        assertEquals(2, dates.size());
        assertTrue(dates.contains(date1));
        assertTrue(dates.contains(date2));
    }

    @Test
    void testPartitionExists() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        
        // Partition doesn't exist yet
        assertFalse(partitioner.partitionExists(TABLE_NAME, testDate));
        
        // Create partition
        partitioner.ensurePartitionExists(TABLE_NAME, testDate);
        
        // Now it exists
        assertTrue(partitioner.partitionExists(TABLE_NAME, testDate));
    }

    @Test
    void testGetPartitionForRow() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDate testDate = LocalDate.of(2023, 12, 1);
        
        // Partition path is computed regardless of existence on disk
        Path partitionDir = partitioner.getPartitionForRow(TABLE_NAME, testDate);
        assertNotNull(partitionDir);
        assertFalse(Files.exists(partitionDir)); // Partition does not exist yet on disk
        
        partitioner.ensurePartitionExists(TABLE_NAME, testDate);
        
        partitionDir = partitioner.getPartitionForRow(TABLE_NAME, testDate);
        assertNotNull(partitionDir);
        assertTrue(Files.exists(partitionDir));
        assertTrue(partitionDir.getFileName().toString().contains("dt=2023-12-01"));
    }

    @Test
    void testGetPartitionsForDateRange() throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        
        // Create partitions for different dates
        LocalDate date1 = LocalDate.of(2023, 11, 30);
        LocalDate date2 = LocalDate.of(2023, 12, 1);
        LocalDate date3 = LocalDate.of(2023, 12, 2);
        LocalDate date4 = LocalDate.of(2023, 12, 15);
        
        partitioner.ensurePartitionExists(TABLE_NAME, date1);
        partitioner.ensurePartitionExists(TABLE_NAME, date2);
        partitioner.ensurePartitionExists(TABLE_NAME, date3);
        partitioner.ensurePartitionExists(TABLE_NAME, date4);
        
        LocalDate startDate = LocalDate.of(2023, 12, 1);
        LocalDate endDate = LocalDate.of(2023, 12, 15);
        
        List<Path> partitions = partitioner.getPartitionsForDateRange(TABLE_NAME, startDate, endDate);
        assertEquals(3, partitions.size());
        
        assertTrue(partitions.stream().anyMatch(p -> p.getFileName().toString().equals("dt=2023-12-01")));
        assertTrue(partitions.stream().anyMatch(p -> p.getFileName().toString().equals("dt=2023-12-02")));
        assertTrue(partitions.stream().anyMatch(p -> p.getFileName().toString().equals("dt=2023-12-15")));
    }

    @ParameterizedTest
    @CsvSource({
        "2023-12-01, 2023-12-01, true",
        "2023-12-01T14:30:00, 2023-12-01, true",
        "2023-12-01 14:30:00, 2023-12-01, true",
        "1701388800000, 2023-12-01, true",
        "19692, 2023-12-01, true",
        "2023-12-02, 2023-12-01, false",
        "2023-11-30, 2023-12-01, false"
    })
    void testDateParsing(String input, String expectedDateStr, boolean shouldMatch) throws Exception {
        AvroDatePartitioner partitioner = new AvroDatePartitioner(storage, createEnabledConfig());
        LocalDate expectedDate = LocalDate.parse(expectedDateStr);
        
        LocalDate actualDate = extractDateFromValue(partitioner, input);
        
        if (shouldMatch) {
            assertEquals(expectedDate, actualDate);
        } else {
            assertNotEquals(expectedDate, actualDate);
        }
    }

    private AvroDatePartitioner.PartitionConfig createEnabledConfig() {
        return new AvroDatePartitioner.PartitionConfig(
            true, "event_date", AvroDatePartitioner.Granularity.DAY, ZoneId.systemDefault()
        );
    }

    private AvroDatePartitioner.PartitionConfig createDisabledConfig() {
        return new AvroDatePartitioner.PartitionConfig(
            false, "event_date", AvroDatePartitioner.Granularity.DAY, ZoneId.systemDefault()
        );
    }

    // Helper method to access private extractDateFromValue method via reflection
    private LocalDate extractDateFromValue(AvroDatePartitioner partitioner, Object value) {
        try {
            java.lang.reflect.Method method = AvroDatePartitioner.class.getDeclaredMethod("extractDateFromValue", Object.class);
            method.setAccessible(true);
            return (LocalDate) method.invoke(partitioner, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract date from value", e);
        }
    }
}