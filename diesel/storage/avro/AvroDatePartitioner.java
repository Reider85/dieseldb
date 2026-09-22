package diesel.storage.avro;

import diesel.storage.AbstractRowStorage;
import diesel.storage.StorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

/**
 * AVRO date partitioning for DieselDB with automatic partition creation, pruning, and configurable granularity.
 * Partitioning follows Hive-style directory structure: data/avro/table_name/dt=YYYY-MM-DD/
 * 
 * @since Prompt 88
 */
public class AvroDatePartitioner {
    private static final Logger logger = LoggerFactory.getLogger(AvroDatePartitioner.class);
    private static final DateTimeFormatter SPACE_DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public enum Granularity {
        DAY, MONTH, YEAR
    }
    
    public static class PartitionConfig {
        private final boolean enabled;
        private final String partitionColumn;
        private final Granularity granularity;
        private final ZoneId zoneId;
        
        public PartitionConfig(boolean enabled, String partitionColumn, Granularity granularity, ZoneId zoneId) {
            this.enabled = enabled;
            this.partitionColumn = partitionColumn;
            this.granularity = granularity;
            this.zoneId = zoneId != null ? zoneId : ZoneId.systemDefault();
        }
        
        public boolean isEnabled() { return enabled; }
        public String getPartitionColumn() { return partitionColumn; }
        public Granularity getGranularity() { return granularity; }
        public ZoneId getZoneId() { return zoneId; }
    }
    
    private final AbstractRowStorage storage;
    private final PartitionConfig config;
    
    public AvroDatePartitioner(AbstractRowStorage storage, PartitionConfig config) {
        this.storage = storage;
        this.config = config;
    }
    
    /**
     * Creates a partition directory if it doesn't exist
     */
    public void ensurePartitionExists(String tableName, Object partitionValue) throws IOException {
        if (!config.isEnabled()) {
            return;
        }
        
        Path partitionDir = resolvePartitionDir(tableName, partitionValue);
        if (!Files.exists(partitionDir)) {
            Files.createDirectories(partitionDir);
            logger.info("Created partition directory: {}", partitionDir);
        }
    }
    
    /**
     * Resolves the partition directory path for a given partition value
     */
    public Path resolvePartitionDir(String tableName, Object partitionValue) {
        if (!config.isEnabled() || partitionValue == null) {
            return null;
        }
        
        LocalDate partitionDate = extractDateFromValue(partitionValue);
        if (partitionDate == null) {
            return null;
        }
        
        String partitionKey = getPartitionKey(partitionDate);
        Path dataDir = getDataDir(tableName);
        return dataDir.resolve(tableName).resolve(partitionKey);
    }
    
    /**
     * Extracts date from various value types
     */
    private LocalDate extractDateFromValue(Object value) {
        if (value == null) {
            return null;
        }
        
        try {
            if (value instanceof LocalDate) {
                return (LocalDate) value;
            } else if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).toLocalDate();
            } else if (value instanceof String) {
                return parseDateString((String) value);
            } else if (value instanceof Integer) {
                return LocalDate.ofEpochDay((Integer) value);
            } else if (value instanceof Long) {
                return Instant.ofEpochMilli((Long) value).atZone(config.getZoneId()).toLocalDate();
            } else {
                logger.warn("Unsupported partition value type: {}", value.getClass());
                return null;
            }
        } catch (Exception e) {
            logger.warn("Failed to extract date from partition value: {}", value, e);
            return null;
        }
    }
    
    /**
     * Parses date string in various ISO-8601 formats, plus space-separated
     * datetime and numeric epoch values.
     */
    private LocalDate parseDateString(String dateString) {
        String trimmed = dateString.trim();
        try {
            // Try standard ISO-8601 format first
            return LocalDate.parse(trimmed);
        } catch (DateTimeParseException e1) {
            try {
                // Try LocalDateTime format (ISO with 'T' separator)
                LocalDateTime dateTime = LocalDateTime.parse(trimmed);
                return dateTime.toLocalDate();
            } catch (DateTimeParseException e2) {
                try {
                    // Try LocalDateTime with a space separator
                    LocalDateTime dateTime = LocalDateTime.parse(trimmed, SPACE_DATE_TIME);
                    return dateTime.toLocalDate();
                } catch (DateTimeParseException e3) {
                    try {
                        // Try with time zone
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(trimmed);
                        return zonedDateTime.toLocalDate();
                    } catch (DateTimeParseException e4) {
try {
                        // Numeric epoch: epoch-day (small) or epoch-millis (large).
                        long numeric = Long.parseLong(trimmed);
                        if (Math.abs(numeric) < 100_000_000_000L) {
                            return LocalDate.ofEpochDay(numeric);
                        }
                        return Instant.ofEpochMilli(numeric)
                                .atZone(config.getZoneId()).toLocalDate();
                    } catch (NumberFormatException | DateTimeException nfe) {
                        logger.warn("Failed to parse date string: {}", dateString);
                        return null;
                    }
                    }
                }
            }
        }
    }
    
    /**
     * Gets the partition key (e.g., "dt=2023-12-01")
     */
    private String getPartitionKey(LocalDate date) {
        String formattedDate;
        switch (config.getGranularity()) {
            case DAY:
                formattedDate = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
                break;
            case MONTH:
                formattedDate = date.format(DateTimeFormatter.ofPattern("yyyy-MM"));
                break;
            case YEAR:
                formattedDate = Integer.toString(date.getYear());
                break;
            default:
                formattedDate = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        return "dt=" + formattedDate;
    }
    
    /**
     * Gets the data directory for AVRO storage
     */
    private Path getDataDir(String tableName) {
        try {
            // Extract dataDir from storage using reflection
            java.lang.reflect.Method method = AbstractRowStorage.class.getDeclaredMethod("resolveFilePath", String.class);
            method.setAccessible(true);
            String filePath = (String) method.invoke(storage, ".avro");
            return Paths.get(filePath).getParent().getParent();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get dataDir from storage", e);
        }
    }
    
    /**
     * Lists all available partitions for a table
     */
    public List<Path> listPartitions(String tableName) throws IOException {
        if (!config.isEnabled()) {
            return Collections.emptyList();
        }
        
        Path tableDir = getDataDir(tableName).resolve(tableName);
        if (!Files.exists(tableDir)) {
            return Collections.emptyList();
        }
        
        List<Path> partitions = new ArrayList<>();
        try (Stream<Path> stream = Files.list(tableDir)) {
            stream.filter(path -> path.getFileName().toString().startsWith("dt="))
                  .forEach(partitions::add);
        }
        return partitions;
    }
    
    /**
     * Lists all available partitions for a table with date parsing
     */
    public List<LocalDate> listPartitionDates(String tableName) throws IOException {
        List<Path> partitionPaths = listPartitions(tableName);
        List<LocalDate> dates = new ArrayList<>();
        
        for (Path path : partitionPaths) {
            String dirName = path.getFileName().toString();
            try {
                LocalDate date = parsePartitionDirectoryName(dirName);
                if (date != null) {
                    dates.add(date);
                }
            } catch (Exception e) {
                logger.warn("Failed to parse partition directory name: {}", dirName, e);
            }
        }
        
        return dates;
    }
    
    /**
     * Parses partition directory name to LocalDate
     */
    private LocalDate parsePartitionDirectoryName(String dirName) {
        if (!dirName.startsWith("dt=")) {
            return null;
        }
        
        String dateStr = dirName.substring(3);
        try {
            switch (config.getGranularity()) {
                case DAY:
                    return LocalDate.parse(dateStr);
                case MONTH:
                    return YearMonth.parse(dateStr + "-01").atDay(1);
                case YEAR:
                    return LocalDate.of(Integer.parseInt(dateStr), 1, 1);
                default:
                    return LocalDate.parse(dateStr);
            }
        } catch (DateTimeParseException e) {
            logger.warn("Failed to parse partition date from directory name: {}", dirName);
            return null;
        }
    }
    
    /**
     * Gets partition directory for a specific date
     */
    public Path getPartitionDirectory(String tableName, LocalDate date) {
        if (!config.isEnabled() || date == null) {
            return null;
        }
        
        String partitionKey = getPartitionKey(date);
        Path dataDir = getDataDir(tableName);
        return dataDir.resolve(tableName).resolve(partitionKey);
    }
    
    /**
     * Checks if a partition exists for a given date
     */
    public boolean partitionExists(String tableName, LocalDate date) throws IOException {
        Path partitionDir = getPartitionDirectory(tableName, date);
        return partitionDir != null && Files.exists(partitionDir);
    }
    
    /**
     * Gets the partition for a given row value
     */
    public Path getPartitionForRow(String tableName, Object partitionValue) {
        if (!config.isEnabled() || partitionValue == null) {
            return null;
        }
        
        LocalDate partitionDate = extractDateFromValue(partitionValue);
        if (partitionDate == null) {
            return null;
        }
        
        return getPartitionDirectory(tableName, partitionDate);
    }
    
    /**
     * Gets all partitions that contain data for the given date range
     */
    public List<Path> getPartitionsForDateRange(String tableName, LocalDate startDate, LocalDate endDate) throws IOException {
        List<Path> allPartitions = listPartitions(tableName);
        List<Path> result = new ArrayList<>();
        
        for (Path partition : allPartitions) {
            LocalDate partitionDate = parsePartitionDirectoryName(partition.getFileName().toString());
            if (partitionDate != null && !partitionDate.isBefore(startDate) && !partitionDate.isAfter(endDate)) {
                result.add(partition);
            }
        }
        
        return result;
    }
    
    /**
     * Prunes partitions outside the given date range
     */
    public void prunePartitionsOutsideRange(String tableName, LocalDate startDate, LocalDate endDate) throws IOException {
        List<Path> allPartitions = listPartitions(tableName);
        
        for (Path partition : allPartitions) {
            LocalDate partitionDate = parsePartitionDirectoryName(partition.getFileName().toString());
            if (partitionDate != null && (partitionDate.isBefore(startDate) || partitionDate.isAfter(endDate))) {
                try {
                    Files.walk(partition)
                         .sorted(Comparator.reverseOrder())
                         .forEach(path -> {
                             try {
                                 Files.delete(path);
                             } catch (IOException e) {
                                 logger.warn("Failed to delete partition file: {}", path, e);
                             }
                         });
                    logger.info("Pruned partition: {}", partition);
                } catch (IOException e) {
                    logger.warn("Failed to prune partition: {}", partition, e);
                }
            }
        }
    }
}