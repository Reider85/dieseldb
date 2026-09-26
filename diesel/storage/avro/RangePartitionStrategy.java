package diesel.storage.avro;

import diesel.storage.AbstractRowStorage;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Strategy class for range partitioning operations in AvroRangePartitioner.
 * Extracts complex logic from splitRange, mergeRanges, and toNumeric methods
 * to reduce cognitive complexity (S3776) and eliminate brain methods (S6541).
 *
 * @since Prompt 9
 */
public class RangePartitionStrategy {

    private final AbstractRowStorage storage;
    private final String partitionColumn;

    public RangePartitionStrategy(AbstractRowStorage storage, String partitionColumn) {
        this.storage = storage;
        this.partitionColumn = partitionColumn;
    }

    /**
     * Computes sub-range boundaries for splitting a range into equal parts.
     *
     * @param target     the original range boundary to split
     * @param splitCount number of sub-ranges to create (must be >= 2)
     * @return list of sub-range boundaries
     */
    public List<AvroRangePartitioner.RangeBoundary> computeSubRangeBoundaries(
            AvroRangePartitioner.RangeBoundary target, int splitCount) {
        List<AvroRangePartitioner.RangeBoundary> subRanges = new ArrayList<>();
        double span = target.upperInclusive() - target.lowerInclusive() + 1;
        double subSize = span / splitCount;

        for (int i = 0; i < splitCount; i++) {
            double lo = target.lowerInclusive() + i * subSize;
            double hi = (i == splitCount - 1)
                    ? target.upperInclusive()
                    : lo + subSize - 1;
            subRanges.add(new AvroRangePartitioner.RangeBoundary(-1, lo, hi));
        }

        return subRanges;
    }

    /**
     * Redistributes rows from a source partition to target sub-range partitions.
     * Handles both creating new partitions and appending to existing ones.
     *
     * @param tableName        the table name
     * @param rows             rows to redistribute
     * @param subRanges        target sub-range boundaries
     * @param written          set of already-written partition directories (to track created partitions)
     * @return redistribution results
     */
public RedistributionResult redistributeRows(String tableName, 
                                                List<Map<String, Object>> rows,
                                                List<AvroRangePartitioner.RangeBoundary> subRanges,
                                                Set<Path> written) {
        List<String> errors = new ArrayList<>();
        int rangesCreated = 0;

        for (Map<String, Object> row : rows) {
            Object value = extractPartitionValue(row);
            Double numeric = toNumeric(value);
            AvroRangePartitioner.RangeBoundary targetSub = findTargetSubRange(numeric, subRanges);
            
            Path subDir = getPartitionDir(tableName, targetSub);
            try {
                if (!written.contains(subDir)) {
                    if (!Files.exists(subDir)) {
                        Files.createDirectories(subDir);
                    }
                    AvroRowStorage subStorage = newPartitionStorage(subDir.toString());
                    List<Map<String, Object>> existing = subStorage.scan();
                    existing.add(row);
                    subStorage.setRows(existing);
                    subStorage.saveToFile(tableName);
                    written.add(subDir);
                    rangesCreated++;
                } else {
                    AvroRowStorage subStorage = newPartitionStorage(subDir.toString());
                    subStorage.loadFromFile(tableName);
                    List<Map<String, Object>> existing = subStorage.scan();
                    existing.add(row);
                    subStorage.setRows(existing);
                    subStorage.saveToFile(tableName);
                }
            } catch (Exception e) {
                errors.add("Failed to write row to sub-range " + subDir + ": " + e.getMessage());
            }
        }

        return new RedistributionResult(rangesCreated, errors);
    }

    /**
     * Reads rows from a partition directory.
     *
     * @param tableName      the table name
     * @param partitionDir   the partition directory path
     * @return list of rows from the partition
     * @throws IOException on filesystem errors
     */
    public List<Map<String, Object>> readPartitionRows(String tableName, Path partitionDir) throws IOException {
        AvroRowStorage partitionStorage = newPartitionStorage(partitionDir.toString());
        partitionStorage.loadFromFile(tableName);
        return partitionStorage.scan();
    }

    /**
     * Writes rows to a partition directory.
     *
     * @param tableName      the table name
     * @param partitionDir   the partition directory path
     * @param rows           rows to write
     * @throws IOException on filesystem errors
     */
    public void writePartitionRows(String tableName, Path partitionDir, 
                                   List<Map<String, Object>> rows) throws IOException {
        if (!Files.exists(partitionDir)) {
            Files.createDirectories(partitionDir);
        }
        AvroRowStorage partitionStorage = newPartitionStorage(partitionDir.toString());
        partitionStorage.setRows(rows);
        partitionStorage.saveToFile(tableName);
    }

    /**
     * Converts any Object to a Double for range comparison using a strategy map.
     * Replaces the long if/else chain with a more maintainable approach.
     *
     * @param value the value to convert
     * @return Double representation or null if conversion fails
     */
    public Double toNumeric(Object value) {
        if (value == null) {
            return null;
        }

        // Strategy map for type conversion
        Map<Class<?>, Function<Object, Double>> converters = new HashMap<>();
        converters.put(Double.class, v -> (Double) v);
        converters.put(Float.class, v -> ((Float) v).doubleValue());
        converters.put(Integer.class, v -> ((Integer) v).doubleValue());
        converters.put(Long.class, v -> ((Long) v).doubleValue());
        converters.put(Short.class, v -> ((Short) v).doubleValue());
        converters.put(Byte.class, v -> ((Byte) v).doubleValue());
        converters.put(BigDecimal.class, v -> ((BigDecimal) v).doubleValue());
        converters.put(Number.class, v -> ((Number) v).doubleValue());

        Function<Object, Double> converter = converters.get(value.getClass());
        if (converter != null) {
            return converter.apply(value);
        }

        if (value instanceof String) {
            try {
                return Double.parseDouble(((String) value).trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return null;
    }

    /**
     * Reindexes a list of boundaries with sequential indexes.
     *
     * @param boundaries the boundaries to reindex
     */
    public void reindexBoundaries(List<AvroRangePartitioner.RangeBoundary> boundaries) {
        boundaries.sort((x, y) -> Double.compare(x.lowerInclusive(), y.lowerInclusive()));
        for (int i = 0; i < boundaries.size(); i++) {
            AvroRangePartitioner.RangeBoundary old = boundaries.get(i);
            boundaries.set(i, new AvroRangePartitioner.RangeBoundary(i,
                    old.lowerInclusive(), old.upperInclusive()));
        }
    }

    // Private helper methods

private AvroRangePartitioner.RangeBoundary findTargetSubRange(Double numeric, 
                                                            List<AvroRangePartitioner.RangeBoundary> subRanges) {
        if (numeric != null) {
            for (AvroRangePartitioner.RangeBoundary sub : subRanges) {
                if (numeric >= sub.lowerInclusive() && numeric <= sub.upperInclusive()) {
                    return sub;
                }
            }
        }
        // Row does not fit any sub-range — put it in the first sub-range
        return subRanges.get(0);
    }

    private Object extractPartitionValue(Map<String, Object> row) {
        if (partitionColumn.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(partitionColumn)) {
                return e.getValue();
            }
        }
        return null;
    }

    private Path getPartitionDir(String tableName, AvroRangePartitioner.RangeBoundary boundary) {
        Path dataDir = getDataDir(tableName);
        return dataDir.resolve(tableName).resolve("range=" + formatBoundary(boundary));
    }

    private String formatBoundary(AvroRangePartitioner.RangeBoundary boundary) {
        return String.format("%.0f-%.0f", boundary.lowerInclusive(), boundary.upperInclusive());
    }

    private AvroRowStorage newPartitionStorage(String partitionDir) {
        AvroRowStorage partitionStorage = new AvroRowStorage(
                storage.getTableName(), storage.getColumns(), storage.getColumnTypes());
        partitionStorage.setDataDir(partitionDir);
        return partitionStorage;
    }

    private Path getDataDir(String tableName) {
        try {
            java.lang.reflect.Method method = AbstractRowStorage.class
                    .getDeclaredMethod("resolveFilePath", String.class);
            method.setAccessible(true);
            String filePath = (String) method.invoke(storage, ".avro");
            return Path.of(filePath).getParent().getParent();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get dataDir from storage", e);
        }
    }

    // Inner records for results

    /**
     * Result of a row redistribution operation.
     *
     * @param rangesCreated number of new partition directories created
     * @param errors        error messages encountered during redistribution
     */
    public record RedistributionResult(int rangesCreated, List<String> errors) {}
}