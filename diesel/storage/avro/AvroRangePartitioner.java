package diesel.storage.avro;

import diesel.storage.AbstractRowStorage;
import diesel.storage.avro.RangePartitionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * AVRO range partitioning for DieselDB: distributes rows across partitions
 * based on numeric ranges of a chosen column. Partitioning follows a
 * Hive-style directory structure: {@code data/avro/table_name/range=L-U/}.
 *
 * <p>Features (prompt 90):
 * <ol>
 *   <li>Range partitioning by numeric columns — {@link #resolveRangeForValue(Object)}</li>
 *   <li>Range boundary definition — either explicit bounds or auto-detected</li>
 *   <li>Dynamic range adjustment — {@link #splitRange(String, int, int)} and
 *       {@link #mergeRanges(String, int, int)}</li>
 *   <li>Query pruning by ranges — {@link #getPartitionsForValueRange(String, Number, Number)}</li>
 * </ol>
 *
 * @since Prompt 90
 */
public class AvroRangePartitioner {

    private static final Logger logger = LoggerFactory.getLogger(AvroRangePartitioner.class);

    /** Directory name prefix for range partitions, e.g. {@code range=0-999}. */
    private static final String PARTITION_PREFIX = "range=";

    /**
     * Immutable configuration for range partitioning.
     *
     * @param enabled         whether partitioning is active
     * @param partitionColumn column whose numeric value drives the range lookup
     * @param lowerBound      lower bound of the overall range ({@code null} = auto-detect from data)
     * @param upperBound      upper bound of the overall range ({@code null} = auto-detect from data)
     * @param rangeSize       size of each range when auto-generating boundaries ({@code <= 0} = manual)
     */
    public static class RangePartitionConfig {
        private final boolean enabled;
        private final String partitionColumn;
        private final Double lowerBound;
        private final Double upperBound;
        private final double rangeSize;

        public RangePartitionConfig(boolean enabled, String partitionColumn,
                                    Double lowerBound, Double upperBound, double rangeSize) {
            this.enabled = enabled;
            this.partitionColumn = partitionColumn != null ? partitionColumn : "";
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.rangeSize = rangeSize;
        }

        public boolean isEnabled() { return enabled; }
        public String getPartitionColumn() { return partitionColumn; }
        public Double getLowerBound() { return lowerBound; }
        public Double getUpperBound() { return upperBound; }
        public double getRangeSize() { return rangeSize; }
    }

    /**
     * A single numeric range boundary.
     *
     * @param index          zero-based range index
     * @param lowerInclusive lower bound (inclusive)
     * @param upperInclusive upper bound (inclusive)
     */
    public record RangeBoundary(int index, double lowerInclusive, double upperInclusive) {
    }

    /**
     * Report of a {@link #splitRange(String, int, int)} or
     * {@link #mergeRanges(String, int, int)} operation.
     *
     * @param oldRangeCount  range count before the operation
     * @param newRangeCount  range count after the operation
     * @param rowsMoved      number of rows redistributed
     * @param rangesCreated  range directories written
     * @param rangesRemoved  stale range directories removed
     * @param success        whether the operation completed without errors
     * @param errors         per-range error messages exposed to the caller
     */
    public record RangeSplitReport(int oldRangeCount, int newRangeCount, long rowsMoved,
                                   int rangesCreated, int rangesRemoved,
                                   boolean success, List<String> errors) {
    }

    /**
     * Introspection info for one range partition on disk.
     *
     * @param rangeIndex    zero-based range index
     * @param lowerBound    lower bound of the range
     * @param upperBound    upper bound of the range
     * @param partitionPath absolute path of the range directory
     * @param rowCount      number of rows stored in this range (0 if unreadable)
     */
    public record PartitionRangeInfo(int rangeIndex, double lowerBound, double upperBound,
                                     Path partitionPath, long rowCount) {
    }

    private final AbstractRowStorage storage;
    private final RangePartitionConfig config;
    private final RangePartitionStrategy strategy;
    private final List<RangeBoundary> boundaries = new ArrayList<>();

    public AvroRangePartitioner(AbstractRowStorage storage, RangePartitionConfig config) {
        this.storage = storage;
        this.config = config;
        this.strategy = new RangePartitionStrategy(storage, config.getPartitionColumn());
        initBoundaries();
    }

    // ─── Boundary initialization ────────────────────────────────────

    /**
     * Builds the initial list of range boundaries from the configuration.
     * When {@code lowerBound}/{@code upperBound} and a positive
     * {@code rangeSize} are given, boundaries are auto-generated. Otherwise
     * the boundary list starts empty and is populated via
     * {@link #addBoundary(double, double)} or {@link #setBoundaries(List)}.
     */
    private void initBoundaries() {
        boundaries.clear();
        if (config.getLowerBound() == null || config.getUpperBound() == null
                || config.getRangeSize() <= 0) {
            return;
        }
        double low = config.getLowerBound();
        double high = config.getUpperBound();
        if (high <= low) {
            logger.warn("Invalid range bounds: lower={} upper={}", low, high);
            return;
        }
        int index = 0;
        double start = low;
        while (start < high) {
            double end = Math.min(start + config.getRangeSize() - 1, high);
            boundaries.add(new RangeBoundary(index++, start, end));
            start = end + 1;
        }
        // ensure last range covers the upper bound
        if (index > 0) {
            RangeBoundary last = boundaries.get(boundaries.size() - 1);
            if (last.upperInclusive() < high) {
                boundaries.set(boundaries.size() - 1,
                        new RangeBoundary(last.index(), last.lowerInclusive(), high));
            }
        }
        logger.debug("Initialized {} range boundaries for column {}",
                boundaries.size(), config.getPartitionColumn());
    }

    // ─── Boundary management ────────────────────────────────────────

    /**
     * Returns an unmodifiable view of the current range boundaries.
     */
    public List<RangeBoundary> getBoundaries() {
        return Collections.unmodifiableList(boundaries);
    }

    /**
     * Replaces all range boundaries with the given list (sorted by index).
     */
    public void setBoundaries(List<RangeBoundary> newBoundaries) {
        boundaries.clear();
        if (newBoundaries != null) {
            boundaries.addAll(newBoundaries);
            boundaries.sort((a, b) -> Integer.compare(a.index(), b.index()));
        }
    }

    /**
     * Appends a new boundary. The index is assigned automatically as the
     * next sequential value.
     */
    public void addBoundary(double lower, double upper) {
        if (upper < lower) {
            throw new IllegalArgumentException(
                    "Invalid boundary: lower=" + lower + " upper=" + upper);
        }
        int nextIndex = boundaries.isEmpty()
                ? 0
                : boundaries.get(boundaries.size() - 1).index() + 1;
        boundaries.add(new RangeBoundary(nextIndex, lower, upper));
    }

    /**
     * Finds the {@link RangeBoundary} containing {@code value}, or
     * {@code null} if the value falls outside every defined boundary.
     */
    public RangeBoundary resolveRangeForValue(Object value) {
        Double numeric = toNumeric(value);
        if (numeric == null) {
            return null;
        }
        for (RangeBoundary b : boundaries) {
            if (numeric >= b.lowerInclusive() && numeric <= b.upperInclusive()) {
                return b;
            }
        }
        return null;
    }

    // ─── Numeric conversion ─────────────────────────────────────────

    /**
     * Converts a value to {@code Double} for range comparison.
     * Supports all {@link Number} types and numeric strings.
     *
     * @return the numeric value, or {@code null} if the value is null,
     *         non-numeric, or unparseable
     */
    private Double toNumeric(Object value) {
        return strategy.toNumeric(value);
    }

    // ─── Partition layout ───────────────────────────────────────────

    /**
     * Resolves the partition directory (e.g. {@code .../range=0-999}) for a value.
     *
     * @param tableName the table name
     * @param value     the partition column value
     * @return the partition directory path, or {@code null} when disabled,
     *         the value is non-numeric, or it falls outside all boundaries
     */
    public Path getPartitionDir(String tableName, Object value) {
        if (!config.isEnabled()) {
            return null;
        }
        RangeBoundary boundary = resolveRangeForValue(value);
        if (boundary == null) {
            return null;
        }
        return getDataDir(tableName).resolve(tableName)
                .resolve(PARTITION_PREFIX + formatBoundary(boundary));
    }

    /**
     * Creates the partition directory for a value if it does not exist.
     *
     * @param tableName the table name
     * @param value     the partition column value
     * @throws IOException on filesystem errors
     */
    public void ensurePartitionExists(String tableName, Object value) throws IOException {
        if (!config.isEnabled() || value == null) {
            return;
        }
        Path partitionDir = getPartitionDir(tableName, value);
        if (partitionDir != null && !Files.exists(partitionDir)) {
            Files.createDirectories(partitionDir);
            logger.info("Created partition directory: {}", partitionDir);
        }
    }

    /**
     * Resolves the partition directory for a value without checking existence.
     *
     * @return the compute-only partition path, or {@code null} when disabled,
     *         non-numeric, or outside all boundaries
     */
    public Path getPartitionForRow(String tableName, Object value) {
        return getPartitionDir(tableName, value);
    }

    /**
     * Checks whether the partition hosting {@code value} exists on disk.
     */
    public boolean partitionExists(String tableName, Object value) {
        Path partitionDir = getPartitionDir(tableName, value);
        return partitionDir != null && Files.exists(partitionDir);
    }

    private String formatBoundary(RangeBoundary b) {
        return formatNumber(b.lowerInclusive()) + "-" + formatNumber(b.upperInclusive());
    }

    private String formatNumber(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v)) {
            return String.valueOf((long) v);
        }
        return String.valueOf(v);
    }

    // ─── Partition listing ──────────────────────────────────────────

    /**
     * Lists all existing {@code range=*} directories for a table.
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
            stream.filter(path -> path.getFileName().toString().startsWith(PARTITION_PREFIX))
                    .forEach(partitions::add);
        }
        return partitions;
    }

    /**
     * Lists all range boundaries that have a corresponding directory on disk,
     * sorted by range index.
     */
    public List<RangeBoundary> listRangeBoundaries(String tableName) throws IOException {
        List<Path> partitions = listPartitions(tableName);
        List<RangeBoundary> result = new ArrayList<>();
        for (Path partition : partitions) {
            RangeBoundary b = parseBoundary(partition.getFileName().toString());
            if (b != null) {
                result.add(b);
            }
        }
        result.sort((a, c) -> Integer.compare(a.index(), c.index()));
        return result;
    }

    private RangeBoundary parseBoundary(String dirName) {
        if (!dirName.startsWith(PARTITION_PREFIX)) {
            return null;
        }
        String rangePart = dirName.substring(PARTITION_PREFIX.length());
        String[] parts = rangePart.split("-");
        if (parts.length != 2) {
            logger.warn("Failed to parse range boundary from directory name: {}", dirName);
            return null;
        }
        try {
            double low = Double.parseDouble(parts[0]);
            double high = Double.parseDouble(parts[1]);
            return new RangeBoundary(-1, low, high);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse range bounds from directory name: {}", dirName);
            return null;
        }
    }

    // ─── Query pruning ──────────────────────────────────────────────

    /**
     * Returns partitions whose ranges overlap {@code [low, high]}.
     * A partition overlaps when its {@code [lowerInclusive, upperInclusive]}
     * interval intersects the query interval.
     *
     * @param tableName the table name
     * @param low       inclusive lower bound of the query range
     * @param high      inclusive upper bound of the query range
     * @return overlapping partition directories, sorted by range index
     * @throws IOException on filesystem errors
     */
    public List<Path> getPartitionsForValueRange(String tableName, Number low, Number high)
            throws IOException {
        if (!config.isEnabled() || low == null || high == null) {
            return Collections.emptyList();
        }
        double lo = low.doubleValue();
        double hi = high.doubleValue();
        if (lo > hi) {
            double tmp = lo;
            lo = hi;
            hi = tmp;
        }
        List<Path> all = listPartitions(tableName);
        List<Path> overlapping = new ArrayList<>();
        for (Path p : all) {
            RangeBoundary b = parseBoundary(p.getFileName().toString());
            if (b != null && rangesOverlap(b.lowerInclusive(), b.upperInclusive(), lo, hi)) {
                overlapping.add(p);
            }
        }
        return overlapping;
    }

    private boolean rangesOverlap(double aLo, double aHi, double bLo, double bHi) {
        return aLo <= bHi && bLo <= aHi;
    }

    /**
     * Prunes (deletes) partitions whose ranges do NOT overlap
     * {@code [low, high]}, i.e. removes everything outside the query range.
     *
     * @param tableName the table name
     * @param low       inclusive lower bound of the query range
     * @param high      inclusive upper bound of the query range
     * @throws IOException on filesystem errors
     */
    public void prunePartitionsOutsideRange(String tableName, Number low, Number high)
            throws IOException {
        if (!config.isEnabled() || low == null || high == null) {
            return;
        }
        double lo = low.doubleValue();
        double hi = high.doubleValue();
        if (lo > hi) {
            double tmp = lo;
            lo = hi;
            hi = tmp;
        }
        List<Path> all = listPartitions(tableName);
        for (Path partition : all) {
            RangeBoundary b = parseBoundary(partition.getFileName().toString());
            if (b != null && !rangesOverlap(b.lowerInclusive(), b.upperInclusive(), lo, hi)) {
                if (deleteTree(partition)) {
                    logger.info("Pruned partition: {}", partition);
                }
            }
        }
    }

    // ─── Dynamic range adjustment ───────────────────────────────────

    /**
     * Splits the range at {@code rangeIndex} into {@code splitCount}
     * equal sub-ranges, redistributes the affected rows, and removes the
     * original range directory.
     *
     * @param tableName  the table name
     * @param rangeIndex the index of the range to split (as in the boundaries list)
     * @param splitCount number of sub-ranges to create ({@code >= 2})
     * @return a {@link RangeSplitReport} describing the outcome
     */
    public RangeSplitReport splitRange(String tableName, int rangeIndex, int splitCount) {
        if (!config.isEnabled()) {
            return new RangeSplitReport(boundaries.size(), boundaries.size(),
                    0L, 0, 0, true, Collections.emptyList());
        }
        if (splitCount < 2) {
            return new RangeSplitReport(boundaries.size(), boundaries.size(),
                    0L, 0, 0, false,
                    List.of("splitCount must be >= 2, got " + splitCount));
        }
        RangeBoundary target = findBoundaryByIndex(rangeIndex);
        if (target == null) {
            return new RangeSplitReport(boundaries.size(), boundaries.size(),
                    0L, 0, 0, false,
                    List.of("Range index " + rangeIndex + " not found"));
        }

        List<String> errors = new ArrayList<>();
        int oldCount = boundaries.size();

        try {
            // Build sub-range boundaries using strategy
            List<RangeBoundary> subRanges = strategy.computeSubRangeBoundaries(target, splitCount);

            // Read rows from the original range directory
            Path oldDir = getDataDir(tableName).resolve(tableName)
                    .resolve(PARTITION_PREFIX + formatBoundary(target));
            List<Map<String, Object>> rows = new ArrayList<>();
            if (Files.exists(oldDir)) {
                rows.addAll(strategy.readPartitionRows(tableName, oldDir));
            }

            // Write rows into sub-range directories using strategy
            Set<Path> written = new HashSet<>();
            RangePartitionStrategy.RedistributionResult redistributionResult = 
                strategy.redistributeRows(tableName, rows, subRanges, written);

            // Ensure empty sub-range directories are created
            for (RangeBoundary sub : subRanges) {
                Path subDir = getDataDir(tableName).resolve(tableName)
                        .resolve(PARTITION_PREFIX + formatBoundary(sub));
                if (!written.contains(subDir) && !Files.exists(subDir)) {
                    Files.createDirectories(subDir);
                    written.add(subDir);
                    redistributionResult = new RangePartitionStrategy.RedistributionResult(
                            redistributionResult.rangesCreated() + 1, redistributionResult.errors());
                }
            }

            // Remove the original range directory
            int rangesRemoved = 0;
            if (Files.exists(oldDir) && !written.contains(oldDir)) {
                if (deleteTree(oldDir)) {
                    rangesRemoved = 1;
                }
            }

            // Replace boundary: remove old, add sub-ranges
            int finalIndex = target.index();
            boundaries.removeIf(b -> b.index() == rangeIndex);
            for (int i = 0; i < subRanges.size(); i++) {
                RangeBoundary sub = subRanges.get(i);
                boundaries.add(new RangeBoundary(finalIndex + i,
                        sub.lowerInclusive(), sub.upperInclusive()));
            }
            strategy.reindexBoundaries(boundaries);

            logger.info("Split range {} of table {} into {} sub-ranges ({} rows)",
                    rangeIndex, tableName, splitCount, rows.size());
            return new RangeSplitReport(oldCount, boundaries.size(),
                    rows.size(), redistributionResult.rangesCreated(), rangesRemoved,
                    redistributionResult.errors().isEmpty(), Collections.unmodifiableList(redistributionResult.errors()));
        } catch (Exception e) {
            errors.add("splitRange aborted: " + e.getMessage());
            logger.warn("splitRange aborted: {}", e.getMessage());
            return new RangeSplitReport(oldCount, boundaries.size(),
                    0L, 0, 0, false, Collections.unmodifiableList(errors));
        }
    }

    /**
     * Merges two adjacent ranges identified by their boundary indexes.
     * The combined range covers {@code [min(lower1, lower2), max(upper1, upper2)]}.
     *
     * @param tableName the table name
     * @param indexA    first range index
     * @param indexB    second range index
     * @return a {@link RangeSplitReport} describing the outcome
     */
    public RangeSplitReport mergeRanges(String tableName, int indexA, int indexB) {
        if (!config.isEnabled()) {
            return new RangeSplitReport(boundaries.size(), boundaries.size(),
                    0L, 0, 0, true, Collections.emptyList());
        }
        RangeBoundary a = findBoundaryByIndex(indexA);
        RangeBoundary b = findBoundaryByIndex(indexB);
        if (a == null || b == null) {
            List<String> errs = new ArrayList<>();
            if (a == null) errs.add("Range index " + indexA + " not found");
            if (b == null) errs.add("Range index " + indexB + " not found");
            return new RangeSplitReport(boundaries.size(), boundaries.size(),
                    0L, 0, 0, false, Collections.unmodifiableList(errs));
        }

        List<String> errors = new ArrayList<>();
        int oldCount = boundaries.size();

        try {
            double mergedLow = Math.min(a.lowerInclusive(), b.lowerInclusive());
            double mergedHigh = Math.max(a.upperInclusive(), b.upperInclusive());
            RangeBoundary merged = new RangeBoundary(-1, mergedLow, mergedHigh);

            // Read rows from both range directories using strategy
            List<Map<String, Object>> rows = new ArrayList<>();
            Path dirA = getDataDir(tableName).resolve(tableName)
                    .resolve(PARTITION_PREFIX + formatBoundary(a));
            Path dirB = getDataDir(tableName).resolve(tableName)
                    .resolve(PARTITION_PREFIX + formatBoundary(b));
            if (Files.exists(dirA)) {
                rows.addAll(strategy.readPartitionRows(tableName, dirA));
            }
            if (Files.exists(dirB)) {
                rows.addAll(strategy.readPartitionRows(tableName, dirB));
            }

            // Write to the merged directory using strategy
            Path mergedDir = getDataDir(tableName).resolve(tableName)
                    .resolve(PARTITION_PREFIX + formatBoundary(merged));
            int rangesCreated = 0;
            if (!rows.isEmpty()) {
                try {
                    strategy.writePartitionRows(tableName, mergedDir, rows);
                    rangesCreated = 1;
                } catch (Exception e) {
                    errors.add("Failed to write merged partition: " + e.getMessage());
                    logger.warn("mergeRanges failed to write {}: {}",
                            mergedDir, e.getMessage());
                }
            }

            // Remove old range directories
            int rangesRemoved = 0;
            for (Path old : List.of(dirA, dirB)) {
                if (Files.exists(old) && !old.equals(mergedDir)) {
                    if (deleteTree(old)) {
                        rangesRemoved++;
                    }
                }
            }

            // Replace boundaries
            boundaries.removeIf(bb -> bb.index() == indexA || bb.index() == indexB);
            boundaries.add(merged);
            strategy.reindexBoundaries(boundaries);

            logger.info("Merged ranges {}+{} of table {} ({} rows)",
                    indexA, indexB, tableName, rows.size());
            return new RangeSplitReport(oldCount, boundaries.size(),
                    rows.size(), rangesCreated, rangesRemoved,
                    errors.isEmpty(), Collections.unmodifiableList(errors));
        } catch (Exception e) {
            errors.add("mergeRanges aborted: " + e.getMessage());
            logger.warn("mergeRanges aborted: {}", e.getMessage());
            return new RangeSplitReport(oldCount, boundaries.size(),
                    0L, 0, 0, false, Collections.unmodifiableList(errors));
        }
    }

    // ─── Range introspection ────────────────────────────────────────

    /**
     * Returns info for every range partition on disk including row counts.
     *
     * @param tableName the table name
     * @return list of {@link PartitionRangeInfo}, sorted by range index
     * @throws IOException on filesystem errors
     */
    public List<PartitionRangeInfo> describePartitions(String tableName) throws IOException {
        List<Path> partitions = listPartitions(tableName);
        List<PartitionRangeInfo> result = new ArrayList<>();
        for (Path p : partitions) {
            RangeBoundary b = parseBoundary(p.getFileName().toString());
            if (b == null) {
                continue;
            }
            long count = 0;
            try {
                AvroRowStorage partStorage = newPartitionStorage(p.toString());
                partStorage.loadFromFile(tableName);
                count = partStorage.scan().size();
            } catch (Exception e) {
                logger.warn("Failed to count rows in {}: {}", p, e.getMessage());
            }
            result.add(new PartitionRangeInfo(
                    b.index(), b.lowerInclusive(), b.upperInclusive(), p, count));
        }
        result.sort((x, y) -> Double.compare(x.lowerBound(), y.lowerBound()));
        for (int i = 0; i < result.size(); i++) {
            PartitionRangeInfo info = result.get(i);
            result.set(i, new PartitionRangeInfo(i,
                    info.lowerBound(), info.upperBound(), info.partitionPath(),
                    info.rowCount()));
        }
        return result;
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private RangeBoundary findBoundaryByIndex(int index) {
        for (RangeBoundary b : boundaries) {
            if (b.index() == index) {
                return b;
            }
        }
        return null;
    }

    private void reindexBoundaries() {
        strategy.reindexBoundaries(boundaries);
    }

    private Object extractPartitionValue(Map<String, Object> row) {
        if (config.getPartitionColumn().isEmpty()) {
            return null;
        }
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getKey().equalsIgnoreCase(config.getPartitionColumn())) {
                return e.getValue();
            }
        }
        return null;
    }

    private List<Map<String, Object>> readPartitionRows(String tableName, Path partitionDir)
            throws IOException {
        AvroRowStorage partitionStorage = newPartitionStorage(partitionDir.toString());
        partitionStorage.loadFromFile(tableName);
        return partitionStorage.scan();
    }

    private void writePartitionRows(String tableName, Path partitionDir,
                                    List<Map<String, Object>> rows) throws IOException {
        if (!Files.exists(partitionDir)) {
            Files.createDirectories(partitionDir);
        }
        AvroRowStorage partitionStorage = newPartitionStorage(partitionDir.toString());
        partitionStorage.setRows(rows);
        partitionStorage.saveToFile(tableName);
    }

    private AvroRowStorage newPartitionStorage(String partitionDir) {
        AvroRowStorage partitionStorage = new AvroRowStorage(
                storage.getTableName(), storage.getColumns(), storage.getColumnTypes());
        partitionStorage.setDataDir(partitionDir);
        return partitionStorage;
    }

    private boolean deleteTree(Path dir) {
        try {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                logger.warn("Failed to delete {}: {}", p, e.getMessage());
                            }
                        });
            }
            return !Files.exists(dir);
        } catch (IOException e) {
            logger.warn("Failed to delete stale partition {}: {}", dir, e.getMessage());
            return false;
        }
    }

    // ─── Data directory resolution ──────────────────────────────────

    /**
     * Resolves the base data directory for the storage.
     */
    private Path getDataDir(String tableName) {
        try {
            java.lang.reflect.Method method = AbstractRowStorage.class
                    .getDeclaredMethod("resolveFilePath", String.class);
            method.setAccessible(true);
            String filePath = (String) method.invoke(storage, ".avro");
            return Paths.get(filePath).getParent().getParent();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get dataDir from storage", e);
        }
    }
}
