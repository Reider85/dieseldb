package diesel.storage.avro;

import diesel.storage.AbstractRowStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * AVRO hash partitioning for DieselDB: distributes rows across a configurable
 * number of partitions by hashing the value of a chosen column, ensuring a
 * uniform data distribution. Partitioning follows a Hive-style directory
 * structure: {@code data/avro/table_name/part=N/}.
 *
 * <p>Features (prompt 89):
 * <ol>
 *   <li>Hash partitioning for even distribution — {@link #computeHash(Object)}</li>
 *   <li>Configurable hashing column — {@link HashPartitionConfig#getPartitionColumn()}</li>
 *   <li>Configurable partition count — {@link HashPartitionConfig#getNumPartitions()}</li>
 *   <li>Rebalancing when the partition count changes — {@link #rebalance(String, int)}</li>
 * </ol>
 *
 * @since Prompt 89
 */
public class AvroHashPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(AvroHashPartitioner.class);

    /** Directory name prefix for hash partitions, e.g. {@code part=3}. */
    private static final String PARTITION_PREFIX = "part=";

    /**
     * Supported hash functions. All produce a non-negative 32-bit digest.
     */
    public enum HashFunction {
        /** Fast default: {@link String#hashCode()} style polynomial hash. */
        SIMPLE,
        /** Murmur3 32-bit, better avalanche for skewed string distributions. */
        MURMUR3,
        /** MD5 digest, first 4 bytes folded into an int. */
        MD5,
        /** SHA-256 digest, first 4 bytes folded into an int. */
        SHA256
    }

    /**
     * Immutable configuration for hash partitioning.
     *
     * @param enabled         whether partitioning is active
     * @param partitionColumn column whose value drives the hash
     * @param numPartitions   number of partitions (clamped to {@code >= 1})
     * @param hashFunction    hash algorithm to use
     * @param seed            optional seed mixed into the hash
     */
    public static class HashPartitionConfig {
        private final boolean enabled;
        private final String partitionColumn;
        private final int numPartitions;
        private final HashFunction hashFunction;
        private final int seed;

        public HashPartitionConfig(boolean enabled, String partitionColumn,
                                   int numPartitions, HashFunction hashFunction, int seed) {
            this.enabled = enabled;
            this.partitionColumn = partitionColumn != null ? partitionColumn : "";
            this.numPartitions = Math.max(1, numPartitions);
            this.hashFunction = hashFunction != null ? hashFunction : HashFunction.SIMPLE;
            this.seed = seed;
        }

        public boolean isEnabled() { return enabled; }
        public String getPartitionColumn() { return partitionColumn; }
        public int getNumPartitions() { return numPartitions; }
        public HashFunction getHashFunction() { return hashFunction; }
        public int getSeed() { return seed; }
    }

    /**
     * Report of a {@link #rebalance(String, int)} operation.
     *
     * @param oldCount           partition count before rebalancing
     * @param newCount           partition count after rebalancing
     * @param rowsMoved          number of rows redistributed
     * @param partitionsCreated  partition directories written
     * @param partitionsRemoved  stale partition directories removed
     * @param success            whether the rebalance completed without errors
     * @param errors             per-partition error messages exposed to the caller
     */
    public record RebalanceReport(int oldCount, int newCount, long rowsMoved,
                                  int partitionsCreated, int partitionsRemoved,
                                  boolean success, List<String> errors) {
    }

    private final AbstractRowStorage storage;
    private final HashPartitionConfig config;

    public AvroHashPartitioner(AbstractRowStorage storage, HashPartitionConfig config) {
        this.storage = storage;
        this.config = config;
    }

    // ─── Hashing ────────────────────────────────────────────────────

    /**
     * Computes a non-negative 32-bit hash of the given value using the
     * configured {@link HashFunction}. {@code null} values hash to
     * deterministic partition 0.
     *
     * @param value the value to hash
     * @return a non-negative hash digest
     */
    public int computeHash(Object value) {
        if (value == null) {
            return 0;
        }
        String text = value.toString();
        switch (config.getHashFunction()) {
            case MURMUR3:
                return toPositive(murmur3(text) ^ config.getSeed());
            case MD5:
                return toPositive(foldDigest("MD5", text) ^ config.getSeed());
            case SHA256:
                return toPositive(foldDigest("SHA-256", text) ^ config.getSeed());
            case SIMPLE:
            default:
                return toPositive(text.hashCode() ^ config.getSeed());
        }
    }

    /**
     * Assigns a value to one of {@code numPartitions} buckets using the
     * configured partition count.
     *
     * @param value the value to place
     * @return a partition index in {@code [0, numPartitions)}
     */
    public int getPartitionIndex(Object value) {
        return computeHash(value) % config.getNumPartitions();
    }

    /**
     * Assigns a value to one of {@code count} buckets, ignoring the configured
     * partition count. Used when rebalancing across a new layout.
     *
     * @return a partition index in {@code [0, count)}
     */
    public int getPartitionIndexForCount(Object value, int count) {
        return computeHash(value) % Math.max(1, count);
    }

    private static int toPositive(int h) {
        return (h & Integer.MAX_VALUE) == Integer.MIN_VALUE
                ? 0 : h & Integer.MAX_VALUE;
    }

    private static int murmur3(String text) {
        byte[] data = text.getBytes(StandardCharsets.UTF_8);
        int h1 = 0;
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int len = data.length;
        int remaining = len;

        for (int i = 0; i < len / 4; i++) {
            int k1 = (data[i * 4] & 0xFF)
                    | ((data[i * 4 + 1] & 0xFF) << 8)
                    | ((data[i * 4 + 2] & 0xFF) << 16)
                    | ((data[i * 4 + 3] & 0xFF) << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
            remaining -= 4;
        }

        int tail = 0;
        int off = len / 4 * 4;
        for (int k = 0; k < remaining; k++) {
            tail ^= (data[off + k] & 0xFF) << (8 * k);
        }
        if (remaining > 0) {
            tail *= c1;
            tail = Integer.rotateLeft(tail, 15);
            tail *= c2;
            h1 ^= tail;
        }

        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }

    private static int foldDigest(String algorithm, String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] bytes = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            return (bytes[0] & 0xFF)
                    | ((bytes[1] & 0xFF) << 8)
                    | ((bytes[2] & 0xFF) << 16)
                    | ((bytes[3] & 0xFF) << 24);
        } catch (java.security.NoSuchAlgorithmException e) {
            logger.warn("Hash algorithm {} unavailable, falling back to text hash", algorithm);
            return text.hashCode();
        }
    }

    // ─── Partition layout ───────────────────────────────────────────

    /**
     * Resolves the partition directory (e.g. {@code .../part=3}) for a value.
     *
     * @param tableName the table name
     * @param value     the partition column value
     * @return the partition directory path, or {@code null} when disabled or the value is null
     */
    public Path getPartitionDir(String tableName, Object value) {
        if (!config.isEnabled() || value == null) {
            return null;
        }
        int index = getPartitionIndex(value);
        return getDataDir(tableName).resolve(tableName)
                .resolve(PARTITION_PREFIX + index);
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
     * Resolves the partition directory (e.g. {@code .../part=3}) for a value,
     * without checking that it exists on disk.
     *
     * @return the compute-only partition path, or {@code null} when disabled/value null
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

    // ─── Partition listing ──────────────────────────────────────────

    /**
     * Lists all existing {@code part=*} directories for a table.
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
     * Lists the integer indexes of all existing {@code part=*} directories,
     * sorted ascending.
     */
    public List<Integer> listPartitionIndexes(String tableName) throws IOException {
        List<Path> partitions = listPartitions(tableName);
        List<Integer> indexes = new ArrayList<>();
        for (Path partition : partitions) {
            Integer index = parsePartitionIndex(partition.getFileName().toString());
            if (index != null) {
                indexes.add(index);
            }
        }
        Collections.sort(indexes);
        return indexes;
    }

    private Integer parsePartitionIndex(String dirName) {
        if (!dirName.startsWith(PARTITION_PREFIX)) {
            return null;
        }
        try {
            return Integer.valueOf(dirName.substring(PARTITION_PREFIX.length()));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse partition index from directory name: {}", dirName);
            return null;
        }
    }

    // ─── Rebalancing ────────────────────────────────────────────────

    /**
     * Rebalances the data of a table across a new partition count. Reads every
     * existing {@code part=*} partition of the table, re-hashes each row with
     * the new count, writes the rows into the new layout and removes the stale
     * old partition directories (whose data has been relocated).
     *
     * <p>When {@code newPartitionCount} equals the current count, the operation
     * is a no-op: a success report with zero rows moved and nothing touched.
     *
     * <p>Partition data lives in a per-partition {@code <table>.avro} file
     * written by an {@link AvroRowStorage} pointed at that partition directory.
     *
     * @param tableName         the table name
     * @param newPartitionCount the number of partitions after rebalancing (clamped to {@code >= 1})
     * @return a {@link RebalanceReport} describing the outcome
     */
    public RebalanceReport rebalance(String tableName, int newPartitionCount) {
        if (!config.isEnabled()) {
            return new RebalanceReport(config.getNumPartitions(), config.getNumPartitions(),
                    0L, 0, 0, true, Collections.emptyList());
        }
        int targetCount = Math.max(1, newPartitionCount);
        List<String> errors = new ArrayList<>();

        try {
            List<Path> oldPartitions = listPartitions(tableName);
            if (targetCount == config.getNumPartitions()) {
                return new RebalanceReport(config.getNumPartitions(), targetCount,
                        0L, 0, 0, true, Collections.emptyList());
            }

            List<Map<String, Object>> allRows = new ArrayList<>();
            for (Path partition : oldPartitions) {
                try {
                    allRows.addAll(readPartitionRows(tableName, partition));
                } catch (Exception e) {
                    errors.add("Failed to read partition " + partition + ": " + e.getMessage());
                    logger.warn("Rebalance failed to read partition {}: {}", partition, e.getMessage());
                }
            }

            int partitionsCreated = 0;
            Set<Path> writtenPartitions = new HashSet<>();
            if (!allRows.isEmpty()) {
                Map<Integer, List<Map<String, Object>>> buckets = bucketize(allRows, targetCount);
                for (Map.Entry<Integer, List<Map<String, Object>>> entry : buckets.entrySet()) {
                    int index = entry.getKey();
                    Path partitionDir = getDataDir(tableName).resolve(tableName)
                            .resolve(PARTITION_PREFIX + index);
                    try {
                        writePartitionRows(tableName, partitionDir, entry.getValue());
                        partitionsCreated++;
                        writtenPartitions.add(partitionDir);
                    } catch (Exception e) {
                        errors.add("Failed to write partition " + partitionDir + ": " + e.getMessage());
                        logger.warn("Rebalance failed to write partition {}: {}", partitionDir, e.getMessage());
                    }
                }
            }

            int partitionsRemoved = 0;
            for (Path partition : oldPartitions) {
                // Never delete a partition path that was just rewritten (when the
                // old and new layouts share low indexes of the part= range).
                if (writtenPartitions.contains(partition)) {
                    continue;
                }
                if (deleteTree(partition)) {
                    partitionsRemoved++;
                }
            }

            logger.info("Rebalanced table {} from {} to {} partitions ({} rows moved)",
                    tableName, config.getNumPartitions(), targetCount, allRows.size());
            return new RebalanceReport(config.getNumPartitions(), targetCount,
                    allRows.size(), partitionsCreated, partitionsRemoved,
                    errors.isEmpty(), Collections.unmodifiableList(errors));
        } catch (Exception e) {
            errors.add("Rebalance aborted: " + e.getMessage());
            logger.warn("Rebalance aborted: {}", e.getMessage());
            return new RebalanceReport(config.getNumPartitions(), targetCount,
                    0L, 0, 0, false, Collections.unmodifiableList(errors));
        }
    }

    /**
     * Groups rows into buckets by re-hashing the partition column value with
     * the target partition count.
     */
    private Map<Integer, List<Map<String, Object>>> bucketize(List<Map<String, Object>> rows, int count) {
        Map<Integer, List<Map<String, Object>>> buckets = new TreeMap<>();
        for (Map<String, Object> row : rows) {
            Object value = extractPartitionValue(row);
            int index = getPartitionIndexForCount(value, count);
            buckets.computeIfAbsent(index, k -> new ArrayList<>()).add(row);
        }
        return buckets;
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

    /**
     * Reads all rows stored in a partition directory's {@code <table>.avro} file.
     */
    private List<Map<String, Object>> readPartitionRows(String tableName, Path partitionDir)
            throws IOException {
        AvroRowStorage partitionStorage = newPartitionStorage(partitionDir.toString());
        partitionStorage.loadFromFile(tableName);
        return partitionStorage.scan();
    }

    /**
     * Writes rows to a partition directory as a fresh {@code <table>.avro} file.
     */
    private void writePartitionRows(String tableName, Path partitionDir, List<Map<String, Object>> rows)
            throws IOException {
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

    /**
     * Deletes a directory tree (stale partition whose data has been relocated).
     */
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