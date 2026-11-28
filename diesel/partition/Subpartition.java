package diesel.partition;

import diesel.partition.PartitionManager.Partition;
import diesel.partition.PartitionManager.PartitionType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A level-2 partitioning scheme attached to one level-1 partition
 * (prompt4 #65, R3-017 step 3). Supported combinations are RANGE then LIST
 * and RANGE then HASH.
 */
public final class Subpartition {
    private final String name;
    private final String parentPartitionName;
    private final PartitionType type;
    private final String column;
    private final List<Partition> partitions;
    private final Partition defaultPartition;

    public Subpartition(String name, String parentPartitionName, PartitionType type,
                        String column, List<Partition> partitions, Partition defaultPartition) {
        this.name = Objects.requireNonNull(name, "name");
        this.parentPartitionName = Objects.requireNonNull(parentPartitionName, "parentPartitionName");
        this.type = Objects.requireNonNull(type, "type");
        if (type != PartitionType.LIST && type != PartitionType.HASH) {
            throw new IllegalArgumentException("Subpartition type must be LIST or HASH: " + type);
        }
        this.column = Objects.requireNonNull(column, "column");
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        this.defaultPartition = defaultPartition;
    }

    public String getName() {
        return name;
    }

    public String getParentPartitionName() {
        return parentPartitionName;
    }

    public PartitionType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Partition getDefaultPartition() {
        return defaultPartition;
    }

    /**
     * Resolves the level-2 partition for a row.
     *
     * @param rowValues the row to route
     * @return the matching partition, or null when no partition matches
     */
    public Partition findSubpartition(Map<String, Object> rowValues) {
        Object value = rowValues == null ? null : rowValues.get(column);
        if (value == null) {
            return defaultPartition;
        }
        for (Partition partition : partitions) {
            if (partition.containsValue(value)) {
                return partition;
            }
        }
        return defaultPartition;
    }

    /**
     * Prunes this scheme's subpartitions using the same suffixed filter keys as
     * {@link PartitionPruner} ({@code column}, {@code column_eq},
     * {@code column_in}). HASH subpartitions cannot be pruned and are always
     * all scanned.
     *
     * @param filterValues the WHERE-clause filter values
     * @return the pruning result for this level
     */
    public PruningResult prune(Map<String, Object> filterValues) {
        List<Partition> all = allPartitions();
        if (type == PartitionType.HASH || filterValues == null || filterValues.isEmpty()) {
            return new PruningResult(new ArrayList<>(all), 0, all.size(),
                    type == PartitionType.HASH ? "HASH subpartitioning cannot be pruned" : "No subpartition filter",
                    0.0);
        }

        Object eq = filterValues.containsKey(column + "_eq")
                ? filterValues.get(column + "_eq") : filterValues.get(column);
        Object in = filterValues.get(column + "_in");

        if (eq == null && !(in instanceof Collection<?>)) {
            return new PruningResult(new ArrayList<>(all), 0, all.size(),
                    "No filter on subpartition column " + column, 0.0);
        }

        List<Partition> toScan = new ArrayList<>();
        int skipped = 0;
        for (Partition partition : partitions) {
            boolean skip;
            if (partition instanceof ListPartition list) {
                skip = in instanceof Collection<?> collection
                        ? list.canSkipByIn(collection) : list.canSkipByFilter(eq);
            } else {
                skip = false;
            }
            if (skip) {
                skipped++;
            } else {
                toScan.add(partition);
            }
        }
        if (defaultPartition != null) {
            toScan.add(defaultPartition);
        }
        int total = all.size();
        double ratio = total == 0 ? 0.0 : (double) skipped / total;
        return new PruningResult(toScan, skipped, total,
                "LIST subpartition pruning on " + column, ratio);
    }

    private List<Partition> allPartitions() {
        List<Partition> all = new ArrayList<>(partitions);
        if (defaultPartition != null && !all.contains(defaultPartition)) {
            all.add(defaultPartition);
        }
        return all;
    }

    /**
     * Result of pruning a single subpartitioning level.
     */
    public static final class PruningResult {
        private final List<Partition> partitionsToScan;
        private final int partitionsSkipped;
        private final int totalPartitions;
        private final String reason;
        private final double pruningRatio;

        public PruningResult(List<Partition> partitionsToScan, int partitionsSkipped,
                             int totalPartitions, String reason, double pruningRatio) {
            this.partitionsToScan = Collections.unmodifiableList(new ArrayList<>(partitionsToScan));
            this.partitionsSkipped = partitionsSkipped;
            this.totalPartitions = totalPartitions;
            this.reason = reason;
            this.pruningRatio = pruningRatio;
        }

        public List<Partition> getPartitionsToScan() {
            return partitionsToScan;
        }

        public int getPartitionsSkipped() {
            return partitionsSkipped;
        }

        public int getTotalPartitions() {
            return totalPartitions;
        }

        public String getReason() {
            return reason;
        }

        public double getPruningRatio() {
            return pruningRatio;
        }
    }
}
