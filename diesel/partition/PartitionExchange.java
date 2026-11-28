package diesel.partition;

import diesel.Database;
import diesel.Table;
import diesel.TableNotFoundException;
import diesel.partition.PartitionManager.Partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implements {@code ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...}
 * (prompt4 #65, R3-017 step 3).
 *
 * <p>DieselDB stores partition rows in the parent table, so the exchange is a
 * container swap performed under both tables' write locks: rows belonging to
 * the named partition move into the external table and the external table's
 * rows move into the parent. No table is recreated, so the operation is
 * effectively instantaneous and atomic from a reader's point of view.
 */
public final class PartitionExchange {
    private PartitionExchange() {
    }

    /**
     * Result of a successful partition exchange.
     *
     * @param parentTable      the partitioned table name
     * @param partitionName    the exchanged partition name
     * @param otherTable       the external table name
     * @param rowsToOther      rows moved from the parent into the external table
     * @param rowsToParent     rows moved from the external table into the parent
     * @param durationNanos    wall time spent performing the swap
     */
    public record ExchangeResult(String parentTable, String partitionName, String otherTable,
                                 int rowsToOther, int rowsToParent, long durationNanos) {
    }

    /**
     * Exchanges the rows of one partition with all rows of another table.
     *
     * @param database      the database holding both tables
     * @param parentName    the partitioned table
     * @param partitionName the partition to exchange
     * @param otherName     the external table
     * @return details of the swap
     * @throws IllegalArgumentException if either table is missing, the parent is
     *                                  not partitioned, the partition does not
     *                                  exist, both names denote the same table,
     *                                  or the schemas are incompatible
     */
    public static ExchangeResult exchange(Database database, String parentName,
                                          String partitionName, String otherName) {
        Objects.requireNonNull(database, "database");
        long started = System.nanoTime();

        Table parent = requireTable(database, parentName, "Parent table");
        Table other = requireTable(database, otherName, "Other table");
        if (parent.getName().equals(other.getName())) {
            throw new IllegalArgumentException("Cannot exchange a partition with the same table: " + parentName);
        }
        PartitionManager manager = parent.getPartitionManager();
        if (manager == null || !parent.isPartitioned()) {
            throw new IllegalArgumentException("Table " + parentName + " is not partitioned");
        }
        Partition partition = manager.findPartitionByName(partitionName);
        if (partition == null) {
            throw new IllegalArgumentException("Partition " + partitionName
                    + " does not exist in table " + parentName);
        }
        validateCompatibleSchema(parent, other);

        List<Table> locked = new ArrayList<>();
        locked.add(parent);
        locked.add(other);
        locked.sort(Comparator.comparing(Table::getName));
        for (Table table : locked) {
            table.getTableLock().writeLock().lock();
        }
        int toOther;
        int toParent;
        try {
            List<Map<String, Object>> parentRows = parent.getRows();
            List<Map<String, Object>> otherRows = other.getRows();
            String column = manager.getPartitionColumn();

            List<Map<String, Object>> keepInParent = new ArrayList<>(parentRows.size());
            List<Map<String, Object>> movedToOther = new ArrayList<>();
            for (Map<String, Object> row : parentRows) {
                Object value = row == null ? null : row.get(column);
                if (belongsToPartition(manager, partition, value)) {
                    movedToOther.add(row);
                } else {
                    keepInParent.add(row);
                }
            }
            keepInParent.addAll(otherRows);

            parent.replaceRowsAtomically(keepInParent);
            other.replaceRowsAtomically(movedToOther);
            toOther = movedToOther.size();
            toParent = otherRows.size();
        } finally {
            for (int i = locked.size() - 1; i >= 0; i--) {
                locked.get(i).getTableLock().writeLock().unlock();
            }
        }

        database.invalidateQueryCache();
        return new ExchangeResult(parentName, partitionName, otherName,
                toOther, toParent, System.nanoTime() - started);
    }

    private static Table requireTable(Database database, String name, String label) {
        try {
            return database.getTable(name);
        } catch (TableNotFoundException e) {
            throw new IllegalArgumentException(label + " " + name + " does not exist", e);
        }
    }

    private static void validateCompatibleSchema(Table parent, Table other) {
        List<String> columns = parent.getColumns();
        if (!columns.equals(other.getColumns())) {
            throw new IllegalArgumentException("Table " + other.getName()
                    + " has incompatible columns for EXCHANGE PARTITION with " + parent.getName());
        }
        for (String column : columns) {
            if (parent.getColumnTypes().get(column) != other.getColumnTypes().get(column)) {
                throw new IllegalArgumentException("Column " + column + " has incompatible type in table "
                        + other.getName());
            }
        }
    }

    private static boolean belongsToPartition(PartitionManager manager, Partition partition, Object value) {
        if (partition == manager.getDefaultPartition()) {
            for (Partition defined : manager.getPartitions()) {
                if (defined != partition && matchesDefined(defined, value)) {
                    return false;
                }
            }
            return true;
        }
        return matchesDefined(partition, value);
    }

    private static boolean matchesDefined(Partition partition, Object value) {
        if (value == null) {
            return false;
        }
        if (partition instanceof RangePartition range) {
            if (range.getMinValue() != null && compare(value, range.getMinValue()) < 0) {
                return false;
            }
            if (range.getMaxValue() != null && compare(value, range.getMaxValue()) >= 0) {
                return false;
            }
            return true;
        }
        return partition.containsValue(value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object value, Object bound) {
        return ((Comparable) value).compareTo(bound);
    }
}
