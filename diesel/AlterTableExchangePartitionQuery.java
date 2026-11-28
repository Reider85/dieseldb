package diesel;

import java.util.Objects;

/**
 * Query DTO for {@code ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...}
 * (prompt4 #65, R3-017 step 3). The actual work is performed by
 * {@link diesel.partition.PartitionExchange} through
 * {@link Database#exchangePartition(String, String, String)}.
 */
public final class AlterTableExchangePartitionQuery implements Query<String> {
    private final String tableName;
    private final String partitionName;
    private final String otherTableName;

    public AlterTableExchangePartitionQuery(String tableName, String partitionName, String otherTableName) {
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        this.partitionName = Objects.requireNonNull(partitionName, "Partition name cannot be null");
        this.otherTableName = Objects.requireNonNull(otherTableName, "Other table name cannot be null");
        if (tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        if (partitionName.trim().isEmpty()) {
            throw new IllegalArgumentException("Partition name cannot be empty");
        }
        if (otherTableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Other table name cannot be empty");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getOtherTableName() {
        return otherTableName;
    }

    @Override
    public String execute(Table table) {
        return "ALTER TABLE " + tableName + " EXCHANGE PARTITION " + partitionName
                + " WITH TABLE " + otherTableName;
    }

    @Override
    public String toString() {
        return execute(null);
    }
}
