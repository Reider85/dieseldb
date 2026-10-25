package diesel;

import diesel.schema.AlterAlgorithm;
import diesel.schema.AlterLockMode;
import java.util.Map;
import java.util.Objects;

/**
 * Query DTO for {@code ALTER TABLE ... DROP COLUMN} (prompt4 #39, R3-010 step 3).
 *
 * <p>Supports ALGORITHM=COPY (background rewrite with atomic swap) and
 * ALGORITHM=INPLACE (tombstone + background physical removal via compaction).
 * Default algorithm for DROP is INPLACE (online path).
 */
public final class AlterTableDropColumnQuery implements Query<Void> {
    private final String tableName;
    private final String columnName;
    private final AlterAlgorithm algorithm;
    private final AlterLockMode lockMode;

    /**
     * Creates a DROP COLUMN query with default INPLACE algorithm and NONE lock.
     */
    public AlterTableDropColumnQuery(String tableName, String columnName) {
        this(tableName, columnName, AlterAlgorithm.INPLACE, AlterLockMode.NONE);
    }

    /**
     * Creates a DROP COLUMN query with explicit algorithm and lock mode.
     */
    public AlterTableDropColumnQuery(String tableName, String columnName,
                                    AlterAlgorithm algorithm, AlterLockMode lockMode) {
        this.tableName = Objects.requireNonNull(tableName, "tableName cannot be null");
        this.columnName = Objects.requireNonNull(columnName, "columnName cannot be null");
        this.algorithm = Objects.requireNonNull(algorithm, "algorithm cannot be null");
        this.lockMode = Objects.requireNonNull(lockMode, "lockMode cannot be null");

        // Validate column name is not empty
        if (tableName.trim().isEmpty()) {
            throw new SyntaxErrorException("Table name cannot be empty");
        }
        if (columnName.trim().isEmpty()) {
            throw new SyntaxErrorException("Column name cannot be empty");
        }
    }

    /**
     * Returns the table name being altered.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the column being dropped.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the algorithm for the operation (COPY or INPLACE).
     */
    public AlterAlgorithm getAlgorithm() {
        return algorithm;
    }

    /**
     * Returns the lock mode for the operation.
     */
    public AlterLockMode getLockMode() {
        return lockMode;
    }

    @Override
    public Void execute(Table table) {
        // Real execution is in Database.executeAlterTableDropColumn
        throw new UnsupportedOperationException("ALTER DROP COLUMN execution must be handled by Database");
    }

    @Override
    public String toString() {
        return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName +
               " ALGORITHM=" + algorithm + " LOCK=" + lockMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterTableDropColumnQuery that = (AlterTableDropColumnQuery) o;
        return tableName.equals(that.tableName) &&
               columnName.equals(that.columnName) &&
               algorithm == that.algorithm &&
               lockMode == that.lockMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName, algorithm, lockMode);
    }
}