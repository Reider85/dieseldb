package diesel.concurrency;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

/**
 * A savepoint within a transaction (prompt4.md #26).
 *
 * <p>A savepoint captures the transaction state at a specific point: the undo log
 * marker position and a snapshot of modified/deleted rows for bookkeeping restoration
 * on ROLLBACK TO. Savepoints are hierarchical via parentSequence; releasing a parent
 * releases all descendants.
 *
 * <p>Immutable once created.
 */
public final class Savepoint {

    private final String name;
    private final long sequence;
    private final long parentSequence;
    private final Map<String, Set<Integer>> modifiedSnapshot;
    private final Map<String, Set<Integer>> deletedSnapshot;

    /**
     * Creates a savepoint.
     *
     * @param name           the savepoint name (case-sensitive, normalized to upper)
     * @param sequence       the monotonic sequence number (assigned by SavepointStack)
     * @param parentSequence the parent savepoint sequence (-1 if root)
     * @param modifiedSnapshot defensive copy of Transaction.modifiedRows at savepoint time
     * @param deletedSnapshot defensive copy of Transaction.deletedRows at savepoint time
     */
    public Savepoint(String name, long sequence, long parentSequence,
                     Map<String, Set<Integer>> modifiedSnapshot,
                     Map<String, Set<Integer>> deletedSnapshot) {
        this.name = Objects.requireNonNull(name, "name");
        this.sequence = sequence;
        this.parentSequence = parentSequence;
        this.modifiedSnapshot = Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(modifiedSnapshot, "modifiedSnapshot")));
        this.deletedSnapshot = Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(deletedSnapshot, "deletedSnapshot")));
    }

    /**
     * Returns the savepoint name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the monotonic sequence number (for stack ordering and rollbackTo logic).
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns the parent savepoint sequence (-1 if none).
     */
    public long getParentSequence() {
        return parentSequence;
    }

    /**
     * Returns the defensive copy of modified rows at savepoint time.
     */
    public Map<String, Set<Integer>> getModifiedSnapshot() {
        return modifiedSnapshot;
    }

    /**
     * Returns the defensive copy of deleted rows at savepoint time.
     */
    public Map<String, Set<Integer>> getDeletedSnapshot() {
        return deletedSnapshot;
    }

    @Override
    public String toString() {
        return "Savepoint{name='" + name + "', sequence=" + sequence + 
                ", parent=" + (parentSequence == -1 ? "none" : parentSequence) + 
                ", modified=" + modifiedSnapshot.size() + ", deleted=" + deletedSnapshot.size() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Savepoint savepoint = (Savepoint) o;
        return sequence == savepoint.sequence &&
                parentSequence == savepoint.parentSequence &&
                name.equals(savepoint.name) &&
                modifiedSnapshot.equals(savepoint.modifiedSnapshot) &&
                deletedSnapshot.equals(savepoint.deletedSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, sequence, parentSequence, modifiedSnapshot, deletedSnapshot);
    }
}