package diesel.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A nested stack of savepoints with parent-child relationships (prompt4.md #26).
 *
 * <p>Supports creating savepoints (with optional duplicate name replacement),
 * finding by name (newest non-released), rolling back to (removing descendants),
 * and releasing (removing a node and all its descendants).
 *
 * <p>Not thread-safe; designed for per-transaction use within Database.execute*.
 */
public final class NestedSavepointStack {

    private final Map<String, Savepoint> nameToSavepoint = new LinkedHashMap<>();
    private final Map<Long, Savepoint> sequenceToSavepoint = new LinkedHashMap<>();
    private long nextSequence = 1L;

    /**
     * Creates a new savepoint and pushes it onto the stack.
     *
     * <p>If a savepoint with the same name already exists, it is removed
     * (standard SQL behavior: duplicate names replace the old one).
     *
     * @param name           the savepoint name (case-sensitive, will be normalized to upper)
     * @param modifiedSnapshot defensive copy of Transaction.modifiedRows
     * @param deletedSnapshot defensive copy of Transaction.deletedRows
     * @return the new savepoint
     */
    public Savepoint push(String name,
                          Map<String, Set<Integer>> modifiedSnapshot,
                          Map<String, Set<Integer>> deletedSnapshot) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(modifiedSnapshot, "modifiedSnapshot");
        Objects.requireNonNull(deletedSnapshot, "deletedSnapshot");

        // Remove existing savepoint with same name (standard SQL)
        nameToSavepoint.entrySet().removeIf(entry -> entry.getKey().equalsIgnoreCase(name));

        long parentSequence = sequenceToSavepoint.isEmpty() ? -1L : 
                sequenceToSavepoint.values().stream()
                        .mapToLong(Savepoint::getSequence)
                        .max()
                        .orElse(-1L);

        Savepoint savepoint = new Savepoint(name.toUpperCase(), nextSequence++, parentSequence,
                modifiedSnapshot, deletedSnapshot);

        nameToSavepoint.put(savepoint.getName(), savepoint);
        sequenceToSavepoint.put(savepoint.getSequence(), savepoint);

        return savepoint;
    }

    /**
     * Finds the newest non-released savepoint with the given name.
     *
     * @param name the savepoint name (case-insensitive)
     * @return the savepoint, or empty if not found or already released
     */
    public Optional<Savepoint> findByName(String name) {
        Objects.requireNonNull(name, "name");
        return Optional.ofNullable(nameToSavepoint.get(name.toUpperCase()));
    }

    /**
     * Rolls back to the specified savepoint: removes all savepoints with sequence
     * greater than the target (they are invalidated by the rollback).
     *
     * <p>The target savepoint remains valid and can be rolled back to again.
     *
     * @param name the target savepoint name (case-insensitive)
     * @return the target savepoint and list of invalidated savepoints (may be empty)
     * @throws IllegalArgumentException if no savepoint with this name exists
     */
    public RollbackResult rollbackTo(String name) {
        Objects.requireNonNull(name, "name");
        Savepoint target = findByName(name)
                .orElseThrow(() -> new IllegalArgumentException("Savepoint not found: " + name));

        List<Savepoint> invalidated = new ArrayList<>();
        sequenceToSavepoint.entrySet().removeIf(entry -> {
            if (entry.getKey() > target.getSequence()) {
                invalidated.add(entry.getValue());
                nameToSavepoint.remove(entry.getValue().getName());
                return true;
            }
            return false;
        });

        return new RollbackResult(target, invalidated);
    }

    /**
     * Releases the savepoint with the given name and all its descendants.
     *
     * <p>Descendants are found by following parentSequence chains downward.
     *
     * @param name the savepoint name (case-insensitive)
     * @return list of released savepoints (including the named one)
     * @throws IllegalArgumentException if no savepoint with this name exists
     */
    public List<Savepoint> release(String name) {
        Objects.requireNonNull(name, "name");
        Savepoint target = findByName(name)
                .orElseThrow(() -> new IllegalArgumentException("Savepoint not found: " + name));

        List<Savepoint> released = new ArrayList<>();
        releaseRecursive(target, released);
        return released;
    }

    private void releaseRecursive(Savepoint savepoint, List<Savepoint> released) {
        // Find direct children (parentSequence == this savepoint's sequence)
        List<Savepoint> children = sequenceToSavepoint.values().stream()
                .filter(s -> s.getParentSequence() == savepoint.getSequence())
                .toList();

        released.add(savepoint);
        nameToSavepoint.remove(savepoint.getName());
        sequenceToSavepoint.remove(savepoint.getSequence());

        // Recursively release children
        for (Savepoint child : children) {
            releaseRecursive(child, released);
        }
    }

    /**
     * Clears all savepoints from the stack.
     */
    public void clear() {
        nameToSavepoint.clear();
        sequenceToSavepoint.clear();
        nextSequence = 1L;
    }

    /**
     * Returns the number of savepoints in the stack.
     */
    public int size() {
        return sequenceToSavepoint.size();
    }

    /**
     * Returns whether the stack is empty.
     */
    public boolean isEmpty() {
        return sequenceToSavepoint.isEmpty();
    }

    /**
     * Result of a rollbackTo operation: the target savepoint and list of invalidated ones.
     */
    public static final class RollbackResult {
        private final Savepoint target;
        private final List<Savepoint> invalidated;

        public RollbackResult(Savepoint target, List<Savepoint> invalidated) {
            this.target = Objects.requireNonNull(target, "target");
            this.invalidated = Collections.unmodifiableList(
                    new ArrayList<>(Objects.requireNonNull(invalidated, "invalidated")));
        }

        public Savepoint getTarget() {
            return target;
        }

        public List<Savepoint> getInvalidated() {
            return invalidated;
        }
    }
}