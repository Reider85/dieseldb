package diesel.concurrency;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;

/**
 * Manager for savepoints within a transaction (prompt4.md #26).
 *
 * <p>Tracks savepoint creation count and wraps a {@link NestedSavepointStack}
 * for hierarchical savepoint operations.
 *
 * <p>Implements AutoCloseable for future resource cleanup.
 */
public final class SavepointManager implements AutoCloseable {

    private final NestedSavepointStack stack = new NestedSavepointStack();
    private final AtomicLong savepointCount = new AtomicLong(0);

    /**
     * Creates a new savepoint manager.
     */
    public SavepointManager() {
        // JMX registration removed for simplicity
    }

    /**
     * Creates a new savepoint with the given name.
     *
     * @param name           the savepoint name
     * @param modifiedSnapshot defensive copy of modified rows
     * @param deletedSnapshot defensive copy of deleted rows
     * @return the created savepoint
     */
    public Savepoint createSavepoint(String name,
                                    Map<String, Set<Integer>> modifiedSnapshot,
                                    Map<String, Set<Integer>> deletedSnapshot) {
        Savepoint savepoint = stack.push(name, modifiedSnapshot, deletedSnapshot);
        savepointCount.incrementAndGet();
        return savepoint;
    }

    /**
     * Finds the newest non-released savepoint with the given name.
     *
     * @param name the savepoint name (case-insensitive)
     * @return the savepoint, or empty if not found
     */
    public Optional<Savepoint> findSavepoint(String name) {
        return stack.findByName(name);
    }

    /**
     * Rolls back to the specified savepoint.
     *
     * @param name the savepoint name (case-insensitive)
     * @return rollback result (target + invalidated savepoints)
     * @throws IllegalArgumentException if savepoint not found
     */
    public NestedSavepointStack.RollbackResult rollbackToSavepoint(String name) {
        return stack.rollbackTo(name);
    }

    /**
     * Releases the savepoint and all its descendants.
     *
     * @param name the savepoint name (case-insensitive)
     * @return list of released savepoints
     * @throws IllegalArgumentException if savepoint not found
     */
    public List<Savepoint> releaseSavepoint(String name) {
        return stack.release(name);
    }

    /**
     * Clears all savepoints.
     */
    public void clear() {
        stack.clear();
    }

    /**
     * Returns the number of savepoints created (JMX metric).
     */
    public long getSavepointCount() {
        return savepointCount.get();
    }

    /**
     * Returns whether the stack is empty.
     */
    public boolean isEmpty() {
        return stack.isEmpty();
    }

    /**
     * Returns the underlying stack (for testing/debugging).
     */
    NestedSavepointStack getStack() {
        return stack;
    }

    @Override
    public void close() {
        // No JMX resources to clean up for now
    }

    }