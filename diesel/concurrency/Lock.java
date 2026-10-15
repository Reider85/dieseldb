package diesel.concurrency;

import java.util.Objects;

/**
 * A lock held by a transaction on a specific resource.
 * <p>
 * Locks are immutable and uniquely identified by resource and txid.
 * A transaction can hold multiple locks on the same resource (e.g., S then upgrade to X).
 */
public final class Lock {

    private final String resource;
    private final LockMode mode;
    private final long txid;

    /**
     * Creates a new lock.
     *
     * @param resource the resource being locked (e.g., "table:USERS", "row:USERS#42")
     * @param mode the lock mode
     * @param txid the transaction ID holding the lock
     */
    public Lock(String resource, LockMode mode, long txid) {
        this.resource = Objects.requireNonNull(resource, "Resource cannot be null");
        this.mode = Objects.requireNonNull(mode, "Lock mode cannot be null");
        this.txid = txid;
    }

    /**
     * Returns the resource being locked.
     */
    public String getResource() {
        return resource;
    }

    /**
     * Returns the lock mode.
     */
    public LockMode getMode() {
        return mode;
    }

    /**
     * Returns the transaction ID holding the lock.
     */
    public long getTxid() {
        return txid;
    }

    /**
     * Merges this lock with another lock held by the same transaction on the same resource.
     * This is used when a transaction requests the same lock mode again (idempotent)
     * or upgrades from a weaker to a stronger lock mode.
     *
     * @param other the other lock held by the same transaction
     * @return this lock (unchanged for same mode), or the stronger lock for upgrades
     * @throws IllegalArgumentException if the locks are for different resources or txids
     */
    public Lock merge(Lock other) {
        if (!this.resource.equals(other.resource)) {
            throw new IllegalArgumentException("Cannot merge locks for different resources: " +
                    this.resource + " vs " + other.resource);
        }
        if (this.txid != other.txid) {
            throw new IllegalArgumentException("Cannot merge locks for different transactions: " +
                    this.txid + " vs " + other.txid);
        }

        // For same mode, return this lock (idempotent)
        if (this.mode == other.mode) {
            return this;
        }

        // For upgrades, return the stronger lock mode
        // Standard upgrade hierarchy: S -> X, IS -> IX -> X
        return isStronger(this.mode, other.mode) ? other : this;
    }

    /**
     * Checks if one lock mode is stronger than another for the same transaction.
     * Stronger locks can be upgraded to weaker ones, but not vice versa.
     */
    private boolean isStronger(LockMode weaker, LockMode stronger) {
        if (weaker == stronger) return false;
        if (weaker == LockMode.S && stronger == LockMode.X) return true;
        if (weaker == LockMode.IS && stronger == LockMode.IX) return true;
        if (weaker == LockMode.IS && stronger == LockMode.X) return true;
        if (weaker == LockMode.IX && stronger == LockMode.X) return true;
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return txid == lock.txid && resource.equals(lock.resource) && mode == lock.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, mode, txid);
    }

    @Override
    public String toString() {
        return "Lock{" +
                "resource='" + resource + '\'' +
                ", mode=" + mode +
                ", txid=" + txid +
                '}';
    }
}