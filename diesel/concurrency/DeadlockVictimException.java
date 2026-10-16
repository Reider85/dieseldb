package diesel.concurrency;

import java.util.List;
import java.util.Objects;

/**
 * Exception thrown when a transaction is aborted due to deadlock.
 * <p>
 * This exception is thrown by LockManager.acquire() when a transaction is selected
 * as a deadlock victim. The transaction should catch this exception, release its locks,
 * and retry the operation.
 */
public final class DeadlockVictimException extends RuntimeException {

    private final long victimTxid;
    private final List<Long> cycle;

    /**
     * Creates a new deadlock victim exception.
     *
     * @param victimTxid the transaction ID that was aborted
     * @param cycle the wait-for cycle that caused the deadlock (for logging/retry context)
     */
    public DeadlockVictimException(long victimTxid, List<Long> cycle) {
        super(buildMessage(victimTxid, cycle));
        this.victimTxid = victimTxid;
        this.cycle = List.copyOf(Objects.requireNonNull(cycle, "Cycle cannot be null"));
    }

    /**
     * Returns the transaction ID that was aborted.
     */
    public long getVictimTxid() {
        return victimTxid;
    }

    /**
     * Returns the wait-for cycle that caused the deadlock.
     * The cycle is a list of transaction IDs forming the cycle path.
     */
    public List<Long> getCycle() {
        return cycle;
    }

    /**
     * Builds an informative exception message including the cycle path.
     */
    private static String buildMessage(long victimTxid, List<Long> cycle) {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < cycle.size(); i++) {
            if (i > 0) {
                path.append(" → ");
            }
            path.append(cycle.get(i));
        }
        
        return String.format("Transaction %d aborted: deadlock detected (wait-for cycle: %s); victim txid=%d. Retry recommended.",
                victimTxid, path, victimTxid);
    }
}