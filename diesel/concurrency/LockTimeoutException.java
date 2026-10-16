package diesel.concurrency;

import java.util.Objects;

/**
 * Exception thrown when a transaction's lock wait exceeds the timeout period.
 * <p>
 * This exception is thrown by LockManager.acquire() when a transaction has waited
 * for a lock longer than the configured timeout. The transaction should catch this
 * exception, release its locks, and retry the operation.
 * <p>
 * Similar to DeadlockVictimException, this is a RuntimeException to match
 * the existing exception pattern in the concurrency package.
 */
public final class LockTimeoutException extends RuntimeException {

    private final long txid;
    private final String resource;
    private final long timeoutMs;

    /**
     * Creates a new lock timeout exception.
     *
     * @param txid the transaction ID that timed out
     * @param resource the resource that could not be locked in time
     * @param timeoutMs the configured timeout that was exceeded
     */
    public LockTimeoutException(long txid, String resource, long timeoutMs) {
        super(buildMessage(txid, resource, timeoutMs));
        this.txid = txid;
        this.resource = Objects.requireNonNull(resource, "Resource cannot be null");
        this.timeoutMs = Math.max(1, timeoutMs); // Ensure positive timeout
    }

    /**
     * Returns the transaction ID that timed out.
     */
    public long getTxid() {
        return txid;
    }

    /**
     * Returns the resource that could not be locked in time.
     */
    public String getResource() {
        return resource;
    }

    /**
     * Returns the configured timeout in milliseconds that was exceeded.
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Builds an informative exception message including the resource and timeout.
     */
    private static String buildMessage(long txid, String resource, long timeoutMs) {
        return String.format("Transaction %d lock wait timeout on resource '%s' after %d ms; transaction should retry.",
                txid, resource, timeoutMs);
    }
}