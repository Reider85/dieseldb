package diesel.recovery;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Immutable result of the ARIES analysis phase.
 *
 * <p>Holds the three outputs defined by the ARIES analysis algorithm:
 * <ul>
 *   <li><b>committed</b> — transactions whose COMMIT record was seen in the scanned
 *       portion of the WAL (from the last checkpoint to the end of the log).</li>
 *   <li><b>active</b> — transactions that were still running when the log ended
 *       (seeded from the checkpoint's active-txid list, then updated by the scan).
 *       These are the transactions the undo phase must roll back.</li>
 *   <li><b>lastLSN</b> — the end-of-log LSN captured when analysis started; the redo
 *       phase replays records with {@code checkpoint.lastLSN < lsn <= lastLSN}.</li>
 * </ul>
 *
 * <p>Instances are immutable: the sets are defensively copied and unmodifiable.
 *
 * @see AnalysisPhase
 */
public final class AnalysisResult {

    private final Set<Long> committed;
    private final Set<Long> active;
    private final long lastLSN;

    /**
     * Creates a new analysis result.
     *
     * @param committed transactions committed after the checkpoint
     * @param active    transactions still active at end of log
     * @param lastLSN   end-of-log LSN captured at analysis start
     * @throws NullPointerException if {@code committed} or {@code active} is null
     */
    public AnalysisResult(Set<Long> committed, Set<Long> active, long lastLSN) {
        Objects.requireNonNull(committed, "committed");
        Objects.requireNonNull(active, "active");
        this.committed = Collections.unmodifiableSet(new LinkedHashSet<>(committed));
        this.active = Collections.unmodifiableSet(new LinkedHashSet<>(active));
        this.lastLSN = lastLSN;
    }

    /**
     * Returns the unmodifiable set of committed transaction ids.
     *
     * @return committed txids (insertion-ordered)
     */
    public Set<Long> getCommitted() {
        return committed;
    }

    /**
     * Returns the unmodifiable set of active (uncommitted) transaction ids.
     *
     * @return active txids (insertion-ordered)
     */
    public Set<Long> getActive() {
        return active;
    }

    /**
     * Returns the end-of-log LSN captured when analysis started.
     *
     * @return the last LSN of the scanned log
     */
    public long getLastLSN() {
        return lastLSN;
    }

    /**
     * Returns whether the given transaction committed.
     *
     * @param txid the transaction id
     * @return true if the transaction is in the committed set
     */
    public boolean isCommitted(long txid) {
        return committed.contains(txid);
    }

    /**
     * Returns whether the given transaction is still active (must be undone).
     *
     * @param txid the transaction id
     * @return true if the transaction is in the active set
     */
    public boolean isActive(long txid) {
        return active.contains(txid);
    }

    /**
     * Returns the number of committed transactions.
     *
     * @return committed set size
     */
    public int getCommittedTxidCount() {
        return committed.size();
    }

    /**
     * Returns the number of active (uncommitted) transactions.
     *
     * @return active set size
     */
    public int getActiveTxidCount() {
        return active.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AnalysisResult other)) {
            return false;
        }
        return lastLSN == other.lastLSN
                && committed.equals(other.committed)
                && active.equals(other.active);
    }

    @Override
    public int hashCode() {
        return Objects.hash(committed, active, lastLSN);
    }

    @Override
    public String toString() {
        return "AnalysisResult{committed=" + new TreeSet<>(committed)
                + ", active=" + new TreeSet<>(active)
                + ", lastLSN=" + lastLSN + "}";
    }
}
