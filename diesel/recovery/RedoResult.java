package diesel.recovery;

import java.util.Objects;

/**
 * Summary of an ARIES redo pass (prompt 4 #18).
 *
 * <p>Every record of the scanned window falls into exactly one bucket:
 * <ul>
 *   <li>{@link #getApplied()} — page after-images installed on a page;</li>
 *   <li>{@link #getSkipped()} — page after-images whose page LSN already
 *       satisfied the check (already redone, no-op);</li>
 *   <li>{@link #getCommitsReplayed()} — COMMIT records parsed and delivered to
 *       the {@link MvccRedoSink} (MVCC xmin/xmax restoration);</li>
 *   <li>{@link #getIgnored()} — everything else (BEGIN, DML, ABORT,
 *       CHECKPOINT, COMMIT without a payload).</li>
 * </ul>
 * The four counts sum to {@link #getTotalRecords()} — the number of records in
 * the window {@code checkpoint.lastLSN < lsn <= lastLSN}.
 */
public final class RedoResult {

    private final long applied;
    private final long skipped;
    private final long commitsReplayed;
    private final long ignored;
    private final long lastLsn;

    /**
     * Creates a redo result.
     *
     * @param applied         page images applied
     * @param skipped         page images skipped by the page LSN check
     * @param commitsReplayed COMMIT payloads delivered to the MVCC sink
     * @param ignored         records without redo semantics in this phase
     * @param lastLsn         end-of-log snapshot of the window
     */
    public RedoResult(long applied, long skipped, long commitsReplayed, long ignored, long lastLsn) {
        if (applied < 0 || skipped < 0 || commitsReplayed < 0 || ignored < 0) {
            throw new IllegalArgumentException("Redo counters must be non-negative");
        }
        this.applied = applied;
        this.skipped = skipped;
        this.commitsReplayed = commitsReplayed;
        this.ignored = ignored;
        this.lastLsn = lastLsn;
    }

    /**
     * Returns how many page after-images were installed.
     */
    public long getApplied() {
        return applied;
    }

    /**
     * Returns how many page after-images were no-ops (page LSN already current).
     */
    public long getSkipped() {
        return skipped;
    }

    /**
     * Returns how many COMMIT payloads were replayed to the MVCC sink.
     */
    public long getCommitsReplayed() {
        return commitsReplayed;
    }

    /**
     * Returns how many window records carried no redo semantics.
     */
    public long getIgnored() {
        return ignored;
    }

    /**
     * Returns the end-of-log snapshot (window upper bound).
     */
    public long getLastLsn() {
        return lastLsn;
    }

    /**
     * Returns the total number of records scanned in the window.
     */
    public long getTotalRecords() {
        return applied + skipped + commitsReplayed + ignored;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedoResult other)) {
            return false;
        }
        return applied == other.applied && skipped == other.skipped
                && commitsReplayed == other.commitsReplayed
                && ignored == other.ignored && lastLsn == other.lastLsn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(applied, skipped, commitsReplayed, ignored, lastLsn);
    }

    @Override
    public String toString() {
        return "RedoResult{applied=" + applied + ", skipped=" + skipped
                + ", commitsReplayed=" + commitsReplayed + ", ignored=" + ignored
                + ", lastLsn=" + lastLsn + '}';
    }
}
