package diesel.recovery;

/**
 * Outcome of a point-in-time recovery pass (prompt 4 #43).
 *
 * <p>Immutable counters describing what the pass classified and applied.
 * All "hidden" counters count durable hide operations (tombstones and value
 * restores), not in-memory stamps — the restored directory must stay correct
 * after a later reopen.
 */
public final class PitrResult {

    private final long targetTimestampMs;
    private final long endLsn;
    private final int visibleTxids;
    private final int hiddenTxids;
    private final long appliedInserts;
    private final long appliedUpdates;
    private final long tombstonedHiddenRows;
    private final long restoredHiddenUpdates;
    private final long commitTombstones;
    private final long skippedRecords;
    private final long legacyCommits;
    private final long maxTxid;
    private final long maxCommitCsn;

    /** Creates a result snapshot. */
    public PitrResult(long targetTimestampMs, long endLsn,
                      int visibleTxids, int hiddenTxids,
                      long appliedInserts, long appliedUpdates,
                      long tombstonedHiddenRows, long restoredHiddenUpdates,
                      long commitTombstones, long skippedRecords,
                      long legacyCommits, long maxTxid, long maxCommitCsn) {
        this.targetTimestampMs = targetTimestampMs;
        this.endLsn = endLsn;
        this.visibleTxids = visibleTxids;
        this.hiddenTxids = hiddenTxids;
        this.appliedInserts = appliedInserts;
        this.appliedUpdates = appliedUpdates;
        this.tombstonedHiddenRows = tombstonedHiddenRows;
        this.restoredHiddenUpdates = restoredHiddenUpdates;
        this.commitTombstones = commitTombstones;
        this.skippedRecords = skippedRecords;
        this.legacyCommits = legacyCommits;
        this.maxTxid = maxTxid;
        this.maxCommitCsn = maxCommitCsn;
    }

    /** Returns the restore target in epoch millis. */
    public long getTargetTimestampMs() {
        return targetTimestampMs;
    }

    /** Returns the end-of-log LSN the pass observed. */
    public long getEndLsn() {
        return endLsn;
    }

    /** Returns the number of transactions classified visible (committed at/before target). */
    public int getVisibleTxids() {
        return visibleTxids;
    }

    /** Returns the number of transactions classified hidden (committed after target, aborted, or active). */
    public int getHiddenTxids() {
        return hiddenTxids;
    }

    /** Returns the number of INSERT records physically applied (rows appended). */
    public long getAppliedInserts() {
        return appliedInserts;
    }

    /** Returns the number of UPDATE records physically applied. */
    public long getAppliedUpdates() {
        return appliedUpdates;
    }

    /** Returns hidden-transaction INSERT rows tombstoned in the restored files. */
    public long getTombstonedHiddenRows() {
        return tombstonedHiddenRows;
    }

    /** Returns hidden-transaction UPDATE rows restored to their before-image. */
    public long getRestoredHiddenUpdates() {
        return restoredHiddenUpdates;
    }

    /** Returns rows tombstoned by visible COMMIT payloads (commit-time deletes). */
    public long getCommitTombstones() {
        return commitTombstones;
    }

    /** Returns records skipped by range/value guards (never corruption). */
    public long getSkippedRecords() {
        return skippedRecords;
    }

    /** Returns legacy COMMIT records without a PITR timestamp (treated visible, warned). */
    public long getLegacyCommits() {
        return legacyCommits;
    }

    /** Returns the highest txid seen (tracker flooring). */
    public long getMaxTxid() {
        return maxTxid;
    }

    /** Returns the highest commit CSN replayed (CSN allocator flooring). */
    public long getMaxCommitCsn() {
        return maxCommitCsn;
    }

    @Override
    public String toString() {
        return "PitrResult{targetTs=" + targetTimestampMs
                + ", endLsn=" + endLsn
                + ", visibleTxids=" + visibleTxids
                + ", hiddenTxids=" + hiddenTxids
                + ", appliedInserts=" + appliedInserts
                + ", appliedUpdates=" + appliedUpdates
                + ", hiddenTombstones=" + tombstonedHiddenRows
                + ", hiddenRestores=" + restoredHiddenUpdates
                + ", commitTombstones=" + commitTombstones
                + ", skipped=" + skippedRecords
                + ", legacyCommits=" + legacyCommits + '}';
    }
}
