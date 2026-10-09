package diesel.recovery;

/**
 * Immutable result of one ARIES undo pass (prompt 4 #19).
 *
 * <p>Buckets: {@code undoneInserts}/{@code undoneUpdates}/{@code undoneDeletes}
 * count the logical undo operations delivered to the {@link MvccUndoSink};
 * {@code ignored} counts WAL records inside the scan that carry no undo work
 * (BEGIN, ABORT, COMMIT, CHECKPOINT, PAGE_IMAGE, and DML records of committed
 * transactions).
 */
public final class UndoResult {

    private final long undoneInserts;
    private final long undoneUpdates;
    private final long undoneDeletes;
    private final long ignored;
    private final long lastLsn;

    /**
     * Creates an undo result.
     *
     * @param undoneInserts number of INSERT undos delivered
     * @param undoneUpdates number of UPDATE undos delivered
     * @param undoneDeletes number of DELETE undos delivered
     * @param ignored       number of records scanned without undo work
     * @param lastLsn       the end-of-log LSN snapshot the pass used
     */
    public UndoResult(long undoneInserts, long undoneUpdates, long undoneDeletes,
                      long ignored, long lastLsn) {
        this.undoneInserts = requireNonNegative(undoneInserts, "undoneInserts");
        this.undoneUpdates = requireNonNegative(undoneUpdates, "undoneUpdates");
        this.undoneDeletes = requireNonNegative(undoneDeletes, "undoneDeletes");
        this.ignored = requireNonNegative(ignored, "ignored");
        this.lastLsn = lastLsn;
    }

    private static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0, got " + value);
        }
        return value;
    }

    /**
     * Returns the number of INSERT undo operations delivered.
     */
    public long getUndoneInserts() {
        return undoneInserts;
    }

    /**
     * Returns the number of UPDATE undo operations delivered.
     */
    public long getUndoneUpdates() {
        return undoneUpdates;
    }

    /**
     * Returns the number of DELETE undo operations delivered.
     */
    public long getUndoneDeletes() {
        return undoneDeletes;
    }

    /**
     * Returns the number of WAL records scanned without undo work.
     */
    public long getIgnored() {
        return ignored;
    }

    /**
     * Returns the number of logical undo operations delivered in total.
     */
    public long getTotalUndone() {
        return undoneInserts + undoneUpdates + undoneDeletes;
    }

    /**
     * Returns the number of WAL records scanned by this pass
     * (undone operations + ignored records).
     */
    public long getTotalRecords() {
        return getTotalUndone() + ignored;
    }

    /**
     * Returns the end-of-log LSN snapshot the pass used.
     */
    public long getLastLsn() {
        return lastLsn;
    }

    @Override
    public String toString() {
        return "UndoResult{inserts=" + undoneInserts
                + ", updates=" + undoneUpdates
                + ", deletes=" + undoneDeletes
                + ", ignored=" + ignored
                + ", lastLsn=" + lastLsn + '}';
    }
}
