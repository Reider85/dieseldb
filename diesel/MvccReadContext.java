package diesel;

/**
 * Thread-local MVCC reader context, installed by {@link Database} for the
 * duration of a query dispatch. Every read path consults it through
 * {@link Table#isRowVisibleToReader(int)} to decide which rows the calling
 * client may see.
 *
 * <p><b>Why a ThreadLocal:</b> the scan paths inside {@code SelectQuery} are
 * reached through a deep call chain that does not carry the caller's
 * transaction; the context makes the reader's identity available at the single
 * visibility choke point without changing every signature.
 *
 * <p><b>Context kinds:</b>
 * <ul>
 *   <li><b>Auto-commit reader</b> ({@code transaction == null}): sees every
 *       committed row ({@code snapshotCsn = Long.MAX_VALUE}) and no uncommitted
 *       or aborted row.</li>
 *   <li><b>Explicit transaction</b>: sees its own writes, rows committed at or
 *       before its BEGIN-time {@code snapshotCsn}, and — only at
 *       {@link IsolationLevel#READ_UNCOMMITTED} — the uncommitted writes of
 *       other active transactions (dirty reads). Rows committed by other
 *       transactions <em>after</em> its BEGIN stay invisible (the BEGIN-time
 *       snapshot semantics the prompt67/prompt68 acceptance tests require).</li>
 *   <li><b>Batch transaction</b>: reads a CoW copy, so it filters exactly like
 *       an auto-commit reader; its DML still records undo against the batch
 *       transaction.</li>
 * </ul>
 *
 * <p>A {@code null} context (query executed outside {@code Database.dispatch},
 * e.g. a unit test calling {@code query.execute(table)} directly) disables MVCC
 * filtering and falls back to legacy tombstone-only behaviour.
 */
public final class MvccReadContext {

    /** The reader identity captured for the current query execution. */
    public static final class Context {
        /** The caller's active transaction, or null for an auto-commit reader. */
        final Transaction transaction;
        /** Transaction id whose writes count as "own"; -1 for auto-commit readers. */
        final long readTxid;
        /** Commit CSN at BEGIN; rows committed later stay invisible. */
        final long snapshotCsn;
        /** True when the reader may observe other transactions' uncommitted writes. */
        final boolean readUncommitted;
        /** True for batch (CoW) transactions. */
        final boolean batch;

        Context(Transaction transaction, long readTxid, long snapshotCsn,
                boolean readUncommitted, boolean batch) {
            this.transaction = transaction;
            this.readTxid = readTxid;
            this.snapshotCsn = snapshotCsn;
            this.readUncommitted = readUncommitted;
            this.batch = batch;
        }

        /** Dirty reads (another transaction's uncommitted rows) are allowed. */
        boolean allowsDirtyReads() {
            return transaction != null && readUncommitted && !batch;
        }

        Transaction getTransaction() {
            return transaction;
        }

        boolean isBatch() {
            return batch;
        }
    }

    /** Auto-commit reader: every committed row visible, nothing uncommitted. */
    private static final Context AUTO_COMMIT =
            new Context(null, -1L, Long.MAX_VALUE, false, false);

    private static final ThreadLocal<Context> CURRENT = new ThreadLocal<>();

    private MvccReadContext() {
        throw new AssertionError("Utility class, do not instantiate");
    }

    /**
     * Builds the reader context for a query issued by the given transaction.
     *
     * @param transaction the caller's transaction, or null for auto-commit
     * @return the context to install (never null)
     */
    public static Context contextFor(Transaction transaction) {
        if (transaction == null || !transaction.isActive()) {
            return AUTO_COMMIT;
        }
        boolean dirtyAllowed = transaction.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED;
        if (transaction.isBatchMode()) {
            // Batch reads a CoW copy: filter like auto-commit, but keep the
            // transaction reference so its DML can log undo records.
            return new Context(transaction, -1L, Long.MAX_VALUE, dirtyAllowed, true);
        }
        return new Context(transaction, transaction.getTxid(), transaction.getSnapshotCsn(),
                dirtyAllowed, false);
    }

    /** Returns the context of the query currently executing on this thread. */
    public static Context get() {
        return CURRENT.get();
    }

    /**
     * Installs the context for the current thread.
     *
     * @param context the context, or null to clear
     */
    public static void set(Context context) {
        if (context == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(context);
        }
    }

    /**
     * Core MVCC visibility decision for a row with version metadata.
     *
     * <p>The decision itself is delegated to the canonical contract
     * {@link TupleVisibility#visibleByStatus} so that every read path
     * (scans, index lookups, unique-constraint checks) shares one rule set.
     *
     * @param database the owning database (for the transaction status tracker)
     * @param meta     the row's version metadata (never null)
     * @param ctx      the reader context; null disables MVCC filtering
     * @return true if the row must be returned to this reader
     */
    static boolean isRowVisible(Database database, RowVersionMeta meta, Context ctx) {
        if (ctx == null) {
            // No reader context (query outside Database.dispatch): legacy rule —
            // hide rows that carry an explicit delete mark, show everything else.
            return meta.isAlive();
        }
        TxStatusTracker tracker = tracker(database);
        java.util.function.LongPredicate committedBeforeSnapshot =
                txid -> tracker == null || tracker.isCommittedBefore(txid, ctx.snapshotCsn);
        java.util.function.LongFunction<TxStatusTracker.TxStatus> statusOf =
                tracker == null ? txid -> null : tracker::getStatus;
        return TupleVisibility.visibleByStatus(meta.getXmin(), meta.getXmax(), ctx.readTxid,
                ctx.allowsDirtyReads(), committedBeforeSnapshot, statusOf);
    }

    private static TxStatusTracker tracker(Database database) {
        return database == null ? null : database.getTxStatusTracker();
    }
}
