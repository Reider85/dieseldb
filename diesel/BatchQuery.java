package diesel;

/**
 * Parsed representation of a BEGIN BATCH / END BATCH statement. Batch mode
 * executes multiple DML statements within a single transaction with deferred
 * index updates for improved performance. All index rebuilds are deferred until
 * the END BATCH statement is encountered, at which point indexes are rebuilt
 * once for the entire batch.
 *
 * <p>The actual batch execution is handled by {@link Database} directly, so
 * {@link #execute} is not used.
 *
 * @see TransactionQuery
 * @see Database
 */
class BatchQuery implements TransactionQuery {

    /**
     * The batch operation mode.
     */
    enum Mode {
        /** Starts a batch: begins a transaction and enables deferred index updates. */
        BEGIN_BATCH,
        /** Ends a batch: flushes deferred indexes and commits the transaction. */
        END_BATCH
    }

    private final Mode mode;

    /**
     * Creates a batch query with the given mode.
     *
     * @param mode the batch operation mode (BEGIN_BATCH or END_BATCH)
     */
    public BatchQuery(Mode mode) {
        this.mode = mode;
    }

    /**
     * Returns the batch operation mode.
     *
     * @return the mode (BEGIN_BATCH or END_BATCH)
     */
    public Mode getMode() {
        return mode;
    }

    /**
     * Not supported; BEGIN BATCH / END BATCH is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("unused")
    public String execute(Table table) {
        throw new UnsupportedOperationException("BatchQuery should be handled by Database directly");
    }
}