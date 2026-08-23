package diesel;

/**
 * Parsed representation of a BEGIN TRANSACTION statement. The actual
 * transaction is started by {@link Database} directly, so {@link #execute}
 * is not used.
 *
 * @see TransactionQuery
 * @see Database
 */
class BeginTransactionQuery implements TransactionQuery {
    private final IsolationLevel isolationLevel;

    /**
     * Creates a BEGIN TRANSACTION query with the given isolation level.
     *
     * @param isolationLevel the isolation level, or null for the default
     */
    public BeginTransactionQuery(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * Returns the isolation level requested by the statement.
     *
     * @return the isolation level, possibly null
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Not supported; BEGIN TRANSACTION is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("unused")
    public String execute(Table table) {
        throw new UnsupportedOperationException("BeginTransactionQuery should be handled by Database directly");
    }
}