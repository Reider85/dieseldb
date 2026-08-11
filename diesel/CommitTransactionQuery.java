package diesel;

/**
 * Parsed representation of a COMMIT TRANSACTION statement. The actual commit
 * is performed by {@link Database} directly, so {@link #execute} is not used.
 *
 * @see TransactionQuery
 * @see Database
 */
class CommitTransactionQuery implements TransactionQuery {
    /**
     * Not supported; COMMIT TRANSACTION is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("CommitTransactionQuery should be handled by Database directly");
    }
}