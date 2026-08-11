package diesel;

/**
 * Parsed representation of a ROLLBACK TRANSACTION statement. The actual
 * rollback is performed by {@link Database} directly, so {@link #execute} is
 * not used.
 *
 * @see TransactionQuery
 * @see Database
 */
class RollbackTransactionQuery implements TransactionQuery {
    /**
     * Not supported; ROLLBACK TRANSACTION is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("RollbackTransactionQuery should be handled by Database directly");
    }
}