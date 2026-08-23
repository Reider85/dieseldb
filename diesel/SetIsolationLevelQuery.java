package diesel;

/**
 * Parsed representation of a SET TRANSACTION ISOLATION LEVEL statement. The
 * default isolation level is changed by {@link Database} directly, so
 * {@link #execute} is not used.
 *
 * @see Query
 * @see Database
 */
class SetIsolationLevelQuery implements Query<String> {
    private final IsolationLevel isolationLevel;

    /**
     * Creates a SET ISOLATION LEVEL query.
     *
     * @param isolationLevel the requested isolation level
     */
    public SetIsolationLevelQuery(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * Returns the requested isolation level.
     *
     * @return the isolation level
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Not supported; SET ISOLATION LEVEL is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("unused")
    public String execute(Table table) {
        throw new UnsupportedOperationException("SetIsolationLevelQuery should be handled by Database directly");
    }
}