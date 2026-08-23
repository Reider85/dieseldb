package diesel;

/**
 * Parsed representation of a SET AUTOCOMMIT ON/OFF statement. The auto-commit
 * flag is changed by {@link Database} directly, so {@link #execute} is not
 * used.
 *
 * @see Query
 * @see Database
 */
class SetAutoCommitQuery implements Query<String> {
    private final boolean autoCommit;

    /**
     * Creates a SET AUTOCOMMIT query.
     *
     * @param autoCommit the requested auto-commit flag
     */
    public SetAutoCommitQuery(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Returns the requested auto-commit flag.
     *
     * @return true for ON, false for OFF
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Not supported; SET AUTOCOMMIT is handled by {@link Database}.
     *
     * @param table not used
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    @SuppressWarnings("unused")
    public String execute(Table table) {
        throw new UnsupportedOperationException("SetAutoCommitQuery should be handled by Database directly");
    }
}
