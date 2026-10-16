package diesel;

import java.util.Objects;

/**
 * SAVEPOINT name statement (prompt4.md #26).
 * 
 * <p>A savepoint creates a marker in the transaction's undo log at the current
 * position, allowing partial rollback to this point via ROLLBACK TO.
 * 
 * @see TransactionQuery
 * @see Database#executeSavepoint
 */
class SavepointQuery implements TransactionQuery {

    private final String savepointName;

    /**
     * Creates a savepoint query.
     *
     * @param savepointName the savepoint name (case-sensitive, will be normalized to upper)
     */
    public SavepointQuery(String savepointName) {
        this.savepointName = Objects.requireNonNull(savepointName, "savepointName");
    }

    /**
     * Returns the savepoint name.
     */
    public String getSavepointName() {
        return savepointName;
    }

    @Override
    @SuppressWarnings("unused")
    public String execute(Table table) {
        throw new UnsupportedOperationException(
                "SavepointQuery should be handled by Database directly, not by Table");
    }
}