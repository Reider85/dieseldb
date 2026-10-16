package diesel;

import java.util.Objects;

/**
 * ROLLBACK TO SAVEPOINT name statement (prompt4.md #26).
 * 
 * <p>Rolls back the transaction to the specified savepoint: undo records created
 * after the savepoint are applied in reverse order, restoring the database state
 * to the savepoint. The savepoint itself remains valid and can be rolled back to again.
 * 
 * @see TransactionQuery
 * @see Database#executeRollbackToSavepoint
 */
class RollbackToSavepointQuery implements TransactionQuery {

    private final String savepointName;

    /**
     * Creates a rollback to savepoint query.
     *
     * @param savepointName the savepoint name (case-sensitive, will be normalized to upper)
     */
    public RollbackToSavepointQuery(String savepointName) {
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
                "RollbackToSavepointQuery should be handled by Database directly, not by Table");
    }
}