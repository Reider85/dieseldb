package diesel;

import java.util.Objects;

/**
 * RELEASE SAVEPOINT name statement (prompt4.md #26).
 * 
 * <p>Releases the specified savepoint and all savepoints created after it.
 * The released savepoints can no longer be used with ROLLBACK TO.
 * 
 * @see TransactionQuery
 * @see Database#executeReleaseSavepoint
 */
class ReleaseSavepointQuery implements TransactionQuery {

    private final String savepointName;

    /**
     * Creates a release savepoint query.
     *
     * @param savepointName the savepoint name (case-sensitive, will be normalized to upper)
     */
    public ReleaseSavepointQuery(String savepointName) {
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
                "ReleaseSavepointQuery should be handled by Database directly, not by Table");
    }
}