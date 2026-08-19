package diesel;

/**
 * Common contract of every parsed SQL statement: an executable object that
 * performs its operation against a {@link Table}.
 *
 * @param <T> the result type (a row list for SELECT, null for DML/DDL, or a
 *            String for transaction statements)
 */
interface Query<T> {
    /**
     * Executes the query against the given table.
     *
     * @param table the table the query operates on
     * @return the query result
     */
    T execute(Table table);
}
