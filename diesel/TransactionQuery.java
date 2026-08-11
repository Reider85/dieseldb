package diesel;

/**
 * Marker contract for transaction statements (BEGIN/COMMIT/ROLLBACK
 * TRANSACTION). These queries are handled by {@link Database} directly rather
 * than executed against a table.
 *
 * @see BeginTransactionQuery
 * @see CommitTransactionQuery
 * @see RollbackTransactionQuery
 */
interface TransactionQuery extends Query<String> {

}
