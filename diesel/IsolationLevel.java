package diesel;

/**
 * The isolation level of a transaction, in increasing order of strictness:
 * <ul>
 *   <li>{@link #READ_UNCOMMITTED} - dirty reads are allowed; a transaction can
 *       see another transaction's uncommitted modifications.</li>
 *   <li>{@link #READ_COMMITTED} - only committed data is visible (currently
 *       treated the same as {@link #REPEATABLE_READ}).</li>
 *   <li>{@link #REPEATABLE_READ} - the BEGIN-time snapshot is used for reads.</li>
 *   <li>{@link #SERIALIZABLE} - strongest isolation (currently treated the
 *       same as {@link #REPEATABLE_READ}).</li>
 * </ul>
 */
public enum IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}
