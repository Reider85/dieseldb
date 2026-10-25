package diesel.schema;

import diesel.SyntaxErrorException;

/**
 * Lock mode for ALTER TABLE ... LOCK=NONE|SHARED|EXCLUSIVE (prompt4 #39, R3-010 step 3).
 *
 * <p>Controls concurrent access during online schema changes:
 * <ul>
 *   <li>NONE - no extra blocking beyond the operation's intrinsic locking (default).</li>
 *   <li>SHARED - writers are blocked during the ALTER; readers proceed normally.</li>
 *   <li>EXCLUSIVE - both readers and writers are blocked during the ALTER.</li>
 * </ul>
 *
 * <p>For INPLACE operations (metadata-only), the window is typically microseconds.
 * Gates are advisory and use the same timeout pattern as {@code DualWriteCoordinator}
 * (alter.copy.gate.timeout.ms default 10s).
 */
public enum AlterLockMode {
    /** No blocking beyond the operation's intrinsic locking (default). */
    NONE,

    /** Writers blocked during the ALTER; readers proceed normally. */
    SHARED,

    /** Both readers and writers blocked during the ALTER. */
    EXCLUSIVE;

    /**
     * Parses a lock mode token from SQL (case-insensitive).
     *
     * @param token The lock mode token (e.g. "SHARED", "none", "ExClUsIvE")
     * @return The lock mode enum value
     * @throws diesel.SyntaxErrorException if the token is not recognized
     */
    public static AlterLockMode fromToken(String token) {
        if (token == null) {
            return NONE;
        }
        try {
            return AlterLockMode.valueOf(token.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new diesel.SyntaxErrorException(
                    "Unsupported LOCK mode: " + token + " (supported: NONE, SHARED, EXCLUSIVE)");
        }
    }
}