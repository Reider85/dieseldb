package diesel.concurrency;

/**
 * Lock compatibility matrix for DieselDB's granular locking system.
 * <p>
 * Defines which lock modes are compatible when held and requested.
 * A lock request is granted only if it is compatible with all currently held locks on the resource.
 * <p>
 * Compatibility matrix (requested → held ↓):
 * <pre>
 * Requested \ Held | IS | IX | S  | X  | SIX
 * -----------------|----|----|----|----|-----
 * IS              | ✓  | ✓  | ✗  | ✗  | ✓
 * IX              | ✓  | ✓  | ✗  | ✗  | ✗
 * S               | ✓  | ✗  | ✓  | ✗  | ✗
 * X               | ✗  | ✗  | ✗  | ✗  | ✗
 * SIX             | ✓  | ✗  | ✗  | ✗  | ✗
 * </pre>
 */
public final class LockCompatibility {

    // Compatibility matrix: isCompatible[held.ordinal()][requested.ordinal()]
    // True = compatible, False = conflict
    private static final boolean[][] COMPATIBILITY_MATRIX = {
        // Requested: IS, IX, S,  X, SIX
        /* Held IS */   {true, true, false, false, true},
        /* Held IX */   {true, true, false, false, false},
        /* Held S */    {true, false, true, false, false},
        /* Held X */    {false, false, false, false, false},
        /* Held SIX */  {true, false, false, false, false}
    };

    private LockCompatibility() {
        // Utility class - prevent instantiation
    }

    /**
     * Checks if a requested lock mode is compatible with a held lock mode.
     *
     * @param held the lock mode currently held on the resource
     * @param requested the lock mode being requested
     * @return true if the modes are compatible, false if there is a conflict
     */
    public static boolean isCompatible(LockMode held, LockMode requested) {
        return COMPATIBILITY_MATRIX[held.ordinal()][requested.ordinal()];
    }

    /**
     * Checks if a requested lock mode is compatible with multiple held lock modes.
     * The request is compatible only if it is compatible with ALL held locks.
     *
     * @param requested the lock mode being requested
     * @param heldModes the set of lock modes currently held on the resource
     * @return true if the request is compatible with all held locks, false if there is any conflict
     */
    public static boolean isCompatibleWithAll(LockMode requested, Iterable<LockMode> heldModes) {
        for (LockMode held : heldModes) {
            if (!isCompatible(requested, held)) {
                return false;
            }
        }
        return true;
    }
}