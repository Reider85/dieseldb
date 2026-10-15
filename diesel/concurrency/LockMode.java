package diesel.concurrency;

/**
 * Lock modes for DieselDB's granular locking system.
 * <p>
 * Modes follow standard database lock compatibility:
 * <ul>
 *   <li>IS (Intent Shared): Compatible with IS, S, IX</li>
 *   <li>IX (Intent Exclusive): Compatible with IS, IX</li>
 *   <li>S (Shared): Compatible with IS, S</li>
 *   <li>X (Exclusive): Compatible with nothing</li>
 *   <li>SIX (Shared Intent Exclusive): Compatible with IS only</li>
 * </ul>
 */
public enum LockMode {
    IS,
    IX,
    S,
    X,
    SIX;

    // Bit masks for compatibility matrix optimization
    private static final int IS_BIT = 1;
    private static final int IX_BIT = 2;
    private static final int S_BIT = 4;
    private static final int X_BIT = 8;
    private static final int SIX_BIT = 16;

    /**
     * Returns the bit mask for this lock mode.
     */
    public int getBit() {
        switch (this) {
            case IS: return IS_BIT;
            case IX: return IX_BIT;
            case S: return S_BIT;
            case X: return X_BIT;
            case SIX: return SIX_BIT;
            default: throw new AssertionError("Unknown lock mode: " + this);
        }
    }
}