package diesel;

/**
 * Three-valued logic for SQL NULL semantics.
 * Each constant represents a truth value: TRUE, FALSE, or UNKNOWN.
 * A short-circuit is possible only when the accumulated result already
 * determines the whole expression: for OR that is TRUE (TRUE OR x = TRUE),
 * for AND that is FALSE (FALSE AND x = FALSE).
 */
public enum ThreeValuedLogic {

    TRUE,
    FALSE,
    UNKNOWN;

    public ThreeValuedLogic and(ThreeValuedLogic right) {
        if (this == FALSE || right == FALSE) {
            return FALSE;
        }
        if (this == TRUE && right == TRUE) {
            return TRUE;
        }
        return UNKNOWN;
    }

    public ThreeValuedLogic or(ThreeValuedLogic right) {
        if (this == TRUE || right == TRUE) {
            return TRUE;
        }
        if (this == FALSE && right == FALSE) {
            return FALSE;
        }
        return UNKNOWN;
    }

    public ThreeValuedLogic not() {
        if (this == TRUE) {
            return FALSE;
        }
        if (this == FALSE) {
            return TRUE;
        }
        return UNKNOWN;
    }

    public boolean isTrue() {
        return this == TRUE;
    }

    public boolean orIsDetermined() {
        return this == TRUE;
    }

    public boolean andIsDetermined() {
        return this == FALSE;
    }
}
