package diesel;

/**
 * Three-valued logic helpers for SQL NULL semantics.
 * TRUE / FALSE / UNKNOWN (represented as Boolean.TRUE / Boolean.FALSE / null).
 * A short-circuit is possible only when the accumulated result already
 * determines the whole expression: for OR that is TRUE (TRUE OR x = TRUE),
 * for AND that is FALSE (FALSE AND x = FALSE).
 */
public final class ThreeValuedLogic {

    private ThreeValuedLogic() {
    }

    public static Boolean and(Boolean left, Boolean right) {
        if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
            return Boolean.FALSE;
        }
        if (Boolean.TRUE.equals(left) && Boolean.TRUE.equals(right)) {
            return Boolean.TRUE;
        }
        return null;
    }

    public static Boolean or(Boolean left, Boolean right) {
        if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
            return Boolean.TRUE;
        }
        if (Boolean.FALSE.equals(left) && Boolean.FALSE.equals(right)) {
            return Boolean.FALSE;
        }
        return null;
    }

    public static Boolean not(Boolean value) {
        if (value == null) {
            return null;
        }
        return Boolean.valueOf(!value.booleanValue());
    }

    public static boolean isTrue(Boolean value) {
        return Boolean.TRUE.equals(value);
    }

    public static boolean orIsDetermined(Boolean result) {
        return Boolean.TRUE.equals(result);
    }

    public static boolean andIsDetermined(Boolean result) {
        return Boolean.FALSE.equals(result);
    }
}
