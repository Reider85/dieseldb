package diesel;

/**
 * Prompt 30 (java:S5869, S6353): simple full-match regexes replaced by
 * char-loop string operations. Every helper is an exact ASCII equivalent of
 * the regex it replaces (Java {@code \d} matches [0-9] and {@code \s} matches
 * [ \t\n\x0B\f\r] unless the UNICODE_CHARACTER_CLASS flag is set), so there is
 * no semantic drift - only the allocation-free hot path changes.
 */
public final class CharOps {

    private CharOps() {
    }

    /**
     * Exact equivalent of {@code s.matches("[a-zA-Z_]\\w*")}.
     */
    public static boolean isAsciiIdentifier(String s) {
        if (s.isEmpty()) {
            return false;
        }
        if (!(isAsciiLetter(s.charAt(0)) || s.charAt(0) == '_')) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!(isAsciiLetter(c) || isDigit(c) || c == '_')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Exact equivalent of {@code s.matches(".*\\s+.*")} (contains a Java
     * {@code \s} whitespace character).
     */
    public static boolean containsWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (isAsciiWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Exact equivalent of {@code s.matches("\\d{4}-\\d{2}-\\d{2}")}.
     */
    public static boolean isLocalDateLiteral(String s) {
        if (s.length() != 10 || s.charAt(4) != '-' || s.charAt(7) != '-') {
            return false;
        }
        return isDigit(s, 0, 4) && isDigit(s, 5, 2) && isDigit(s, 8, 2);
    }

    /**
     * Exact equivalent of {@code s.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")}.
     */
    public static boolean isLocalDateTimeLiteral(String s) {
        if (s.length() != 19 || !isLocalDateLiteral(s.substring(0, 10))
                || s.charAt(10) != ' ' || s.charAt(13) != ':' || s.charAt(16) != ':') {
            return false;
        }
        return isDigit(s, 11, 2) && isDigit(s, 14, 2) && isDigit(s, 17, 2);
    }

    /**
     * Exact equivalent of {@code s.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}")}.
     */
    public static boolean isLocalDateTimeMillisLiteral(String s) {
        if (s.length() != 23 || !isLocalDateTimeLiteral(s.substring(0, 19))
                || s.charAt(19) != '.') {
            return false;
        }
        return isDigit(s, 20, 3);
    }

    private static boolean isDigit(String s, int offset, int count) {
        for (int i = 0; i < count; i++) {
            if (!isDigit(s.charAt(offset + i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isAsciiWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == 0x0B || c == '\f' || c == '\r';
    }
}