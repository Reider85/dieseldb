package diesel.replication;

import java.util.Locale;
import java.util.Set;

/**
 * Write-rejection gate for a hot-standby database (prompt4 #58, R3-016
 * step 1/5).
 *
 * <p>In hot standby mode the replica serves read-only traffic: SELECT (and
 * other read statements) execute normally, while every data/DDL mutation is
 * rejected before parsing. The gate lives on the public {@code Database}
 * entry points ({@code executeQuery} family), so embedded, server, libpq and
 * prepared-statement paths all hit the same check; the replication apply
 * pipeline bypasses it by design — it writes through the engine's
 * {@code PitrSink} hooks, not through SQL.
 *
 * <p>Classification is the first significant keyword of the statement (leading
 * comments and whitespace are skipped). Anything not explicitly listed as a
 * read is treated as a write — fail-closed, so new write statements cannot
 * slip through unnoticed.
 */
public final class HotStandbyHandler {

    /** First keywords of statements allowed on a hot standby. */
    private static final Set<String> READ_KEYWORDS = Set.of(
            "SELECT",
            "WITH",       // CTE selects (DieselDB has no data-modifying CTE)
            "SHOW",
            "EXPLAIN",
            "DESCRIBE",
            "VALUES",
            "TABLE",      // TABLE t — plain read form
            "SET",        // session-local no-op on a replica
            "BEGIN",
            "START",      // START TRANSACTION
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
            "ANALYZE",    // read-only maintenance
            "CHECK"       // consistency check if ever added
    );

    private HotStandbyHandler() {
        throw new AssertionError("Utility class, do not instantiate");
    }

    /**
     * Returns true when the statement is a write (mutation or DDL) that must
     * be rejected on a hot standby.
     *
     * @param query the SQL text (null-safe: null is treated as a write)
     * @return true when the statement must be rejected
     */
    public static boolean isWrite(String query) {
        String keyword = firstKeyword(query);
        return keyword == null || !READ_KEYWORDS.contains(keyword);
    }

    /**
     * Throws when the statement is not allowed on a hot standby.
     *
     * @param query the SQL text
     * @throws IllegalStateException with a PostgreSQL-style read-only error
     *         message when the statement is a write
     */
    public static void checkAllowed(String query) {
        if (isWrite(query)) {
            String keyword = firstKeyword(query);
            String shown = keyword == null ? "statement" : keyword;
            throw new IllegalStateException(
                    "cannot execute " + shown + " in hot standby mode: the replica is read-only");
        }
    }

    /**
     * Extracts the leading SQL keyword in uppercase, skipping whitespace and
     * {@code --}/{@code /*} comments.
     *
     * @param query the SQL text
     * @return the uppercased first word, or null for blank/comment-only input
     */
    static String firstKeyword(String query) {
        if (query == null) {
            return null;
        }
        int i = 0;
        int n = query.length();
        while (i < n) {
            char c = query.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && i + 1 < n && query.charAt(i + 1) == '-') {
                int nl = query.indexOf('\n', i + 2);
                if (nl < 0) {
                    return null; // comment runs to end of input
                }
                i = nl + 1;
            } else if (c == '/' && i + 1 < n && query.charAt(i + 1) == '*') {
                int end = query.indexOf("*/", i + 2);
                if (end < 0) {
                    return null;
                }
                i = end + 2;
            } else {
                break;
            }
        }
        if (i >= n) {
            return null;
        }
        int start = i;
        while (i < n && (Character.isLetterOrDigit(query.charAt(i))
                || query.charAt(i) == '_')) {
            i++;
        }
        if (start == i) {
            return null;
        }
        return query.substring(start, i).toUpperCase(Locale.ROOT);
    }
}
