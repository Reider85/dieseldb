package diesel;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes an ALTER USER statement: replaces the account's password hash
 * (prompt4 #31, R3-008 step 2/4). The freshly hashed value carries no
 * must-change flag, so a successful change clears a pending forced change.
 *
 * @see Query
 * @see diesel.security.UserStore
 */
class AlterUserQuery implements Query<String> {

    /** Kept in sync with QueryParser's ALTER_USER_PATTERN for server-side gating. */
    private static final Pattern ALTER_USER_PATTERN = Pattern.compile(
            "(?i)^ALTER\\s+USER\\s+(\\S+)\\s+PASSWORD\\s+'([^']*)'\\s*;?\\s*$");

    private final String userName;
    private final String password;

    /**
     * @param userName the account name, case-preserved
     * @param password the new plaintext password (hashed before storage)
     */
    public AlterUserQuery(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    /**
     * Returns the new plaintext password. Only for parser/dispatch plumbing —
     * never log or persist it.
     */
    public String getPassword() {
        return password;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeAlterUser(this);
    }

    /** True when the SQL text is an ALTER USER statement (prompt4 #31 gating). */
    static boolean isAlterUser(String sql) {
        return sql != null && targetUserName(sql) != null;
    }

    /**
     * Returns the target account of an ALTER USER statement, or null when the
     * text is not a well-formed ALTER USER.
     */
    static String targetUserName(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher m = ALTER_USER_PATTERN.matcher(sql.trim());
        return m.matches() ? m.group(1) : null;
    }
}
