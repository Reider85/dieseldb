package diesel.security;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Privilege cache for an active session (prompt4 #32, R3-008 step 3/4).
 * Stores resolved privileges with versioning for invalidation.
 */
final class PrivilegeCache {
    
    private final long epoch;
    private final boolean hasAdminPrivilege;
    private final Set<String> grantedPrivileges;
    
    /**
     * Creates an empty cache with the given epoch.
     */
    PrivilegeCache(long epoch) {
        this(epoch, false, Collections.emptySet());
    }
    
    /**
     * Creates a cache with resolved privileges.
     */
    PrivilegeCache(long epoch, boolean hasAdminPrivilege, Set<String> grantedPrivileges) {
        this.epoch = epoch;
        this.hasAdminPrivilege = hasAdminPrivilege;
        this.grantedPrivileges = Set.copyOf(grantedPrivileges);
    }
    
    /** Returns the epoch this cache was built with. */
    long getEpoch() {
        return epoch;
    }
    
    /** Returns true if the user has ADMIN privilege (database-wide). */
    boolean hasAdminPrivilege() {
        return hasAdminPrivilege;
    }
    
    /** Returns the set of granted privileges (action|tableName format). */
    Set<String> getGrantedPrivileges() {
        return grantedPrivileges;
    }
    
    /**
     * Returns true if the user has the specified privilege on the specified table.
     * ADMIN privilege grants access to everything. Table names are compared
     * case-insensitively (unquoted SQL identifiers are stored uppercase).
     */
    boolean hasPrivilege(Privilege.Action action, String tableName) {
        if (hasAdminPrivilege) {
            return true;
        }
        String table = normalizeTableName(tableName);
        if (grantedPrivileges.contains(action.name() + "|" + table)) {
            return true;
        }
        return !table.isEmpty() && hasDatabaseWidePrivilege(action);
    }
    
    /**
     * Returns true if the user has any of the specified privileges on the specified table.
     * ADMIN privilege grants access to everything.
     */
    boolean hasAnyPrivilege(Set<Privilege.Action> actions, String tableName) {
        if (hasAdminPrivilege) {
            return true;
        }
        for (Privilege.Action action : actions) {
            if (hasPrivilege(action, tableName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Returns true if the user has the specified privilege database-wide.
     * ADMIN privilege grants access to everything. Database-wide grants are
     * stored with an empty table name ("ACTION|"); the legacy wildcard form
     * ("ACTION|*") is accepted as well.
     */
    boolean hasDatabaseWidePrivilege(Privilege.Action action) {
        if (hasAdminPrivilege) {
            return true;
        }
        return grantedPrivileges.contains(action.name() + "|")
                || grantedPrivileges.contains(action.name() + "|*");
    }
    
    /** Uppercases a table name the same way unquoted SQL identifiers are stored. */
    private static String normalizeTableName(String tableName) {
        return tableName == null ? "" : tableName.toUpperCase(Locale.ROOT);
    }
}