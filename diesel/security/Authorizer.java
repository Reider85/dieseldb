package diesel.security;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Authorizer that checks privileges before executing queries (prompt4 #32, R3-008 step 3/4).
 * Maintains per-session privilege caches and handles invalidation.
 */
public final class Authorizer {
    
    private final UserStore userStore;
    private final Map<UUID, PrivilegeCache> sessionCaches = new ConcurrentHashMap<>();
    
    /**
     * Creates an authorizer that uses the given user store.
     */
    public Authorizer(UserStore userStore) {
        this.userStore = userStore;
    }
    
    /**
     * Checks if the session has the specified privilege on the specified table.
     * Also enforces must-change password policy and handles ADMIN privilege.
     *
     * @throws AccessDeniedException if the access is denied
     */
    public void check(Session session, Privilege.Action action, String tableName) {
        // Check must-change password policy
        if (session.isMustChangePassword()) {
            throw new AccessDeniedException(
                "Password change required: user '" + session.getUserName() + "' must change password first");
        }
        
        PrivilegeCache cache = getCachedPrivileges(session);
        
        // ADMIN privilege grants access to everything
        if (cache.hasAdminPrivilege()) {
            return;
        }
        
        if (!cache.hasPrivilege(action, tableName)) {
            throw new AccessDeniedException(
                "Access denied: user '" + session.getUserName() + 
                "' has no " + action + " privilege on " + 
                (tableName.isEmpty() ? "database" : tableName));
        }
    }
    
    /**
     * Checks if the session has any of the specified privileges on the specified table.
     * Also enforces must-change password policy and handles ADMIN privilege.
     *
     * @throws AccessDeniedException if all privileges are denied
     */
    public void checkAny(Session session, Set<Privilege.Action> actions, String tableName) {
        // Check must-change password policy
        if (session.isMustChangePassword()) {
            throw new AccessDeniedException(
                "Password change required: user '" + session.getUserName() + "' must change password first");
        }
        
        PrivilegeCache cache = getCachedPrivileges(session);
        
        // ADMIN privilege grants access to everything
        if (cache.hasAdminPrivilege()) {
            return;
        }
        
        if (!cache.hasAnyPrivilege(actions, tableName)) {
            throw new AccessDeniedException(
                "Access denied: user '" + session.getUserName() + 
                "' has none of " + actions + " privilege on " + 
                (tableName.isEmpty() ? "database" : tableName));
        }
    }
    
    /**
     * Invalidates all privilege caches. Call this after any GRANT or REVOKE
     * operation. The caches are additionally keyed off the UserStore's
     * change counter, so direct store mutations are seen even without this.
     */
    public void invalidateAll() {
        sessionCaches.clear();
    }
    
    /**
     * Gets or builds the privilege cache for the given session. A cache is
     * rebuilt whenever the UserStore's grant/role change counter moves past
     * the version the cache was built with (prompt4 #32: GRANT/REVOKE must be
     * immediately visible on active sessions).
     */
    private PrivilegeCache getCachedPrivileges(Session session) {
        long version = userStore.getChangeCounter();
        UUID tokenId = session.getToken().getId();
        PrivilegeCache cached = sessionCaches.get(tokenId);
        if (cached != null && cached.getEpoch() == version) {
            return cached;
        }
        
        // Build fresh cache
        boolean hasAdmin = false;
        Set<String> privileges = new HashSet<>();
        List<Role> roles = userStore.rolesOfUser(session.getUserName());
        
        for (Role role : roles) {
            List<Privilege> rolePrivileges = userStore.privilegesOfRole(role.getName());
            for (Privilege priv : rolePrivileges) {
                if (Privilege.Action.ADMIN.equals(priv.getAction())) {
                    hasAdmin = true;
                }
                // Normalize like PrivilegeCache does for lookups: unquoted SQL
                // identifiers are stored uppercase, so grants must match that.
                String table = priv.getTableName() == null ? "" : priv.getTableName().toUpperCase(Locale.ROOT);
                privileges.add(priv.getAction().name() + "|" + table);
            }
        }
        
        PrivilegeCache fresh = new PrivilegeCache(version, hasAdmin, privileges);
        sessionCaches.put(tokenId, fresh);
        return fresh;
    }
}