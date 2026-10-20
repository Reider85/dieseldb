package diesel.security;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CRUD over the RBAC system tables (prompt4 #30, R3-008 step 1/4).
 *
 * <p>Users and roles are stored through a {@link UserStoreBackend} backed by
 * the engine's {@code DIESEL_USERS}/{@code DIESEL_ROLES}/
 * {@code DIESEL_USER_ROLES}/{@code DIESEL_PRIVILEGES} tables. All column
 * values are Strings (UUID/timestamp encoded) so every storage format
 * round-trips them identically.
 */
public final class UserStore {

    /** Account bootstrapped on the first server start (prompt4 #31). */
    public static final String ADMIN_NAME = "admin";

    /** Bootstrap / reset password for {@link #ADMIN_NAME}; must be changed at first login. */
    public static final String DEFAULT_ADMIN_PASSWORD = "admin";

    private final UserStoreBackend backend;
    private final PasswordHasher hasher;

    /**
     * Monotonically bumped on every grant/role-membership mutation
     * (prompt4 #32). Privilege caches are keyed off this counter so an
     * active session sees GRANT/REVOKE effects immediately.
     */
    private final AtomicLong changeCounter = new AtomicLong(0);

    public UserStore(UserStoreBackend backend, PasswordHasher hasher) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.hasher = Objects.requireNonNull(hasher, "hasher");
    }

    /** Returns the current grant/role mutation counter. */
    public long getChangeCounter() {
        return changeCounter.get();
    }

    private void bumpChangeCounter() {
        changeCounter.incrementAndGet();
    }

    /** Returns the hasher in use (step 31 verifies passwords through it). */
    public PasswordHasher getPasswordHasher() {
        return hasher;
    }

    /**
     * Creates a user with a salted password hash.
     *
     * @throws UserExistsException when the name is already taken
     * @throws IllegalArgumentException when name or password is blank
     */
    public User createUser(String name, String password) {
        requireName(name, "User");
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Password must not be null or empty");
        }
        if (findUser(name).isPresent()) {
            throw new UserExistsException("User '" + name + "' already exists");
        }
        User user = new User(UUID.randomUUID(), name, hasher.hash(password), LocalDateTime.now());
        backend.appendRow(UserStoreBackend.USERS, userRow(user));
        return user;
    }

    /**
     * Creates a role.
     *
     * @throws RoleExistsException when the name is already taken
     * @throws IllegalArgumentException when the name is blank
     */
    public Role createRole(String name) {
        requireName(name, "Role");
        if (findRole(name).isPresent()) {
            throw new RoleExistsException("Role '" + name + "' already exists");
        }
        Role role = new Role(UUID.randomUUID(), name, LocalDateTime.now());
        backend.appendRow(UserStoreBackend.ROLES, roleRow(role));
        return role;
    }

    /**
     * Grants an existing role to an existing user. Granting the same role
     * twice is a no-op.
     *
     * @throws UserNotFoundException when the user does not exist
     * @throws RoleNotFoundException when the role does not exist
     */
    public void grantRoleToUser(String userName, String roleName) {
        User user = findUser(userName)
                .orElseThrow(() -> new UserNotFoundException("User '" + userName + "' does not exist"));
        Role role = findRole(roleName)
                .orElseThrow(() -> new RoleNotFoundException("Role '" + roleName + "' does not exist"));
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.USER_ROLES)) {
            if (user.getId().toString().equals(row.get("USER_ID"))
                    && role.getId().toString().equals(row.get("ROLE_ID"))) {
                return;
            }
        }
        Map<String, Object> grant = new LinkedHashMap<>();
        grant.put("ID", UUID.randomUUID().toString());
        grant.put("USER_ID", user.getId().toString());
        grant.put("ROLE_ID", role.getId().toString());
        backend.appendRow(UserStoreBackend.USER_ROLES, grant);
        bumpChangeCounter();
    }

    /**
     * Revokes a previously granted role from a user (prompt4 #32). Revoking
     * a role the user does not hold is a no-op.
     *
     * @throws UserNotFoundException when the user does not exist
     * @throws RoleNotFoundException when the role does not exist
     */
    public void revokeRoleFromUser(String userName, String roleName) {
        User user = findUser(userName)
                .orElseThrow(() -> new UserNotFoundException("User '" + userName + "' does not exist"));
        Role role = findRole(roleName)
                .orElseThrow(() -> new RoleNotFoundException("Role '" + roleName + "' does not exist"));
        List<Map<String, Object>> memberships = backend.readAll(UserStoreBackend.USER_ROLES);
        for (Map<String, Object> row : memberships) {
            if (user.getId().toString().equals(row.get("USER_ID"))
                    && role.getId().toString().equals(row.get("ROLE_ID"))) {
                backend.deleteRow(UserStoreBackend.USER_ROLES, row.get("ID").toString());
                bumpChangeCounter();
                return;
            }
        }
    }

    /**
     * Replaces the user's password hash (prompt4 #31). The freshly hashed
     * value carries no must-change flag, so a successful call clears a
     * pending forced change.
     *
     * @param userName  the account to update
     * @param newPassword the new plaintext password
     * @throws UserNotFoundException when the user does not exist
     * @throws IllegalArgumentException when the password is null or empty
     */
    public void updatePassword(String userName, String newPassword) {
        requireName(userName, "User");
        if (newPassword == null || newPassword.isEmpty()) {
            throw new IllegalArgumentException("Password must not be null or empty");
        }
        User user = findUser(userName)
                .orElseThrow(() -> new UserNotFoundException("User '" + userName + "' does not exist"));
        // Hash the new password and clear any must-change flag
        String newHash = PasswordHasher.withoutMustChangeFlag(hasher.hash(newPassword));
        User updated = new User(user.getId(), user.getName(), newHash, user.getCreatedAt());
        backend.updateRow(UserStoreBackend.USERS, userRow(updated));
    }

    /**
     * Bootstraps the {@link #ADMIN_NAME}/{@link #DEFAULT_ADMIN_PASSWORD}
     * account on the very first run (prompt4 #31, no users exist yet).
     * The account is flagged so the first login must change the password.
     * Also ensures admin role and privileges (prompt4 #32).
     *
     * @return the admin user; when bootstrap is skipped because users are
     *         already present, the admin itself (or the first user when no
     *         account is named {@code admin})
     */
    public synchronized User ensureDefaultAdmin() {
        ensureAdminPrivileges();
        Optional<User> admin = findUser(ADMIN_NAME);
        if (admin.isPresent()) {
            return admin.get();
        }
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.USERS)) {
            return userFromRow(row);
        }
        return createAdminWithFlag();
    }

    /**
     * Restores the admin account to the bootstrap state: password
     * {@link #DEFAULT_ADMIN_PASSWORD} plus the forced-change flag
     * (prompt4 #31 {@code --reset-admin}). Recreates the account when it
     * was removed. Also ensures admin role and privileges (prompt4 #32).
     *
     * @return the admin user
     */
    public synchronized User resetAdminPassword() {
        ensureAdminPrivileges();
        String hash = PasswordHasher.withMustChangeFlag(hasher.hash(DEFAULT_ADMIN_PASSWORD));
        User existing = findUser(ADMIN_NAME).orElse(null);
        if (existing != null) {
            User reset = new User(existing.getId(), existing.getName(), hash, existing.getCreatedAt());
            backend.updateRow(UserStoreBackend.USERS, userRow(reset));
            return reset;
        }
        User created = new User(UUID.randomUUID(), ADMIN_NAME, hash, LocalDateTime.now());
        backend.appendRow(UserStoreBackend.USERS, userRow(created));
        // ensureAdminPrivileges() ran before the account existed, so the
        // admin-role membership could not be granted then - grant it now.
        grantRoleToUser(ADMIN_NAME, ADMIN_NAME);
        return created;
    }

    private User createAdminWithFlag() {
        String hash = PasswordHasher.withMustChangeFlag(hasher.hash(DEFAULT_ADMIN_PASSWORD));
        User admin = new User(UUID.randomUUID(), ADMIN_NAME, hash, LocalDateTime.now());
        backend.appendRow(UserStoreBackend.USERS, userRow(admin));
        // Assign admin role to the admin user (the role setup ran while no
        // admin user existed yet, so it could not grant membership itself).
        grantRoleToUser(ADMIN_NAME, ADMIN_NAME);
        return admin;
    }

    /** Returns the user with the given name, when present. */
    public Optional<User> findUser(String name) {
        if (name == null || name.isEmpty()) {
            return Optional.empty();
        }
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.USERS)) {
            if (name.equals(row.get("NAME"))) {
                return Optional.of(userFromRow(row));
            }
        }
        return Optional.empty();
    }

    /** True when a user with the given name exists. */
    public boolean userExists(String name) {
        return findUser(name).isPresent();
    }

    /** True when a role with the given name exists. */
    public boolean roleExists(String name) {
        return findRole(name).isPresent();
    }

    /** Returns the role with the given name, when present. */
    public Optional<Role> findRole(String name) {
        if (name == null || name.isEmpty()) {
            return Optional.empty();
        }
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.ROLES)) {
            if (name.equals(row.get("NAME"))) {
                return Optional.of(roleFromRow(row));
            }
        }
        return Optional.empty();
    }

    /** Returns every role granted to the user; empty for unknown users. */
    public List<Role> rolesOfUser(String userName) {
        Optional<User> user = findUser(userName);
        if (user.isEmpty()) {
            return List.of();
        }
        String userId = user.get().getId().toString();
        List<Role> roles = new ArrayList<>();
        for (Map<String, Object> grant : backend.readAll(UserStoreBackend.USER_ROLES)) {
            if (!userId.equals(grant.get("USER_ID"))) {
                continue;
            }
            String roleId = String.valueOf(grant.get("ROLE_ID"));
            for (Map<String, Object> roleRow : backend.readAll(UserStoreBackend.ROLES)) {
                if (roleId.equals(roleRow.get("ID"))) {
                    roles.add(roleFromRow(roleRow));
                    break;
                }
            }
        }
        return roles;
    }

    /**
     * Grants a privilege to a role. Granting the same privilege
     * twice is a no-op.
     *
     * @throws RoleNotFoundException when the role does not exist
     * @throws IllegalArgumentException when action or table name is null/empty
     */
    public void grantPrivilege(String roleName, Privilege.Action action, String tableName) {
        requireName(roleName, "Role");
        if (action == null) {
            throw new IllegalArgumentException("Action must not be null");
        }
        if (tableName == null) {
            tableName = "";
        }
        Role role = findRole(roleName)
                .orElseThrow(() -> new RoleNotFoundException("Role '" + roleName + "' does not exist"));
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.PRIVILEGES)) {
            if (role.getId().toString().equals(row.get("ROLE_ID"))
                    && action.name().equals(row.get("ACTION"))
                    && tableName.equals(row.get("TABLE_NAME"))) {
                return;
            }
        }
        Map<String, Object> grant = new LinkedHashMap<>();
        grant.put("ID", UUID.randomUUID().toString());
        grant.put("ROLE_ID", role.getId().toString());
        grant.put("ACTION", action.name());
        grant.put("TABLE_NAME", tableName);
        backend.appendRow(UserStoreBackend.PRIVILEGES, grant);
        bumpChangeCounter();
    }

    /**
     * Revokes a privilege from a role. Revoking a non-existent privilege
     * is a no-op.
     *
     * @throws RoleNotFoundException when the role does not exist
     * @throws IllegalArgumentException when action or table name is null/empty
     */
    public void revokePrivilege(String roleName, Privilege.Action action, String tableName) {
        requireName(roleName, "Role");
        if (action == null) {
            throw new IllegalArgumentException("Action must not be null");
        }
        if (tableName == null) {
            tableName = "";
        }
        Role role = findRole(roleName)
                .orElseThrow(() -> new RoleNotFoundException("Role '" + roleName + "' does not exist"));
        List<Map<String, Object>> privileges = backend.readAll(UserStoreBackend.PRIVILEGES);
        for (int i = 0; i < privileges.size(); i++) {
            Map<String, Object> row = privileges.get(i);
            if (role.getId().toString().equals(row.get("ROLE_ID"))
                    && action.name().equals(row.get("ACTION"))
                    && tableName.equals(row.get("TABLE_NAME"))) {
                backend.deleteRow(UserStoreBackend.PRIVILEGES, row.get("ID").toString());
                bumpChangeCounter();
                return;
            }
        }
    }

    /**
     * Returns every privilege granted to the role; empty for unknown roles.
     */
    public List<Privilege> privilegesOfRole(String roleName) {
        Optional<Role> role = findRole(roleName);
        if (role.isEmpty()) {
            return List.of();
        }
        String roleId = role.get().getId().toString();
        List<Privilege> privileges = new ArrayList<>();
        for (Map<String, Object> row : backend.readAll(UserStoreBackend.PRIVILEGES)) {
            if (!roleId.equals(row.get("ROLE_ID"))) {
                continue;
            }
            Privilege.Action action = Privilege.Action.valueOf(String.valueOf(row.get("ACTION")));
            String tableName = String.valueOf(row.get("TABLE_NAME"));
            privileges.add(new Privilege(UUID.randomUUID(), roleName, action, tableName));
        }
        return privileges;
    }

    /**
     * Ensures the admin role exists and has ADMIN privilege DB-wide,
     * and the admin user has the admin role (prompt4 #32). Idempotent.
     */
    public synchronized void ensureAdminPrivileges() {
        // Ensure admin role exists
        Optional<Role> adminRole = findRole(ADMIN_NAME);
        if (adminRole.isEmpty()) {
            createRole(ADMIN_NAME);
        }
        
        // Grant ADMIN privilege to admin role (DB-wide)
        grantPrivilege(ADMIN_NAME, Privilege.Action.ADMIN, "");
        
        // Ensure admin user has admin role
        Optional<User> adminUser = findUser(ADMIN_NAME);
        if (adminUser.isPresent()) {
            grantRoleToUser(ADMIN_NAME, ADMIN_NAME);
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Row encoding — all values are Strings for storage-format safety   */
    /* ------------------------------------------------------------------ */

    static Map<String, Object> userRow(User user) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", user.getId().toString());
        row.put("NAME", user.getName());
        row.put("PASSWORD_HASH", user.getPasswordHash());
        row.put("CREATED_AT", user.getCreatedAt().toString());
        return row;
    }

    static Map<String, Object> roleRow(Role role) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", role.getId().toString());
        row.put("NAME", role.getName());
        row.put("CREATED_AT", role.getCreatedAt().toString());
        return row;
    }

    static User userFromRow(Map<String, Object> row) {
        return new User(
                UUID.fromString(String.valueOf(row.get("ID"))),
                String.valueOf(row.get("NAME")),
                String.valueOf(row.get("PASSWORD_HASH")),
                LocalDateTime.parse(String.valueOf(row.get("CREATED_AT"))));
    }

    static Role roleFromRow(Map<String, Object> row) {
        return new Role(
                UUID.fromString(String.valueOf(row.get("ID"))),
                String.valueOf(row.get("NAME")),
                LocalDateTime.parse(String.valueOf(row.get("CREATED_AT"))));
    }

    private static void requireName(String name, String kind) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(kind + " name must not be blank");
        }
    }
}
