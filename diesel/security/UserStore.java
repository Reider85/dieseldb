package diesel.security;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

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

    private final UserStoreBackend backend;
    private final PasswordHasher hasher;

    public UserStore(UserStoreBackend backend, PasswordHasher hasher) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.hasher = Objects.requireNonNull(hasher, "hasher");
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
