package diesel.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt 4 #30 acceptance: create user stores a salted hash (never
 * plaintext), duplicates raise UserExistsException, roles and grants work.
 * Uses an in-memory {@link UserStoreBackend} so the store logic is tested
 * without the engine. Password updates (#31) exercise the backend
 * {@code updateRow} path.
 */
@Tag("smoke")
class UserStoreTest {

    private InMemoryUserStoreBackend backend;
    private UserStore store;

    @BeforeEach
    void setUp() {
        backend = new InMemoryUserStoreBackend();
        // Low iteration count keeps the unit suite fast; the 100k acceptance
        // timing lives in PasswordHasherTest.
        store = new UserStore(backend, new PasswordHasher(1_000));
    }

    @Test
    void createUserStoresHashNotPlaintext() {
        store.createUser("alice", "s3cret");
        Map<String, Object> row = backend.readAll(UserStoreBackend.USERS).get(0);
        String stored = String.valueOf(row.get("PASSWORD_HASH"));
        assertNotEquals("s3cret", stored);
        assertTrue(PasswordHasher.isEncodedHash(stored));
        assertTrue(store.getPasswordHasher().verify("s3cret", stored));
        assertEquals("alice", row.get("NAME"));
    }

    @Test
    void duplicateCreateUserThrowsUserExists() {
        store.createUser("alice", "s3cret");
        assertThrows(UserExistsException.class, () -> store.createUser("alice", "other"));
        assertTrue(store.userExists("alice"));
        assertFalse(store.userExists("bob"));
    }

    @Test
    void createUserRejectsBlankNameAndPassword() {
        assertThrows(IllegalArgumentException.class, () -> store.createUser("", "pw"));
        assertThrows(IllegalArgumentException.class, () -> store.createUser("  ", "pw"));
        assertThrows(IllegalArgumentException.class, () -> store.createUser("alice", ""));
        assertThrows(IllegalArgumentException.class, () -> store.createUser("alice", null));
    }

    @Test
    void userNameCaseIsPreserved() {
        store.createUser("Alice", "pw");
        assertTrue(store.userExists("Alice"));
        assertFalse(store.userExists("alice"), "user names are case-sensitive");
    }

    @Test
    void createRoleAndDuplicate() {
        store.createRole("analyst");
        assertTrue(store.roleExists("analyst"));
        assertThrows(RoleExistsException.class, () -> store.createRole("analyst"));
        assertFalse(store.roleExists("dba"));
    }

    @Test
    void grantRoleToUser() {
        store.createUser("alice", "pw");
        store.createRole("analyst");
        store.grantRoleToUser("alice", "analyst");
        List<Role> roles = store.rolesOfUser("alice");
        assertEquals(1, roles.size());
        assertEquals("analyst", roles.get(0).getName());
        assertEquals(List.of(), store.rolesOfUser("bob"), "unknown user has no roles");
    }

    @Test
    void grantSameRoleTwiceIsIdempotent() {
        store.createUser("alice", "pw");
        store.createRole("analyst");
        store.grantRoleToUser("alice", "analyst");
        store.grantRoleToUser("alice", "analyst");
        assertEquals(1, store.rolesOfUser("alice").size());
    }

    @Test
    void grantUnknownUserOrRoleThrows() {
        store.createUser("alice", "pw");
        store.createRole("analyst");
        assertThrows(UserNotFoundException.class, () -> store.grantRoleToUser("ghost", "analyst"));
        assertThrows(RoleNotFoundException.class, () -> store.grantRoleToUser("alice", "ghost"));
    }

    @Test
    void findUserReturnsAccountWithHash() {
        store.createUser("alice", "pw");
        User user = store.findUser("alice").orElseThrow();
        assertEquals("alice", user.getName());
        assertTrue(store.getPasswordHasher().verify("pw", user.getPasswordHash()));
        assertTrue(store.findUser("ghost").isEmpty());
    }

    @Test
    void updatePasswordReplacesHashInPlace() {
        store.createUser("alice", "old-pw");
        store.updatePassword("alice", "new-pw");
        Map<String, Object> row = backend.readAll(UserStoreBackend.USERS).get(0);
        String stored = String.valueOf(row.get("PASSWORD_HASH"));
        assertTrue(store.getPasswordHasher().verify("new-pw", stored));
        assertFalse(store.getPasswordHasher().verify("old-pw", stored));
        assertThrows(UserNotFoundException.class, () -> store.updatePassword("ghost", "pw"));
        assertThrows(IllegalArgumentException.class, () -> store.updatePassword("alice", ""));
    }
}
