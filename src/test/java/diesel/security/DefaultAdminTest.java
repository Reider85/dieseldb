package diesel.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt4 #31 acceptance (DefaultAdminTest): the first start on an empty user
 * store creates admin/admin with a forced password change on the first login;
 * later starts and stores that already have users are left alone.
 */
@Tag("smoke")
class DefaultAdminTest {

    private UserStore store;
    private Authenticator authenticator;

    @BeforeEach
    void setUp() {
        store = new UserStore(new InMemoryUserStoreBackend(), new PasswordHasher(1_000));
        authenticator = new Authenticator(store);
    }

    @Test
    void firstRunCreatesFlaggedAdminAcceptedByAuthenticator() {
        User admin = store.ensureDefaultAdmin();
        assertEquals(UserStore.ADMIN_NAME, admin.getName());
        assertTrue(admin.isMustChangePassword(), "the bootstrap account is flagged for a change");
        assertTrue(PasswordHasher.requiresPasswordChange(admin.getPasswordHash()));

        Session session = authenticator.authenticate(UserStore.ADMIN_NAME, UserStore.DEFAULT_ADMIN_PASSWORD);
        assertTrue(session.isMustChangePassword(), "the first login must demand a new password");
    }

    @Test
    void bootstrapSkippedWhenUsersAlreadyExist() {
        store.createUser("alice", "pw");
        User returned = store.ensureDefaultAdmin();
        assertEquals("alice", returned.getName(), "the existing user is returned");
        assertFalse(store.userExists("admin"), "no admin is injected into a populated store");
    }

    @Test
    void bootstrapIsIdempotent() {
        store.ensureDefaultAdmin();
        UUID firstId = store.findUser(UserStore.ADMIN_NAME).orElseThrow().getId();
        store.updatePassword(UserStore.ADMIN_NAME, "changed-pw");

        User again = store.ensureDefaultAdmin();
        assertEquals(firstId, again.getId(), "the existing admin is reused, not recreated");
        assertFalse(again.isMustChangePassword(), "bootstrap never overwrites a changed password");
        assertFalse(PasswordHasher.requiresPasswordChange(again.getPasswordHash()));
    }
}
