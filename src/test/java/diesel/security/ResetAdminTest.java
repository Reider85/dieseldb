package diesel.security;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt4 #31 acceptance (ResetAdminTest): resetting the admin restores the
 * bootstrap state (admin/admin plus the forced-change flag), recreating the
 * account when it was removed. The filesystem-only gate of the CLI flag
 * {@code --reset-admin} lives in the engine test AuthHandshakeTest, which
 * exercises {@code CliRepl.resetAdmin} against a real data directory.
 */
@Tag("smoke")
class ResetAdminTest {

    private UserStore store;
    private Authenticator authenticator;

    @BeforeEach
    void setUp() {
        store = new UserStore(new InMemoryUserStoreBackend(), new PasswordHasher(1_000));
        authenticator = new Authenticator(store);
    }

    @Test
    void resetRestoresDefaultPasswordAndForcedChange() {
        store.ensureDefaultAdmin();
        store.updatePassword(UserStore.ADMIN_NAME, "changed-pw");
        authenticator.authenticate(UserStore.ADMIN_NAME, "changed-pw");
        assertTrue(store.resetAdminPassword().isMustChangePassword());

        authenticator.authenticate(UserStore.ADMIN_NAME, UserStore.DEFAULT_ADMIN_PASSWORD);
        assertThrows(AuthException.class, () -> authenticator.authenticate(UserStore.ADMIN_NAME, "changed-pw"),
                "the old password no longer works after the reset");
    }

    @Test
    void resetRecreatesMissingAdmin() {
        store.createUser("alice", "pw");
        User recreated = store.resetAdminPassword();
        assertTrue(recreated.isMustChangePassword());
        assertTrue(store.userExists(UserStore.ADMIN_NAME));
        authenticator.authenticate(UserStore.ADMIN_NAME, UserStore.DEFAULT_ADMIN_PASSWORD);
    }
}
