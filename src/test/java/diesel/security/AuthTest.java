package diesel.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt4 #31 acceptance (AuthTest): wrong password raises AuthException with
 * the uniform message, valid credentials open a registered session, and the
 * must-change flag travels into the session.
 */
@Tag("smoke")
class AuthTest {

    private UserStore store;
    private Authenticator authenticator;

    @BeforeEach
    void setUp() {
        // Low iteration count keeps the unit suite fast.
        store = new UserStore(new InMemoryUserStoreBackend(), new PasswordHasher(1_000));
        authenticator = new Authenticator(store);
    }

    @Test
    void validCredentialsOpenRegisteredSession() {
        store.createUser("alice", "s3cret");
        Session session = authenticator.authenticate("alice", "s3cret");
        assertEquals("alice", session.getUserName());
        assertNotNull(session.getToken());
        assertEquals(1, authenticator.sessionCount());
        assertTrue(authenticator.findSession(session.getToken().getId()).isPresent());
        assertFalse(session.isMustChangePassword(), "a plain createUser carries no forced change");
    }

    @Test
    void wrongPasswordThrowsAuthException() {
        store.createUser("alice", "s3cret");
        AuthException e = assertThrows(AuthException.class,
                () -> authenticator.authenticate("alice", "wrong"));
        assertEquals(AuthException.INVALID_CREDENTIALS, e.getMessage());
        assertEquals(0, authenticator.sessionCount(), "a failed attempt opens no session");
    }

    @Test
    void unknownUserThrowsSameMessageAsWrongPassword() {
        store.createUser("alice", "s3cret");
        AuthException wrongPassword = assertThrows(AuthException.class,
                () -> authenticator.authenticate("alice", "wrong"));
        AuthException unknownUser = assertThrows(AuthException.class,
                () -> authenticator.authenticate("ghost", "s3cret"));
        assertEquals(unknownUser.getMessage(), wrongPassword.getMessage(),
                "the message must not reveal whether the account exists");
    }

    @Test
    void blankCredentialsThrow() {
        store.createUser("alice", "s3cret");
        assertThrows(AuthException.class, () -> authenticator.authenticate(null, "s3cret"));
        assertThrows(AuthException.class, () -> authenticator.authenticate("", "s3cret"));
        assertThrows(AuthException.class, () -> authenticator.authenticate("alice", null));
        assertThrows(AuthException.class, () -> authenticator.authenticate("alice", ""));
    }

    @Test
    void mustChangeFlagTravelsIntoSessionAndClearsOnChange() {
        store.ensureDefaultAdmin();
        Session first = authenticator.authenticate(UserStore.ADMIN_NAME, UserStore.DEFAULT_ADMIN_PASSWORD);
        assertTrue(first.isMustChangePassword(), "the bootstrap admin must change its password");

        store.updatePassword(UserStore.ADMIN_NAME, "fresh-pw");
        Session second = authenticator.authenticate(UserStore.ADMIN_NAME, "fresh-pw");
        assertFalse(second.isMustChangePassword(), "updatePassword clears the forced change");
    }

    @Test
    void invalidateDropsSessionFromRegistry() {
        store.createUser("alice", "s3cret");
        Session session = authenticator.authenticate("alice", "s3cret");
        authenticator.invalidate(session.getToken());
        assertEquals(0, authenticator.sessionCount());
        assertTrue(authenticator.findSession(session.getToken().getId()).isEmpty());
        authenticator.invalidate(null);
        assertEquals(0, authenticator.sessionCount(), "null token is a no-op");
    }
}
