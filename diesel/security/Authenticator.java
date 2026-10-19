package diesel.security;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Verifies credentials and manages sessions (prompt4 #31, R3-008 step 2/4).
 *
 * <p>Passwords are checked through {@link PasswordHasher} (constant-time);
 * a failed attempt throws {@link AuthException} with a uniform message.
 * One shared instance per server keeps the token registry; connections
 * additionally hold their own {@link Session} reference.
 */
public final class Authenticator {

    private final UserStore store;
    private final Map<UUID, Session> sessions = new ConcurrentHashMap<>();

    public Authenticator(UserStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Returns the user store backing this authenticator. */
    public UserStore getUserStore() {
        return store;
    }

    /**
     * Verifies the credentials and opens a session.
     *
     * @param userName the account name
     * @param password the plaintext password
     * @return the fresh session, with a newly issued token
     * @throws AuthException when the account does not exist or the password
     *                       does not match
     */
    public Session authenticate(String userName, String password) throws AuthException {
        if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
            throw new AuthException(AuthException.INVALID_CREDENTIALS);
        }
        User user = store.findUser(userName).orElse(null);
        if (user == null) {
            throw new AuthException(AuthException.INVALID_CREDENTIALS);
        }
        if (!store.getPasswordHasher().verify(password, user.getPasswordHash())) {
            throw new AuthException(AuthException.INVALID_CREDENTIALS);
        }
        SessionToken token = new SessionToken(UUID.randomUUID(), LocalDateTime.now());
        Session session = new Session(user.getId(), user.getName(), token,
                user.isMustChangePassword(), LocalDateTime.now());
        sessions.put(token.getId(), session);
        return session;
    }

    /** Resolves a live session by its token id, when still registered. */
    public Optional<Session> findSession(UUID tokenId) {
        return tokenId == null ? Optional.empty() : Optional.ofNullable(sessions.get(tokenId));
    }

    /** Drops a session from the registry (connection close / logout). */
    public void invalidate(SessionToken token) {
        if (token != null) {
            sessions.remove(token.getId());
        }
    }

    /** Number of live registered sessions (metrics/tests). */
    public int sessionCount() {
        return sessions.size();
    }
}
