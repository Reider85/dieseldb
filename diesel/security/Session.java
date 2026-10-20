package diesel.security;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * An authenticated session bound to one connection
 * (prompt4 #31, R3-008 step 2/4). The session-scoped user id answers
 * "who is running this query" for every request on the connection.
 */
public final class Session {

    private final UUID userId;
    private final String userName;
    private final SessionToken token;
    private final LocalDateTime createdAt;
    private volatile boolean mustChangePassword;
    private volatile String sourceIp = "embedded";

    /**
     * @param userId             the authenticated user id
     * @param userName           the authenticated user name
     * @param token              the issued session token
     * @param mustChangePassword whether the user must change the password first
     * @param createdAt          session start time
     */
    public Session(UUID userId, String userName, SessionToken token,
            boolean mustChangePassword, LocalDateTime createdAt) {
        this.userId = Objects.requireNonNull(userId, "userId");
        this.userName = Objects.requireNonNull(userName, "userName");
        this.token = Objects.requireNonNull(token, "token");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        this.mustChangePassword = mustChangePassword;
    }

    /** The id of the user running this session's queries. */
    public UUID getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public SessionToken getToken() {
        return token;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    /** True while the session must complete a password change. */
    public boolean isMustChangePassword() {
        return mustChangePassword;
    }

    /** Clears (or re-raises) the forced password-change requirement. */
    public void setMustChangePassword(boolean mustChangePassword) {
        this.mustChangePassword = mustChangePassword;
    }
    /**
     * Remote address of the client owning this session (prompt4 #33), or
     * {@code "embedded"} when the session was created outside the network
     * stack (tests, CLI local mode).
     */
    public String getSourceIp() {
        return sourceIp;
    }

    /** Records the client's remote address for the audit log (prompt4 #33). */
    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp == null || sourceIp.isBlank() ? "embedded" : sourceIp;
    }

    @Override
    public String toString() {
        return "Session{userId=" + userId + ", userName='" + userName + "'}";
    }
}
