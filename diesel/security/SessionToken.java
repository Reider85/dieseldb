package diesel.security;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Opaque session token issued by the credentials handshake
 * (prompt4 #31, R3-008 step 2/4). Value object.
 */
public final class SessionToken {

    private final UUID id;
    private final LocalDateTime issuedAt;

    public SessionToken(UUID id, LocalDateTime issuedAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.issuedAt = Objects.requireNonNull(issuedAt, "issuedAt");
    }

    public UUID getId() {
        return id;
    }

    public LocalDateTime getIssuedAt() {
        return issuedAt;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SessionToken other && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "SessionToken{id=" + id + ", issuedAt=" + issuedAt + "}";
    }
}
