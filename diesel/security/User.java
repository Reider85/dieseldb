package diesel.security;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * An authenticated account. Immutable value object (prompt4 #30, R3-008 step 1/4).
 * The password is never stored in plaintext — {@code passwordHash} holds the
 * encoded salted hash produced by {@link PasswordHasher}.
 */
public final class User {

    private final UUID id;
    private final String name;
    private final String passwordHash;
    private final LocalDateTime createdAt;

    public User(UUID id, String name, String passwordHash, LocalDateTime createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = Objects.requireNonNull(name, "name");
        this.passwordHash = Objects.requireNonNull(passwordHash, "passwordHash");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return "User{id=" + id + ", name='" + name + "', createdAt=" + createdAt + "}";
    }
}
