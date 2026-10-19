package diesel.security;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * A named role that can be granted to users. Immutable value object
 * (prompt4 #30, R3-008 step 1/4). Privileges are attached to roles in step 3.
 */
public final class Role {

    private final UUID id;
    private final String name;
    private final LocalDateTime createdAt;

    public Role(UUID id, String name, LocalDateTime createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = Objects.requireNonNull(name, "name");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return "Role{id=" + id + ", name='" + name + "', createdAt=" + createdAt + "}";
    }
}
