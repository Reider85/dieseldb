package diesel.security;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable audit event (prompt4 #33, R3-008 step 4/4): one SQL operation
 * executed by a user, with its outcome and duration.
 *
 * <p>All column values are encoded as Strings when persisted so every storage
 * format round-trips them identically (same rule as the RBAC tables of
 * prompt4 #30).
 */
public final class AuditRecord {

    /** Status recorded for a successful statement. */
    public static final String STATUS_OK = "OK";

    /** Status recorded for a failed statement (parse/authorize/execute error). */
    public static final String STATUS_ERROR = "ERROR";

    /** User label for requests arriving without a session (embedded/legacy). */
    public static final String USER_EMBEDDED = "embedded";

    private final UUID id;
    private final LocalDateTime eventTime;
    private final String userName;
    private final String sourceIp;
    private final String query;
    private final String status;
    private final long durationMs;

    public AuditRecord(UUID id, LocalDateTime eventTime, String userName,
                       String sourceIp, String query, String status, long durationMs) {
        this.id = Objects.requireNonNull(id, "id");
        this.eventTime = Objects.requireNonNull(eventTime, "eventTime");
        this.userName = Objects.requireNonNull(userName, "userName");
        this.sourceIp = Objects.requireNonNull(sourceIp, "sourceIp");
        this.query = Objects.requireNonNull(query, "query");
        this.status = Objects.requireNonNull(status, "status");
        this.durationMs = durationMs;
    }

    /**
     * Named factory (not {@code record()} - Sonar S6213 restricted
     * identifier, precedent 3.1.94). Assigns a fresh UUID and the current
     * timestamp.
     */
    public static AuditRecord create(String userName, String sourceIp,
                                     String query, String status, long durationMs) {
        return new AuditRecord(UUID.randomUUID(), LocalDateTime.now(),
                userName, sourceIp, query, status, durationMs);
    }

    public UUID getId() {
        return id;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public String getUserName() {
        return userName;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public String getQuery() {
        return query;
    }

    public String getStatus() {
        return status;
    }

    public long getDurationMs() {
        return durationMs;
    }

    /**
     * Encodes the record as an all-String row for the {@code DIESEL_AUDIT_LOG}
     * table.
     */
    public Map<String, Object> toRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", id.toString());
        row.put("EVENT_TIME", eventTime.toString());
        row.put("USER_NAME", userName);
        row.put("SOURCE_IP", sourceIp);
        row.put("QUERY", query);
        row.put("STATUS", status);
        row.put("DURATION_MS", Long.toString(durationMs));
        return row;
    }

    /** Decodes a {@link #toRow()} row back into a record. */
    public static AuditRecord fromRow(Map<String, Object> row) {
        return new AuditRecord(
                UUID.fromString(String.valueOf(row.get("ID"))),
                LocalDateTime.parse(String.valueOf(row.get("EVENT_TIME"))),
                String.valueOf(row.get("USER_NAME")),
                String.valueOf(row.get("SOURCE_IP")),
                String.valueOf(row.get("QUERY")),
                String.valueOf(row.get("STATUS")),
                Long.parseLong(String.valueOf(row.get("DURATION_MS"))));
    }
}