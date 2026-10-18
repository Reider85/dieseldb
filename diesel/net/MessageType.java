package diesel.net;

/**
 * Wire identifiers for the DieselDB binary protocol v2 (prompt4.md #27,
 * R3-007 step 1/3).
 *
 * <p>Each message type maps to a stable unsigned 16-bit id written into the
 * frame header. Ids must never be reused or renumbered once published: old
 * clients may still send them. Unknown ids are rejected at decode time with
 * {@link InvalidMessageException}.
 *
 * @see WireProtocol
 */
public enum MessageType {

    /** SQL query with bind variables (prompt4.md #28). */
    QUERY((short) 1),

    /** Result of a single query: columns, rows, status, duration (prompt4.md #28). */
    QUERY_RESULT((short) 2),

    /** Prepare a SQL template (prompt4.md #28). */
    PREPARE((short) 3),

    /** Execute a previously prepared statement with parameters (prompt4.md #28). */
    EXECUTE_PREPARED((short) 4),

    /** Batch of N queries executed in one round-trip (prompt4.md #28). */
    BATCH_QUERY((short) 5),

    /** Close a prepared statement by id. */
    CLOSE_PREPARED((short) 6),

    /** Open a server-side cursor over a query. */
    OPEN_CURSOR((short) 7),

    /** Fetch the next batch of rows from an open cursor. */
    FETCH_CURSOR((short) 8),

    /** Close an open cursor. */
    CLOSE_CURSOR((short) 9),

    /** Health / liveness probe (prompt4.md #28). */
    HEALTH_CHECK((short) 10),

    /** Health probe response (prompt4.md #28). */
    HEALTH_CHECK_RESPONSE((short) 11),

    /** Compression negotiation handshake, client -> server (prompt4.md #29). */
    COMPRESSION_HANDSHAKE((short) 12),

    /** Compression negotiation response, server -> client (prompt4.md #29). */
    COMPRESSION_HANDSHAKE_RESPONSE((short) 13),

    /** Generic error payload (server -> client). */
    ERROR((short) 14);

    private final short id;

    MessageType(short id) {
        this.id = id;
    }

    /**
     * Returns the stable unsigned wire id of this message type.
     *
     * @return the wire id as a short
     */
    public short id() {
        return id;
    }

    /**
     * Resolves a wire id back to its message type.
     *
     * @param id the unsigned wire id
     * @return the matching message type
     * @throws InvalidMessageException if the id is unknown
     */
    public static MessageType fromId(short id) throws InvalidMessageException {
        for (MessageType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new InvalidMessageException("Unknown message type id: " + (id & 0xFFFF));
    }
}
