package diesel.wal;

/**
 * Operation codes for WAL entries (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>Stored as 1 byte in the entry header. The declaration order matches the
 * wire code, so {@code values()[b]} is valid for any known code; unknown bytes
 * are rejected by {@link #fromByte(byte)}.
 *
 * <p>Codes 1..7 are the ops mandated by prompt 11
 * (INSERT/UPDATE/DELETE/COMMIT/ABORT/TRUNCATE/CHECKPOINT). {@code BEGIN} is
 * reserved as code 0 so the ARIES recovery prompts (16-19) can emit BEGIN
 * records without a format change. Code 8 ({@code PAGE_IMAGE}) was appended
 * for the ARIES redo phase (prompt 18) and is never present in logs written
 * by earlier versions, so old segments stay readable.
 */
public enum WALOpcode {

    /** Transaction start — reserved for ARIES analysis phase (prompt 17). */
    BEGIN(0),
    INSERT(1),
    UPDATE(2),
    DELETE(3),
    COMMIT(4),
    ABORT(5),
    TRUNCATE(6),
    CHECKPOINT(7),
    /**
     * Physical page after-image for ARIES redo (prompt 4 #18): the after-image
     * is a full serialized page ({@code Page.toBytes()}); the before-image is
     * unused by the redo phase.
     */
    PAGE_IMAGE(8);

    private static final WALOpcode[] BY_CODE = values();

    private final byte code;

    WALOpcode(int code) {
        this.code = (byte) code;
    }

    /**
     * Returns the 1-byte wire code of this opcode.
     */
    public byte code() {
        return code;
    }

    /**
     * Maps a wire byte to its opcode.
     *
     * @param b the raw opcode byte
     * @return the matching opcode
     * @throws IllegalArgumentException if the byte is not a known opcode
     */
    public static WALOpcode fromByte(byte b) {
        int unsigned = b & 0xFF;
        if (unsigned >= BY_CODE.length) {
            throw new IllegalArgumentException("Unknown WAL opcode: " + unsigned);
        }
        return BY_CODE[unsigned];
    }
}
