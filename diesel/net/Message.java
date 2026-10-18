package diesel.net;

/**
 * Base contract for all DieselDB wire-protocol messages (prompt4.md #27,
 * R3-007 step 1/3).
 *
 * <p>Implementations carry explicit fields only — never Java native
 * serialization. Concrete message classes with {@code writeTo}/{@code
 * readFrom} field codecs arrive in prompt4.md #28; the framing layer only
 * needs the type tag to build a frame header.
 *
 * @see MessageCodec
 * @see WireProtocol
 */
public interface Message {

    /**
     * Returns the wire type tag of this message, written into the frame
     * header and into the payload prefix by {@link MessageCodec#encode}.
     *
     * @return the message type, never null
     */
    MessageType getType();
}
