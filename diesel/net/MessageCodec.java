package diesel.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Explicit field codec for DieselDB wire-protocol v2 messages (prompt4.md
 * #27, R3-007 step 1/3).
 *
 * <p>Payload layout produced/consumed by implementations:
 * <pre>
 *   [type: u16 BE][message-specific fields...]
 * </pre>
 * The leading type tag makes {@link #decode} self-dispatching: the frame
 * header type (validated by {@link WireProtocol#decodeFrame}) and the
 * payload type tag must agree, giving a second line of defense against
 * corrupted or forged frames.
 *
 * <p>No {@code ObjectInputStream}/{@code ObjectOutputStream} is ever involved
 * — only explicit primitive and string fields.
 *
 * @see WireProtocol#writeMessage
 * @see WireProtocol#readMessage
 */
public interface MessageCodec {

    /**
     * Writes the message payload (type tag + fields) to the given output.
     * Framing (magic/version/length/CRC) is added by
     * {@link WireProtocol#writeMessage}.
     *
     * @param message the message to encode, never null
     * @param out     the payload sink
     * @throws IOException if a field cannot be written
     */
    void encode(Message message, DataOutput out) throws IOException;

    /**
     * Reads a message payload (type tag + fields) from the given input.
     *
     * @param in the payload source
     * @return the decoded message
     * @throws IOException            if a field cannot be read
     * @throws InvalidMessageException if the payload type tag is unknown or
     *                                 does not match the message content
     */
    Message decode(DataInput in) throws IOException;
}
