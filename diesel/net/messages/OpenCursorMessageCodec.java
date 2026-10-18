package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for {@link OpenCursorMessage} (prompt4.md #29).
 */
public final class OpenCursorMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof OpenCursorMessage)) {
            throw new InvalidMessageException("Expected OpenCursorMessage, got " + message.getClass());
        }
        ((OpenCursorMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return OpenCursorMessage.readFrom(in);
    }
}
