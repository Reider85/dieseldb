package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for {@link CloseCursorMessage} (prompt4.md #29).
 */
public final class CloseCursorMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof CloseCursorMessage)) {
            throw new InvalidMessageException("Expected CloseCursorMessage, got " + message.getClass());
        }
        ((CloseCursorMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return CloseCursorMessage.readFrom(in);
    }
}
