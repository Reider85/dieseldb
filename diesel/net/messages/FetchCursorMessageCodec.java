package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for {@link FetchCursorMessage} (prompt4.md #29).
 */
public final class FetchCursorMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof FetchCursorMessage)) {
            throw new InvalidMessageException("Expected FetchCursorMessage, got " + message.getClass());
        }
        ((FetchCursorMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return FetchCursorMessage.readFrom(in);
    }
}
