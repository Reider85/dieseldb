package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for AuthMessage (prompt4.md #31).
 */
public final class AuthMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof AuthMessage)) {
            throw new InvalidMessageException("Expected AuthMessage, got " + message.getClass());
        }
        ((AuthMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return AuthMessage.readFrom(in);
    }
}
