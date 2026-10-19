package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for AuthResponseMessage (prompt4.md #31).
 */
public final class AuthResponseMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof AuthResponseMessage)) {
            throw new InvalidMessageException("Expected AuthResponseMessage, got " + message.getClass());
        }
        ((AuthResponseMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return AuthResponseMessage.readFrom(in);
    }
}
