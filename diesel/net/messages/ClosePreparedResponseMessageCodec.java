package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for ClosePreparedResponseMessage (prompt4.md #28).
 */
public final class ClosePreparedResponseMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof ClosePreparedResponseMessage)) {
            throw new InvalidMessageException("Expected ClosePreparedResponseMessage, got " + message.getClass());
        }
        ClosePreparedResponseMessage response = (ClosePreparedResponseMessage) message;
        response.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return ClosePreparedResponseMessage.readFrom(in);
    }
}