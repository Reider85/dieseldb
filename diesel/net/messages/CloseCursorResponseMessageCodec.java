package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for CloseCursorResponseMessage (prompt4.md #28).
 */
public final class CloseCursorResponseMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof CloseCursorResponseMessage)) {
            throw new InvalidMessageException("Expected CloseCursorResponseMessage, got " + message.getClass());
        }
        CloseCursorResponseMessage response = (CloseCursorResponseMessage) message;
        response.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return CloseCursorResponseMessage.readFrom(in);
    }
}