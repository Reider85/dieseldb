package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for PrepareResponseMessage (prompt4.md #28).
 */
public final class PrepareResponseMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof PrepareResponseMessage)) {
            throw new InvalidMessageException("Expected PrepareResponseMessage, got " + message.getClass());
        }
        PrepareResponseMessage response = (PrepareResponseMessage) message;
        response.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return PrepareResponseMessage.readFrom(in);
    }
}