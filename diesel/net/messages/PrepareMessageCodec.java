package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for PrepareMessage (prompt4.md #28).
 */
public final class PrepareMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof PrepareMessage)) {
            throw new InvalidMessageException("Expected PrepareMessage, got " + message.getClass());
        }
        PrepareMessage prepareMsg = (PrepareMessage) message;
        prepareMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return PrepareMessage.readFrom(in);
    }
}