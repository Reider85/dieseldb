package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for BatchQueryMessage (prompt4.md #28).
 */
public final class BatchQueryMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof BatchQueryMessage)) {
            throw new InvalidMessageException("Expected BatchQueryMessage, got " + message.getClass());
        }
        BatchQueryMessage batchMsg = (BatchQueryMessage) message;
        batchMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return BatchQueryMessage.readFrom(in);
    }
}