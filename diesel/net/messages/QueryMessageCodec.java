package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for QueryMessage (prompt4.md #28).
 */
public final class QueryMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof QueryMessage)) {
            throw new InvalidMessageException("Expected QueryMessage, got " + message.getClass());
        }
        QueryMessage queryMsg = (QueryMessage) message;
        queryMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return QueryMessage.readFrom(in);
    }
}