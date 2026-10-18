package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for QueryResultMessage (prompt4.md #28).
 */
public final class QueryResultMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof QueryResultMessage)) {
            throw new InvalidMessageException("Expected QueryResultMessage, got " + message.getClass());
        }
        QueryResultMessage resultMsg = (QueryResultMessage) message;
        resultMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return QueryResultMessage.readFrom(in);
    }
}