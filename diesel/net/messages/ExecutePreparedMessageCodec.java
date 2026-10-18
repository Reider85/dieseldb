package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for ExecutePreparedMessage (prompt4.md #28).
 */
public final class ExecutePreparedMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof ExecutePreparedMessage)) {
            throw new InvalidMessageException("Expected ExecutePreparedMessage, got " + message.getClass());
        }
        ExecutePreparedMessage executeMsg = (ExecutePreparedMessage) message;
        executeMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return ExecutePreparedMessage.readFrom(in);
    }
}