package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for HealthCheckMessage (prompt4.md #28).
 */
public final class HealthCheckMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof HealthCheckMessage)) {
            throw new InvalidMessageException("Expected HealthCheckMessage, got " + message.getClass());
        }
        HealthCheckMessage healthMsg = (HealthCheckMessage) message;
        healthMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return HealthCheckMessage.readFrom(in);
    }
}