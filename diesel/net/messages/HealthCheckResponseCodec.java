package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for HealthCheckResponse (prompt4.md #28).
 */
public final class HealthCheckResponseCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof HealthCheckResponse)) {
            throw new InvalidMessageException("Expected HealthCheckResponse, got " + message.getClass());
        }
        HealthCheckResponse healthResponse = (HealthCheckResponse) message;
        healthResponse.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return HealthCheckResponse.readFrom(in);
    }
}