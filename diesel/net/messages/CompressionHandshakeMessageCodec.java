package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Codec for CompressionHandshakeMessage (prompt4.md #28).
 */
public final class CompressionHandshakeMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof CompressionHandshakeMessage)) {
            throw new InvalidMessageException("Expected CompressionHandshakeMessage, got " + message.getClass());
        }
        CompressionHandshakeMessage handshakeMsg = (CompressionHandshakeMessage) message;
        handshakeMsg.writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return CompressionHandshakeMessage.readFrom(in);
    }
}