package diesel.net.messages;

import diesel.net.Message;
import diesel.net.MessageCodec;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Binary codec for {@link CompressionHandshakeResponseMessage}.
 */
public final class CompressionHandshakeResponseMessageCodec implements MessageCodec {

    @Override
    public void encode(Message message, DataOutput out) throws IOException {
        if (!(message instanceof CompressionHandshakeResponseMessage)) {
            throw new IllegalArgumentException(
                    "Expected CompressionHandshakeResponseMessage, got: " + message);
        }
        ((CompressionHandshakeResponseMessage) message).writeTo(out);
    }

    @Override
    public Message decode(DataInput in) throws IOException {
        return CompressionHandshakeResponseMessage.readFrom(in);
    }
}