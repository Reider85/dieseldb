package diesel.net.messages;

import diesel.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry mapping message types to their codecs (prompt4.md #28).
 */
public final class MessageCodecRegistry {
    
    private static final Map<MessageType, MessageCodec> CODECS = new HashMap<>();
    
    static {
        // Register all message types with their codecs
        CODECS.put(MessageType.QUERY, new QueryMessageCodec());
        CODECS.put(MessageType.QUERY_RESULT, new QueryResultMessageCodec());
        CODECS.put(MessageType.PREPARE, new PrepareMessageCodec());
        CODECS.put(MessageType.EXECUTE_PREPARED, new ExecutePreparedMessageCodec());
        CODECS.put(MessageType.BATCH_QUERY, new BatchQueryMessageCodec());
        CODECS.put(MessageType.HEALTH_CHECK, new HealthCheckMessageCodec());
        CODECS.put(MessageType.HEALTH_CHECK_RESPONSE, new HealthCheckResponseCodec());
        CODECS.put(MessageType.COMPRESSION_HANDSHAKE, new CompressionHandshakeMessageCodec());
        CODECS.put(MessageType.PREPARE_RESPONSE, new PrepareResponseMessageCodec());
        CODECS.put(MessageType.CLOSE_PREPARED_RESPONSE, new ClosePreparedResponseMessageCodec());
        CODECS.put(MessageType.CLOSE_CURSOR_RESPONSE, new CloseCursorResponseMessageCodec());
    }
    
    /**
     * Gets the codec for the given message type.
     *
     * @param type the message type
     * @return the codec for that type
     * @throws InvalidMessageException if no codec is registered for the type
     */
    public static MessageCodec getCodec(MessageType type) throws InvalidMessageException {
        MessageCodec codec = CODECS.get(type);
        if (codec == null) {
            throw new InvalidMessageException("No codec registered for message type: " + type);
        }
        return codec;
    }
    
    /**
     * Checks if a codec is registered for the given message type.
     *
     * @param type the message type
     * @return true if a codec is registered, false otherwise
     */
    public static boolean hasCodec(MessageType type) {
        return CODECS.containsKey(type);
    }
    
    private MessageCodecRegistry() {
        // Utility class - prevent instantiation
    }
}