package diesel.net.libpq;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * libpq message protocol constants and encode/decode helpers.
 * 
 * Implements PostgreSQL wire protocol message format:
 * - Message type (1 byte)
 * - Message length (4 bytes, includes self)
 * - Message payload (variable)
 * 
 * https://www.postgresql.org/docs/current/protocol-message-formats.html
 */
public final class LibpqProtocol {
    
    // Message types (single ASCII character)
    public static final byte BYTECODE = 'B';  // Backend key data
    public static final byte CANCELLATION_KEY = 'K';  // Secret key for cancellation
    public static final byte CLOSE_COMPLETE = '3';  // Close completed
    public static final byte COMMAND_COMPLETE = 'C';  // Command completed
    public static final byte COPY_DATA = 'd';  // Copy data
    public static final byte COPY_DONE = 'c';  // Copy done
    public static final byte COPY_FAIL = 'f';  // Copy fail
    public static final byte COPY_IN_RESPONSE = 'G';  // Copy-in response message
    public static final byte COPY_OUT_RESPONSE = 'H';  // Copy-out response message
    public static final byte DATA_ROW = 'D';  // Row data
    public static final byte ERROR_RESPONSE = 'E';  // Error response
    public static final byte FUNCTION_CALL_RESPONSE = 'V';  // Function call response
    public static final byte NEGOTIATE_PROTOCOL_VERSION = 'v';  // Negotiate protocol version
    public static final byte NOTICE_RESPONSE = 'N';  // Notice response
    public static final byte PARAMETER_DESCRIPTION = 't';  // Parameter description
    public static final byte PARAMETER_STATUS = 'S';  // Parameter status
    public static final byte PARSE_COMPLETE = '1';  // Parse completed
    public static final byte BIND_COMPLETE = '2';  // Bind completed
    public static final byte PASSWORD_CHALLENGE = 'R';  // Authentication request
    public static final byte PORTAL_SUSPENDED = 's';  // Portal suspended
    public static final byte READY_FOR_QUERY = 'Z';  // Ready for query
    public static final byte ROW_DESCRIPTION = 'T';  // Row description
    public static final byte SASL_CONTINUATION = 'c';  // SASL continuation
    public static final byte SASL_RESPONSE = 'p';  // SASL response
    public static final byte SIMPLE_QUERY = 'Q';  // Simple query
    public static final byte STARTUP_MESSAGE = '0';  // Startup message
    public static final byte TERMINATION = 'X';  // Server closing connection
    
    // Authentication types
    public static final int AUTH_OK = 0;
    public static final int AUTH_KRB5 = 2;
    public static final int AUTH_KRB5_WITH_PASS = 3;
    public static final int AUTH_CRYPT = 4;
    public static final int AUTH_MD5 = 5;
    public static final int AUTH_SCM_CREDENTIALS = 6;
    public static final int AUTH_GSS = 7;
    public static final int AUTH_GSS_CONT = 8;
    public static final int AUTH_SSPI = 9;
    public static final int AUTH_SASL = 10;
    public static final int AUTH_SASL_CONTINUE = 11;
    public static final int AUTH_SASL_FINAL = 12;
    public static final int AUTH_SCREAM_SHA256 = 13;
    
    // Transaction status codes for ReadyForQuery
    public static final byte IDLE = 'I';
    public static final byte IN_TRANSACTION = 'T';
    public static final byte IN_FAILED_TRANSACTION = 'E';
    
    // Default port
    public static final int DEFAULT_PORT = 5432;
    
    // libpq protocol version
    public static final int PROTOCOL_VERSION = 196608; // 3.0
    
    private LibpqProtocol() {
        // Utility class
    }
    
    /**
     * Encodes a message with the given type and payload.
     */
    public static byte[] encodeMessage(byte type, byte[] payload) {
        int messageLength = 4 + payload.length; // length field includes type (1) + length (4) + payload
        
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + payload.length); // type + length + payload
        buffer.put(type);
        buffer.putInt(messageLength);
        buffer.put(payload);
        
        return buffer.array();
    }
    
    /**
     * Decodes a message from a ByteBuffer.
     * 
     * @return decoded message or null if not enough data
     */
    public static LibpqMessage decodeMessage(ByteBuffer buffer) {
        if (buffer.remaining() < 5) {
            return null; // Not enough data for header
        }
        
        byte type = buffer.get();
        int length = buffer.getInt();
        
        if (length < 5) {
            throw new IllegalArgumentException("Invalid message length: " + length);
        }
        
        int payloadLength = length - 4;
        if (buffer.remaining() < payloadLength) {
            return null; // Not enough data for payload
        }
        
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        
        return new LibpqMessage(type, payload);
    }
    
    /**
     * Encodes a string as UTF-8 with null terminator.
     */
    public static byte[] encodeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        result[bytes.length] = 0; // Null terminator
        return result;
    }
    
    /**
     * Decodes a null-terminated string from UTF-8 bytes.
     */
    public static String decodeString(byte[] bytes) {
        int nullPos = 0;
        while (nullPos < bytes.length && bytes[nullPos] != 0) {
            nullPos++;
        }
        
        return new String(bytes, 0, nullPos, StandardCharsets.UTF_8);
    }
    
    /**
     * Encodes an authentication request message.
     */
    public static byte[] encodeAuthRequest(int authType, byte[]... additionalData) {
        // Calculate total size
        int totalSize = 5; // authType (4) + null terminator (1)
        
        for (byte[] data : additionalData) {
            totalSize += data.length;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(authType);
        buffer.put((byte) 0);
        
        // Add additional data if any
        for (byte[] data : additionalData) {
            buffer.put(data);
        }
        
        return encodeMessage(PASSWORD_CHALLENGE, buffer.array());
    }
    
    /**
     * Encodes a CommandComplete message.
     */
    public static byte[] encodeCommandComplete(String tag) {
        byte[] tagBytes = encodeString(tag);
        return encodeMessage(COMMAND_COMPLETE, tagBytes);
    }
    
    /**
     * Encodes a ReadyForQuery message.
     */
    public static byte[] encodeReadyForQuery(byte transactionStatus) {
        byte[] status = new byte[]{transactionStatus, 0}; // transaction status + null terminator
        return encodeMessage(READY_FOR_QUERY, status);
    }
    
    /**
     * Encodes a RowDescription message.
     */
    public static byte[] encodeRowDescription(LibpqMessage.FieldDescription[] fields) {
        int totalLength = 2; // Number of fields (2 bytes)
        
        for (LibpqMessage.FieldDescription field : fields) {
            totalLength += 4; // Object ID (4 bytes)
            totalLength += field.name.length() + 1; // Name + null terminator
            totalLength += 2; // Table ID (2 bytes)
            totalLength += 2; // Column ID (2 bytes)
            totalLength += 4; // Type ID (4 bytes)
            totalLength += 2; // Type size (2 bytes)
            totalLength += 2; // Type modifier (2 bytes)
            totalLength += 2; // Format code (2 bytes)
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putShort((short) fields.length);
        
        for (LibpqMessage.FieldDescription field : fields) {
            buffer.putInt(field.objectId);
            byte[] nameBytes = encodeString(field.name);
            buffer.put(nameBytes);
            buffer.putShort(field.tableId);
            buffer.putShort(field.columnId);
            buffer.putInt(field.fieldType);
            buffer.putShort(field.typeSize);
            buffer.putShort(field.typeModifier);
            buffer.putShort(field.formatCode);
        }
        
        return encodeMessage(ROW_DESCRIPTION, buffer.array());
    }
    
    /**
     * Encodes a DataRow message.
     */
    public static byte[] encodeDataRow(Object[] row) {
        // Calculate total payload size
        int payloadSize = 2; // column count (short)
        
        for (Object value : row) {
            if (value == null) {
                payloadSize += 4; // length int (-1)
            } else {
                byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                payloadSize += 4 + bytes.length; // length int + bytes
            }
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
        
        // Write column count
        buffer.putShort((short) row.length);
        
        // Write each column
        for (Object value : row) {
            if (value == null) {
                buffer.putInt(-1);
            } else {
                byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            }
        }
        
        return encodeMessage(DATA_ROW, buffer.array());
    }
    
    
}