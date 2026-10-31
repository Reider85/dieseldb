package diesel.net.libpq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Tests for libpq protocol encoding and decoding.
 */
public class LibpqProtocolTest {
    
    @Test
    void testEncodeMessage() {
        String testMessage = "test message";
        byte[] payload = testMessage.getBytes(StandardCharsets.UTF_8);
        
        byte[] encoded = LibpqProtocol.encodeMessage((byte)'Q', payload);
        
        // Check message structure: type (1 byte) + length (4 bytes) + payload
        assertEquals('Q', encoded[0]);
        
        // Check length field (big-endian)
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertEquals(payload.length + 4, length);
        
        // Check payload
        assertEquals(testMessage, new String(encoded, 5, payload.length, StandardCharsets.UTF_8));
    }
    
    @Test
    void testEncodeAuthRequest() {
        byte[] payload = "SCRAM-SHA-256".getBytes(StandardCharsets.UTF_8);
        
        byte[] encoded = LibpqProtocol.encodeAuthRequest(LibpqProtocol.AUTH_SASL, payload);
        
        // Check message structure
        assertEquals('R', encoded[0]);
        
        // Check length field
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertEquals(payload.length + 5 + 4, length); // +5 for auth code (4) + null terminator (1) +4 for length field
        
        // Check authentication code
        assertEquals(LibpqProtocol.AUTH_SASL, encoded[8]); // authType is 4 bytes, big-endian
        
        // Check payload (starts after null terminator at position 9)
        assertEquals("SCRAM-SHA-256", new String(encoded, 10, payload.length, StandardCharsets.UTF_8));
    }
    
    @Test
    void testEncodeReadyForQuery() {
        byte[] encoded = LibpqProtocol.encodeReadyForQuery(LibpqProtocol.IDLE);
        
        // Check message structure
        assertEquals('Z', encoded[0]);
        
        // Check length field
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertEquals(6, length); // 2 bytes payload + 4 bytes length
        
        // Check transaction status
        assertEquals(LibpqProtocol.IDLE, encoded[5]);
    }
    
    @Test
    void testEncodeRowDescription() {
        LibpqMessage.FieldDescription[] fields = {
            new LibpqMessage.FieldDescription(0, "id", (short)0, (short)0, 0, (short)23, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "name", (short)0, (short)0, 0, (short)25, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "email", (short)0, (short)0, 0, (short)25, (short)0, (short)0)
        };
        
        byte[] encoded = LibpqProtocol.encodeRowDescription(fields);
        
        // Check message structure
        assertEquals('T', encoded[0]);
        
        // Check length field (should be large enough to contain all field descriptions)
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertTrue(length > 4); // Should be more than just the header
        
        // Check field count (short, 2 bytes)
        int fieldCount = ((encoded[5] & 0xFF) << 8) | 
                         (encoded[6] & 0xFF);
        
        assertEquals(3, fieldCount);
    }
    
    @Test
    void testEncodeCommandComplete() {
        String tag = "SELECT 1";
        byte[] encoded = LibpqProtocol.encodeCommandComplete(tag);
        
        // Check message structure
        assertEquals('C', encoded[0]);
        
        // Check length field
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertEquals(tag.length() + 4 + 1, length); // +4 for length field, +1 for null terminator
        
        // Check tag
        assertEquals(tag, new String(encoded, 5, tag.length(), StandardCharsets.UTF_8));
        
        // Check null terminator
        assertEquals(0, encoded[5 + tag.length()]);
    }
    
    @Test
    void testEncodeDataRow() {
        Object[] row = {1, "test", "test@example.com"};
        
        byte[] encoded = LibpqProtocol.encodeDataRow(row);
        
        // Check message structure
        assertEquals('D', encoded[0]);
        
        // Check length field
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertTrue(length > 4); // Should be more than just the header
        
        // Check column count
        int columnCount = ((encoded[5] & 0xFF) << 8) | (encoded[6] & 0xFF);
        
        assertEquals(3, columnCount);
    }
    
    @Test
    void testDecodeMessage() {
        // Create a test message
        String testMessage = "test query";
        byte[] payload = testMessage.getBytes(StandardCharsets.UTF_8);
        
        byte[] encoded = LibpqProtocol.encodeMessage((byte)'Q', payload);
        
        // Simulate message decoding
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        
        // Read type
        byte type = buffer.get();
        assertEquals('Q', type);
        
        // Read length
        int length = buffer.getInt();
        assertEquals(payload.length + 4, length);
        
        // Read payload
        byte[] decodedPayload = new byte[payload.length];
        buffer.get(decodedPayload);
        
        assertEquals(testMessage, new String(decodedPayload, StandardCharsets.UTF_8));
    }
    
    @Test
    void testProtocolConstants() {
        // Test that protocol constants are properly defined
        assertEquals(0x00, LibpqProtocol.AUTH_OK);
        assertEquals(0x0A, LibpqProtocol.AUTH_SASL);
        assertEquals('I', (char)LibpqProtocol.IDLE);
        assertEquals('Q', (char)LibpqProtocol.SIMPLE_QUERY);
        assertEquals('1', (char)LibpqProtocol.PARSE_COMPLETE);
        assertEquals('3', (char)LibpqProtocol.CLOSE_COMPLETE); 
        assertEquals('Z', (char)LibpqProtocol.READY_FOR_QUERY);
        assertEquals('C', (char)LibpqProtocol.COMMAND_COMPLETE);
        assertEquals('T', (char)LibpqProtocol.ROW_DESCRIPTION);
        assertEquals('D', (char)LibpqProtocol.DATA_ROW);
        assertEquals('R', (char)LibpqProtocol.PASSWORD_CHALLENGE); // Using PASSWORD_CHALLENGE instead
        assertEquals('Z', (char)LibpqProtocol.READY_FOR_QUERY);
        assertEquals('S', LibpqProtocol.PARAMETER_STATUS);
        assertEquals('E', LibpqProtocol.ERROR_RESPONSE);
    }
}