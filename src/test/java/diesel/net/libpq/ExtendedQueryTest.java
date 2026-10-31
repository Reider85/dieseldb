package diesel.net.libpq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Tests for extended query protocol (Parse/Bind/Execute).
 */
public class ExtendedQueryTest {
    
    @Test
    void testParseMessageFormat() {
        // Test Parse message format: [statementName][null][query][null][paramCount][paramTypes...]
        String statementName = "stmt1";
        String query = "SELECT * FROM users WHERE id = $1";
        int paramCount = 1;
        int[] paramTypes = {23}; // integer type
        
        ByteBuffer buffer = ByteBuffer.allocate(
            statementName.length() + 1 + // statement name + null
            query.length() + 1 +         // query + null
            2 +                         // param count (short)
            paramCount * 4              // param types (int)
        );
        
        // Write statement name
        buffer.put(statementName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        
        // Write query
        buffer.put(query.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        
        // Write parameter count
        buffer.put((byte) (paramCount >> 8));
        buffer.put((byte) paramCount);
        
        // Write parameter types
        for (int type : paramTypes) {
            buffer.putInt(type);
        }
        
        byte[] payload = buffer.array();
        
        // Verify the message structure
        assertEquals(statementName, readNullTerminatedString(payload, 0));
        assertEquals(query, readNullTerminatedString(payload, statementName.length() + 1));
        
        int paramCountPos = statementName.length() + 1 + query.length() + 1;
        int actualParamCount = ((payload[paramCountPos] & 0xFF) << 8) | (payload[paramCountPos + 1] & 0xFF);
        assertEquals(paramCount, actualParamCount);
        
        int firstParamTypePos = paramCountPos + 2;
        int firstParamType = ((payload[firstParamTypePos] & 0xFF) << 24) |
                           ((payload[firstParamTypePos + 1] & 0xFF) << 16) |
                           ((payload[firstParamTypePos + 2] & 0xFF) << 8) |
                           (payload[firstParamTypePos + 3] & 0xFF);
        assertEquals(23, firstParamType);
    }
    
    @Test
    void testBindMessageFormat() {
        // Test Bind message format: [portalName][null][statementName][null][paramFormatCount][paramFormats...][paramCount][paramLengths][paramValues...][resultFormatCount][resultFormats...]
        String portalName = "portal1";
        String statementName = "stmt1";
        int paramFormatCount = 1;
        short[] paramFormats = {0}; // text format
        int paramCount = 1;
        int[] paramLengths = {5};
        byte[][] paramValues = {"hello".getBytes(StandardCharsets.UTF_8)};
        int resultFormatCount = 0;
        
        ByteBuffer buffer = ByteBuffer.allocate(
            portalName.length() + 1 +           // portal name + null
            statementName.length() + 1 +        // statement name + null
            2 +                               // param format count (short)
            paramFormatCount * 2 +            // param formats (shorts)
            4 +                               // param count (int)
            paramCount * 4 +                  // param lengths (ints)
            paramValues[0].length +          // param values
            4 +                               // result format count (int)
            0                                // result formats (none)
        );
        
        // Write portal name
        buffer.put(portalName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        
        // Write statement name
        buffer.put(statementName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        
        // Write parameter format count
        buffer.put((byte) (paramFormatCount >> 8));
        buffer.put((byte) paramFormatCount);
        
        // Write parameter formats
        for (short format : paramFormats) {
            buffer.put((byte) (format >> 8));
            buffer.put((byte) format);
        }
        
        // Write parameter count
        buffer.put((byte) (paramCount >> 24));
        buffer.put((byte) (paramCount >> 16));
        buffer.put((byte) (paramCount >> 8));
        buffer.put((byte) paramCount);
        
        // Write parameter lengths
        for (int length : paramLengths) {
            buffer.put((byte) (length >> 24));
            buffer.put((byte) (length >> 16));
            buffer.put((byte) (length >> 8));
            buffer.put((byte) length);
        }
        
        // Write parameter values
        for (byte[] value : paramValues) {
            buffer.put(value);
        }
        
        // Write result format count
        buffer.putInt(resultFormatCount);
        
        byte[] payload = buffer.array();
        
        // Verify the message structure
        assertEquals(portalName, readNullTerminatedString(payload, 0));
        assertEquals(statementName, readNullTerminatedString(payload, portalName.length() + 1));
        
        int paramFormatCountPos = portalName.length() + 1 + statementName.length() + 1;
        int actualParamFormatCount = ((payload[paramFormatCountPos] & 0xFF) << 8) | 
                                    (payload[paramFormatCountPos + 1] & 0xFF);
        assertEquals(paramFormatCount, actualParamFormatCount);
    }
    
    @Test
    void testExecuteMessageFormat() {
        // Test Execute message format: [portalName][maxRows]
        String portalName = "portal1";
        int maxRows = 0; // unlimited
        
        ByteBuffer buffer = ByteBuffer.allocate(
            portalName.length() + 1 + // portal name + null
            4                        // max rows (int)
        );
        
        // Write portal name
        buffer.put(portalName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        
        // Write max rows
        buffer.putInt(maxRows);
        
        byte[] payload = buffer.array();
        
        // Verify the message structure
        assertEquals(portalName, readNullTerminatedString(payload, 0));
        
        int maxRowsPos = portalName.length() + 1;
        int actualMaxRows = ((payload[maxRowsPos] & 0xFF) << 24) |
                           ((payload[maxRowsPos + 1] & 0xFF) << 16) |
                           ((payload[maxRowsPos + 2] & 0xFF) << 8) |
                           (payload[maxRowsPos + 3] & 0xFF);
        assertEquals(maxRows, actualMaxRows);
    }
    
    @Test
    void testMessageEncoding() {
        // Test that messages are properly encoded with correct headers
        String testQuery = "SELECT 1";
        byte[] payload = testQuery.getBytes(StandardCharsets.UTF_8);
        
        byte[] encoded = LibpqProtocol.encodeMessage((byte)'P', payload);
        
        // Verify header
        assertEquals('P', encoded[0]);
        
        // Verify length (big-endian)
        int length = ((encoded[1] & 0xFF) << 24) | 
                     ((encoded[2] & 0xFF) << 16) | 
                     ((encoded[3] & 0xFF) << 8) | 
                     (encoded[4] & 0xFF);
        
        assertEquals(payload.length + 4, length);
        
        // Verify payload
        assertEquals(testQuery, new String(encoded, 5, payload.length, StandardCharsets.UTF_8));
    }
    
    @Test
    void testMessageDecoding() {
        // Test message decoding from byte buffer
        String testQuery = "SELECT * FROM users";
        byte[] payload = testQuery.getBytes(StandardCharsets.UTF_8);
        
        // Create encoded message
        byte[] encoded = LibpqProtocol.encodeMessage((byte)'Q', payload);
        
        // Simulate decoding
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
        
        assertEquals(testQuery, new String(decodedPayload, StandardCharsets.UTF_8));
    }
    
    @Test
    void testExtendedQueryFlow() {
        // Test the complete extended query flow
        String statementName = "select_users";
        String query = "SELECT id, name FROM users WHERE id = $1";
        String portalName = "user_portal";
        
        // 1. Parse message
        byte[] parseMessage = createParseMessage(statementName, query);
        assertEquals('P', parseMessage[0]);
        
        // 2. Bind message
        byte[] bindMessage = createBindMessage(portalName, statementName);
        assertEquals('B', bindMessage[0]);
        
        // 3. Execute message
        byte[] executeMessage = createExecuteMessage(portalName);
        assertEquals('E', executeMessage[0]);
        
        // 4. Sync message
        byte[] syncMessage = LibpqProtocol.encodeMessage((byte)'S', new byte[0]);
        assertEquals('S', syncMessage[0]);
        
        // Verify all messages are properly formatted
        assertTrue(parseMessage.length > 4);
        assertTrue(bindMessage.length > 4);
        assertTrue(executeMessage.length > 4);
        assertTrue(syncMessage.length > 4);
    }
    
    private byte[] createParseMessage(String statementName, String query) {
        ByteBuffer buffer = ByteBuffer.allocate(
            statementName.length() + 1 +
            query.length() + 1 +
            2 // param count (0)
        );
        
        buffer.put(statementName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        buffer.put(query.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        buffer.putShort((short) 0); // no parameters
        
        byte[] payload = buffer.array();
        return LibpqProtocol.encodeMessage((byte)'P', payload);
    }
    
    private byte[] createBindMessage(String portalName, String statementName) {
        ByteBuffer buffer = ByteBuffer.allocate(
            portalName.length() + 1 +
            statementName.length() + 1 +
            2 + 0 + // param format count (0) + no formats
            4 + 0 + 0 // param count (0) + no lengths/values
        );
        
        buffer.put(portalName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        buffer.put(statementName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        buffer.put((byte) 0); buffer.put((byte) 0); // no parameter formats
        buffer.put((byte) 0); buffer.put((byte) 0); buffer.put((byte) 0); buffer.put((byte) 0); // no parameters
        
        byte[] payload = buffer.array();
        return LibpqProtocol.encodeMessage((byte)'B', payload);
    }
    
    private byte[] createExecuteMessage(String portalName) {
        ByteBuffer buffer = ByteBuffer.allocate(
            portalName.length() + 1 +
            4 // max rows (0)
        );
        
        buffer.put(portalName.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        buffer.put((byte) 0); buffer.put((byte) 0); buffer.put((byte) 0); buffer.put((byte) 0); // unlimited rows
        
        byte[] payload = buffer.array();
        return LibpqProtocol.encodeMessage((byte)'E', payload);
    }
    
    private String readNullTerminatedString(byte[] data, int offset) {
        int start = offset;
        while (offset < data.length && data[offset] != 0) {
            offset++;
        }
        return new String(data, start, offset - start, StandardCharsets.UTF_8);
    }
}