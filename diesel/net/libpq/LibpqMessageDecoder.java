package diesel.net.libpq;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes libpq protocol messages from DataInput.
 */
public class LibpqMessageDecoder {
    
    /**
     * Decodes a single message from DataInput.
     */
    public static LibpqMessage decodeMessage(DataInput in) throws IOException {
        // Read message type
        byte type = in.readByte();
        
        // Read message length (includes self)
        int length = in.readInt();
        
        if (length < 5) {
            throw new IOException("Invalid message length: " + length);
        }
        
        // Read payload
        byte[] payload = new byte[length - 4];
        in.readFully(payload);
        
        return new LibpqMessage(type, payload);
    }
    
    /**
     * Decodes multiple messages from DataInput until EOF or error.
     */
    public static List<LibpqMessage> decodeMessages(DataInput in) throws IOException {
        List<LibpqMessage> messages = new ArrayList<>();
        
        try {
            while (true) {
                LibpqMessage message = decodeMessage(in);
                messages.add(message);
            }
        } catch (IOException e) {
            // EOF or error, return what we have
            if (e.getMessage() != null && e.getMessage().contains("EOF")) {
                return messages;
            }
            throw e;
        }
    }
    
    /**
     * Decodes a startup message.
     */
    public static StartupMessage decodeStartupMessage(LibpqMessage message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read protocol version
        int protocolVersion = buffer.getInt();
        
        // Read parameters until null terminator
        List<String> parameters = new ArrayList<>();
        while (buffer.remaining() > 0) {
            int start = buffer.position();
            while (buffer.hasRemaining() && buffer.get() != 0) {
                // Skip bytes
            }
            int end = buffer.position();
            buffer.position(end + 1); // Skip null terminator
            
            if (start < end) {
                byte[] paramBytes = new byte[end - start];
                buffer.position(start);
                buffer.get(paramBytes);
                String param = new String(paramBytes, java.nio.charset.StandardCharsets.UTF_8);
                parameters.add(param);
            }
        }
        
        return new StartupMessage(protocolVersion, parameters);
    }
    
    /**
     * Decodes a parse message.
     */
    public static ParseMessage decodeParseMessage(LibpqMessage message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read prepared statement name
        String statementName = readNullTerminatedString(buffer);
        
        // Read query string
        String query = readNullTerminatedString(buffer);
        
        // Read parameter type count
        int paramCount = buffer.getShort() & 0xFFFF;
        int[] paramTypes = new int[paramCount];
        
        for (int i = 0; i < paramCount; i++) {
            paramTypes[i] = buffer.getInt();
        }
        
        return new ParseMessage(statementName, query, paramTypes);
    }
    
    /**
     * Decodes a bind message.
     */
    public static BindMessage decodeBindMessage(LibpqMessage message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read portal name
        String portalName = readNullTerminatedString(buffer);
        
        // Read prepared statement name
        String statementName = readNullTerminatedString(buffer);
        
        // Read parameter format codes
        short paramFormatCount = buffer.getShort();
        short[] paramFormats = new short[paramFormatCount];
        for (int i = 0; i < paramFormatCount; i++) {
            paramFormats[i] = buffer.getShort();
        }
        
        // Read parameter values
        short paramCount = buffer.getShort();
        Object[] paramValues = new Object[paramCount];
        for (int i = 0; i < paramCount; i++) {
            int paramLength = buffer.getInt();
            if (paramLength == -1) {
                paramValues[i] = null;
            } else {
                byte[] paramValue = new byte[paramLength];
                buffer.get(paramValue);
                paramValues[i] = paramValue;
            }
        }
        
        // Read result format codes
        short resultFormatCount = buffer.getShort();
        short[] resultFormats = new short[resultFormatCount];
        for (int i = 0; i < resultFormatCount; i++) {
            resultFormats[i] = buffer.getShort();
        }
        
        return new BindMessage(portalName, statementName, paramFormats, paramValues, resultFormats);
    }
    
    /**
     * Decodes an execute message.
     */
    public static ExecuteMessage decodeExecuteMessage(LibpqMessage message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read portal name
        String portalName = readNullTerminatedString(buffer);
        
        // Read max rows
        int maxRows = buffer.getInt();
        
        return new ExecuteMessage(portalName, maxRows);
    }
    
    /**
     * Decodes a sync message.
     */
    public static SyncMessage decodeSyncMessage(LibpqMessage message) {
        // Sync message has no payload
        return new SyncMessage();
    }
    
    /**
     * Decodes a close message.
     */
    public static CloseMessage decodeCloseMessage(LibpqMessage message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read close type
        byte closeType = buffer.get();
        
        // Read target name
        String targetName = readNullTerminatedString(buffer);
        
        return new CloseMessage(closeType, targetName);
    }
    
    /**
     * Decodes a flush message.
     */
    public static FlushMessage decodeFlushMessage(LibpqMessage message) {
        // Flush message has no payload
        return new FlushMessage();
    }
    
    /**
     * Reads a null-terminated string from ByteBuffer.
     */
    private static String readNullTerminatedString(ByteBuffer buffer) {
        int start = buffer.position();
        while (buffer.hasRemaining() && buffer.get() != 0) {
            // Skip bytes
        }
        int end = buffer.position();
        buffer.position(end + 1); // Skip null terminator
        
        if (start < end) {
            byte[] strBytes = new byte[end - start];
            buffer.position(start);
            buffer.get(strBytes);
            return new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return "";
    }
    
    /**
     * Startup message.
     */
    public static class StartupMessage {
        public final int protocolVersion;
        public final List<String> parameters;
        
        public StartupMessage(int protocolVersion, List<String> parameters) {
            this.protocolVersion = protocolVersion;
            this.parameters = parameters;
        }
    }
    
    /**
     * Parse message.
     */
    public static class ParseMessage {
        public final String statementName;
        public final String query;
        public final int[] paramTypes;
        
        public ParseMessage(String statementName, String query, int[] paramTypes) {
            this.statementName = statementName;
            this.query = query;
            this.paramTypes = paramTypes;
        }
    }
    
    /**
     * Bind message.
     */
    public static class BindMessage {
        public final String portalName;
        public final String statementName;
        public final short[] paramFormats;
        public final Object[] paramValues;
        public final short[] resultFormats;
        
        public BindMessage(String portalName, String statementName, short[] paramFormats, 
                          Object[] paramValues, short[] resultFormats) {
            this.portalName = portalName;
            this.statementName = statementName;
            this.paramFormats = paramFormats;
            this.paramValues = paramValues;
            this.resultFormats = resultFormats;
        }
    }
    
    /**
     * Execute message.
     */
    public static class ExecuteMessage {
        public final String portalName;
        public final int maxRows;
        
        public ExecuteMessage(String portalName, int maxRows) {
            this.portalName = portalName;
            this.maxRows = maxRows;
        }
    }
    
    /**
     * Sync message.
     */
    public static class SyncMessage {
        // No fields
    }
    
    /**
     * Close message.
     */
    public static class CloseMessage {
        public final byte closeType; // 'S' for statement, 'P' for portal
        public final String targetName;
        
        public CloseMessage(byte closeType, String targetName) {
            this.closeType = closeType;
            this.targetName = targetName;
        }
    }
    
    /**
     * Flush message.
     */
    public static class FlushMessage {
        // No fields
    }
}