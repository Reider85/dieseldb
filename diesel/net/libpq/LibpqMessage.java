package diesel.net.libpq;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a decoded libpq protocol message.
 */
public class LibpqMessage {
    
    private final byte type;
    private final byte[] payload;
    
    public LibpqMessage(byte type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }
    
    public byte getType() {
        return type;
    }
    
    public byte[] getPayload() {
        return payload;
    }
    
    /**
     * Parses a string from the payload at the given offset.
     */
    public String parseString(int offset) {
        int start = offset;
        while (start < payload.length && payload[start] != 0) {
            start++;
        }
        
        return new String(payload, offset, start - offset, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    /**
     * Parses an integer from the payload at the given offset.
     */
    public int parseInt(int offset) {
        return (payload[offset] & 0xFF) << 24 |
               (payload[offset + 1] & 0xFF) << 16 |
               (payload[offset + 2] & 0xFF) << 8 |
               (payload[offset + 3] & 0xFF);
    }
    
    /**
     * Parses a short from the payload at the given offset.
     */
    public short parseShort(int offset) {
        return (short) ((payload[offset] & 0xFF) << 8 | (payload[offset + 1] & 0xFF));
    }
    
    /**
     * Parses a byte from the payload at the given offset.
     */
    public byte parseByte(int offset) {
        return payload[offset];
    }
    
    /**
     * Parses an authentication request message.
     */
    public AuthRequest parseAuthRequest() {
        int offset = 0;
        int authType = parseInt(offset);
        offset += 4;
        
        List<byte[]> parameters = new ArrayList<>();
        while (offset < payload.length) {
            int start = offset;
            while (start < payload.length && payload[start] != 0) {
                start++;
            }
            
            if (start > offset) {
                byte[] param = new byte[start - offset];
                System.arraycopy(payload, offset, param, 0, param.length);
                parameters.add(param);
            }
            
            offset = start + 1; // Skip null terminator
        }
        
        return new AuthRequest(authType, parameters.toArray(new byte[0][]));
    }
    
    /**
     * Parses an error message.
     */
    public ErrorResponse parseErrorResponse() {
        int offset = 0;
        List<ErrorField> fields = new ArrayList<>();
        
        while (offset < payload.length && payload[offset] != 0) {
            byte fieldType = payload[offset];
            offset++;
            
            int start = offset;
            while (start < payload.length && payload[start] != 0) {
                start++;
            }
            
            String value = new String(payload, offset, start - offset, java.nio.charset.StandardCharsets.UTF_8);
            fields.add(new ErrorField(fieldType, value));
            
            offset = start + 1; // Skip null terminator
        }
        
        return new ErrorResponse(fields);
    }
    
    /**
     * Parses a parameter status message.
     */
    public ParameterStatus parseParameterStatus() {
        int nullPos = 0;
        while (nullPos < payload.length && payload[nullPos] != 0) {
            nullPos++;
        }
        
        String name = new String(payload, 0, nullPos, java.nio.charset.StandardCharsets.UTF_8);
        String value = new String(payload, nullPos + 1, payload.length - nullPos - 1, java.nio.charset.StandardCharsets.UTF_8);
        
        return new ParameterStatus(name, value);
    }
    
    /**
     * Parses a row description message.
     */
    public FieldDescription[] parseRowDescription() {
        int offset = 0;
        int fieldCount = parseShort(offset);
        offset += 2;
        
        FieldDescription[] fields = new FieldDescription[fieldCount];
        
        for (int i = 0; i < fieldCount; i++) {
            int objectId = parseInt(offset);
            offset += 4;
            
            int nameEnd = offset;
            while (nameEnd < payload.length && payload[nameEnd] != 0) {
                nameEnd++;
            }
            String name = new String(payload, offset, nameEnd - offset, java.nio.charset.StandardCharsets.UTF_8);
            offset = nameEnd + 1;
            
            short tableId = parseShort(offset);
            offset += 2;
            
            short columnId = parseShort(offset);
            offset += 2;
            
            int fieldType = parseInt(offset);
            offset += 4;
            
            short typeSize = parseShort(offset);
            offset += 2;
            
            short typeModifier = parseShort(offset);
            offset += 2;
            
            short formatCode = parseShort(offset);
            offset += 2;
            
            fields[i] = new FieldDescription.Builder()
                .objectId(objectId)
                .name(name)
                .tableId(tableId)
                .columnId(columnId)
                .fieldType(fieldType)
                .typeSize(typeSize)
                .typeModifier(typeModifier)
                .formatCode(formatCode)
                .build();
        }
        
        return fields;
    }
    
    /**
     * Parses a data row message.
     */
    public Object[] parseDataRow() {
        int offset = 0;
        int columnCount = parseShort(offset);
        offset += 2;
        
        Object[] values = new Object[columnCount];
        
        for (int i = 0; i < columnCount; i++) {
            int valueLength = parseInt(offset);
            offset += 4;
            
            if (valueLength == -1) {
                values[i] = null;
            } else {
                byte[] valueBytes = new byte[valueLength];
                System.arraycopy(payload, offset, valueBytes, 0, valueLength);
                values[i] = valueBytes;
                offset += valueLength;
            }
        }
        
        return values;
    }
    
    /**
     * Authentication request details.
     */
    public static class AuthRequest {
        public final int authType;
        public final byte[][] parameters;
        
        public AuthRequest(int authType, byte[][] parameters) {
            this.authType = authType;
            this.parameters = parameters;
        }
    }
    
    /**
     * Error response field.
     */
    public static class ErrorField {
        public final byte fieldType;
        public final String value;
        
        public ErrorField(byte fieldType, String value) {
            this.fieldType = fieldType;
            this.value = value;
        }
    }
    
    /**
     * Error response.
     */
    public static class ErrorResponse {
        public final List<ErrorField> fields;
        
        public ErrorResponse(List<ErrorField> fields) {
            this.fields = fields;
        }
        
        public String getMessage() {
            for (ErrorField field : fields) {
                if (field.fieldType == 'M') { // Message
                    return field.value;
                }
            }
            return "Unknown error";
        }
    }
    
    /**
     * Parameter status.
     */
    public static class ParameterStatus {
        public final String name;
        public final String value;
        
        public ParameterStatus(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
    
    /**
     * Field description for row description.
     */
    public static class FieldDescription {
        public final int objectId;
        public final String name;
        public final short tableId;
        public final short columnId;
        public final int fieldType;
        public final short typeSize;
        public final short typeModifier;
        public final short formatCode;
        
        public FieldDescription(int objectId, String name, short tableId, short columnId, 
                               int fieldType, short typeSize, short typeModifier, short formatCode) {
            this.objectId = objectId;
            this.name = name;
            this.tableId = tableId;
            this.columnId = columnId;
            this.fieldType = fieldType;
            this.typeSize = typeSize;
            this.typeModifier = typeModifier;
            this.formatCode = formatCode;
        }
        
        /**
         * Builder for creating FieldDescription instances.
         */
        static class Builder {
            private int objectId;
            private String name;
            private short tableId;
            private short columnId;
            private int fieldType;
            private short typeSize;
            private short typeModifier;
            private short formatCode;
            
            Builder objectId(int objectId) {
                this.objectId = objectId;
                return this;
            }
            
            Builder name(String name) {
                this.name = name;
                return this;
            }
            
            Builder tableId(short tableId) {
                this.tableId = tableId;
                return this;
            }
            
            Builder columnId(short columnId) {
                this.columnId = columnId;
                return this;
            }
            
            Builder fieldType(int fieldType) {
                this.fieldType = fieldType;
                return this;
            }
            
            Builder typeSize(short typeSize) {
                this.typeSize = typeSize;
                return this;
            }
            
            Builder typeModifier(short typeModifier) {
                this.typeModifier = typeModifier;
                return this;
            }
            
            Builder formatCode(short formatCode) {
                this.formatCode = formatCode;
                return this;
            }
            
            FieldDescription build() {
                return new FieldDescription(objectId, name, tableId, columnId, fieldType, 
                                         typeSize, typeModifier, formatCode);
            }
        }
    }
}