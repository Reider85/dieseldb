package diesel;

import diesel.net.libpq.LibpqProtocol;
import diesel.net.libpq.LibpqMessage;
import diesel.net.libpq.ScramSha256;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Handles individual client connections for libpq protocol.
 */
public class LibpqClientHandler implements Runnable {
    
    private final Socket clientSocket;
    private final Database database;
    private final String authMethod;
    private final InputStream input;
    private final OutputStream output;
    
    // Client state
    private boolean authenticated = false;
    private String username = null;
    private String currentDatabase = null;
    private Map<String, Object> sessionParams = new HashMap<>();
    
    public LibpqClientHandler(Socket clientSocket, Database database, String authMethod) throws IOException {
        this.clientSocket = clientSocket;
        this.database = database;
        this.authMethod = authMethod;
        this.input = clientSocket.getInputStream();
        this.output = clientSocket.getOutputStream();
    }
    
    @Override
    public void run() {
        try {
            handleConnection();
        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
    
    private void handleConnection() throws IOException {
        // Handle startup message
        LibpqMessage startupMessage = readMessage();
        if (startupMessage == null) {
            sendErrorResponse("Missing startup message");
            return;
        }
        
        if (startupMessage.getType() != LibpqProtocol.STARTUP_MESSAGE) {
            sendErrorResponse("Expected startup message");
            return;
        }
        
        // Parse startup message
        String[] startupParams = parseStartupMessage(startupMessage);
        username = startupParams[0];
        currentDatabase = startupParams[1];
        
        // Authenticate user
        if (!authenticateUser()) {
            sendErrorResponse("Authentication failed");
            return;
        }
        
        // Send authentication success
        sendAuthenticationSuccess();
        
        // Handle subsequent messages
        while (true) {
            LibpqMessage message = readMessage();
            if (message == null) {
                break;
            }
            
            handleMessage(message);
        }
    }
    
    private String[] parseStartupMessage(LibpqMessage message) {
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Skip protocol version
        buffer.getInt();
        
        // Parse parameters
        String username = "postgres";
        String database = "postgres";
        
        while (buffer.hasRemaining()) {
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
                String param = new String(paramBytes, StandardCharsets.UTF_8);
                
                if (param.equals("user")) {
                    if (buffer.hasRemaining()) {
                        start = buffer.position();
                        while (buffer.hasRemaining() && buffer.get() != 0) {
                            // Skip bytes
                        }
                        end = buffer.position();
                        buffer.position(end + 1);
                        
                        if (start < end) {
                            byte[] valueBytes = new byte[end - start];
                            buffer.position(start);
                            buffer.get(valueBytes);
                            username = new String(valueBytes, StandardCharsets.UTF_8);
                        }
                    }
                } else if (param.equals("database")) {
                    if (buffer.hasRemaining()) {
                        start = buffer.position();
                        while (buffer.hasRemaining() && buffer.get() != 0) {
                            // Skip bytes
                        }
                        end = buffer.position();
                        buffer.position(end + 1);
                        
                        if (start < end) {
                            byte[] valueBytes = new byte[end - start];
                            buffer.position(start);
                            buffer.get(valueBytes);
                            database = new String(valueBytes, StandardCharsets.UTF_8);
                        }
                    }
                }
            }
        }
        
        return new String[]{username, database};
    }
    
    private boolean authenticateUser() throws IOException {
        if ("trust".equals(authMethod)) {
            return true; // Trust authentication - always succeed
        }
        
        if ("scram-sha-256".equals(authMethod)) {
            return authenticateScram();
        }
        
        // For now, assume password authentication
        return authenticatePassword();
    }
    
    private boolean authenticateScram() throws IOException {
        try {
            // Generate challenge
            String nonce = ScramSha256.generateNonce();
            byte[] salt = ScramSha256.generateSalt();
            String challenge = "r=" + nonce + ",s=" + Base64.getEncoder().encodeToString(salt) + ",i=4096";
            
            // Send authentication request
            byte[] authRequest = LibpqProtocol.encodeAuthRequest(LibpqProtocol.AUTH_SASL, challenge.getBytes(StandardCharsets.UTF_8));
            output.write(authRequest);
            
            // Read client response
            LibpqMessage clientMessage = readMessage();
            if (clientMessage == null || clientMessage.getType() != 'p') {
                return false;
            }
            
            String clientResponse = new String(clientMessage.getPayload(), StandardCharsets.UTF_8);
            String clientFirstMessage = ScramSha256.createClientFirstMessage(username, nonce);
            ScramSha256.ServerFirstMessage serverFirstMessage = ScramSha256.parseServerFirstMessage(clientResponse, nonce);
            
            // In a real implementation, we would look up the user's stored password hash
            // For now, we'll use a hardcoded password for testing
            String storedPassword = "testpassword";
            String storedHash = ScramSha256.hashPassword(storedPassword, salt);
            
            // Verify client proof
            String clientFinalMessage = ScramSha256.createClientFinalMessage(clientFirstMessage, serverFirstMessage, storedPassword);
            ScramSha256.ServerFinalMessage serverFinalMessage = ScramSha256.parseServerFinalMessage("v=signature", clientFinalMessage);
            
            if (!ScramSha256.verifyServerSignature(serverFinalMessage, clientFirstMessage, serverFirstMessage, clientFinalMessage, storedPassword)) {
                return false;
            }
            
            // Send server final message
            String serverFinal = "c=biws,r=" + serverFirstMessage.nonce + ",v=signature";
            byte[] serverFinalResponse = LibpqProtocol.encodeMessage((byte)'c', serverFinal.getBytes(StandardCharsets.UTF_8));
            output.write(serverFinalResponse);
            
            authenticated = true;
            return true;
            
        } catch (Exception e) {
            System.err.println("SCRAM authentication error: " + e.getMessage());
            return false;
        }
    }
    
    private boolean authenticatePassword() throws IOException {
        // For now, just accept any username/password combination
        // In a real implementation, we would validate against user credentials
        return true;
    }
    
    private void sendAuthenticationSuccess() throws IOException {
        byte[] response = LibpqProtocol.encodeMessage((byte)'R', new byte[]{0, 0, 0, 0}); // AuthOK
        output.write(response);
        
        // Send parameter status
        sendParameterStatus("server_version", "13.0");
        sendParameterStatus("client_encoding", "UTF8");
        sendParameterStatus("integer_datetimes", "on");
        sendParameterStatus("DateStyle", "ISO, MDY");
        sendParameterStatus("timezone", "UTC");
        
        // Send ready for query
        sendReadyForQuery(LibpqProtocol.IDLE);
    }
    
    private void sendParameterStatus(String name, String value) throws IOException {
        String param = name + "\0" + value + "\0";
        byte[] response = LibpqProtocol.encodeMessage(LibpqProtocol.PARAMETER_STATUS, param.getBytes(StandardCharsets.UTF_8));
        output.write(response);
    }
    
    private void sendReadyForQuery(byte transactionStatus) throws IOException {
        byte[] response = LibpqProtocol.encodeReadyForQuery(transactionStatus);
        output.write(response);
    }
    
    private void sendErrorResponse(String message) throws IOException {
        String error = "SERROR\0C00000\0M" + message + "\0\0";
        byte[] response = LibpqProtocol.encodeMessage(LibpqProtocol.ERROR_RESPONSE, error.getBytes(StandardCharsets.UTF_8));
        output.write(response);
    }
    
    private void handleMessage(LibpqMessage message) throws IOException {
        switch (message.getType()) {
            case 'Q': // Simple query
                handleSimpleQuery(message);
                break;
            case 'P': // Parse
                handleParse(message);
                break;
            case 'B': // Bind
                handleBind(message);
                break;
            case 'E': // Execute
                handleExecute(message);
                break;
            case 'S': // Sync
                sendReadyForQuery(LibpqProtocol.IDLE);
                break;
            case 'H': // Flush
                // No action needed
                break;
            case 'X': // Terminate
                clientSocket.close();
                break;
            default:
                sendErrorResponse("Unknown message type: " + (char) message.getType());
        }
    }
    
    private void handleSimpleQuery(LibpqMessage message) throws IOException {
        String query = new String(message.getPayload(), StandardCharsets.UTF_8).trim();
        
        // Handle psql meta-commands
        if (query.startsWith("\\")) {
            handlePsqlMetaCommand(query);
            return;
        }
        
        // Execute the query
        try {
            Object result = database.executeQuery(query, UUID.randomUUID());
            
            // Send row description if it's a SELECT query
            if (result instanceof java.util.List) {
                java.util.List<?> rows = (java.util.List<?>) result;
                if (!rows.isEmpty()) {
                    LibpqMessage.FieldDescription[] fields = createFieldDescriptions((Object) rows);
                    byte[] rowDesc = LibpqProtocol.encodeRowDescription(fields);
                    output.write(rowDesc);
                    
                    // Send rows
                    for (Object row : rows) {
                        if (row instanceof Object[]) {
                            sendDataRow((Object[]) row);
                        }
                    }
                }
            }
            
            // Send command complete
            String tag = getCommandCompleteTag(query);
            byte[] complete = LibpqProtocol.encodeCommandComplete(tag);
            output.write(complete);
            
        } catch (Exception e) {
            sendErrorResponse("Query execution error: " + e.getMessage());
        }
        
        sendReadyForQuery(LibpqProtocol.IDLE);
    }
    
    private void handleParse(LibpqMessage message) throws IOException {
        // Parse message format: [statementName][null][query][null][paramCount][paramTypes...]
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read statement name
        String statementName = readNullTerminatedString(buffer);
        
        // Read query
        String query = readNullTerminatedString(buffer);
        
        // Read parameter count and types
        int paramCount = buffer.getShort() & 0xFFFF;
        int[] paramTypes = new int[paramCount];
        for (int i = 0; i < paramCount; i++) {
            paramTypes[i] = buffer.getInt();
        }
        
        // Store the prepared statement (in a real implementation)
        // For now, just acknowledge
        byte[] response = LibpqProtocol.encodeMessage((byte)'1', new byte[0]); // ParseComplete
        output.write(response);
    }
    
    private void handleBind(LibpqMessage message) throws IOException {
        // Bind message format: [portalName][null][statementName][null][paramFormatCount][paramFormats...][paramCount][paramLengths][paramValues...][resultFormatCount][resultFormats...]
        // For now, just acknowledge
        byte[] response = LibpqProtocol.encodeMessage((byte)'2', new byte[0]); // BindComplete
        output.write(response);
    }
    
    private void handleExecute(LibpqMessage message) throws IOException {
        // Execute message format: [portalName][maxRows]
        ByteBuffer buffer = ByteBuffer.wrap(message.getPayload());
        
        // Read portal name
        String portalName = readNullTerminatedString(buffer);
        
        // Read max rows
        int maxRows = buffer.getInt();
        
        // Execute the query (in a real implementation, this would use the bound portal)
        try {
            Object result = database.executeQuery("SELECT 1", UUID.randomUUID()); // Placeholder
            
            // Send row description
            LibpqMessage.FieldDescription[] fields = createFieldDescriptions(result instanceof java.util.List ? (Object) (java.util.List<?>) result : (Object) java.util.Collections.emptyList());
            byte[] rowDesc = LibpqProtocol.encodeRowDescription(fields);
            output.write(rowDesc);
            
            // Send rows
            if (result instanceof java.util.List) {
                java.util.List<?> rows = (java.util.List<?>) result;
                for (Object row : rows) {
                    if (row instanceof Object[]) {
                        sendDataRow((Object[]) row);
                    }
                }
            }
            
            // Send command complete
            byte[] complete = LibpqProtocol.encodeCommandComplete("SELECT 1");
            output.write(complete);
            
        } catch (Exception e) {
            sendErrorResponse("Query execution error: " + e.getMessage());
        }
        
        sendReadyForQuery(LibpqProtocol.IDLE);
    }
    
    private void handlePsqlMetaCommand(String command) throws IOException {
        if (command.equals("\\l")) {
            // List databases
            handleListDatabases();
        } else if (command.startsWith("\\dt")) {
            // List tables
            handleListTables();
        } else if (command.startsWith("\\d")) {
            // Describe table
            handleDescribeTable(command);
        } else if (command.equals("\\q")) {
            // Quit
            clientSocket.close();
        } else {
            sendErrorResponse("Unknown psql meta-command: " + command);
        }
        
        sendReadyForQuery(LibpqProtocol.IDLE);
    }
    
    private void handleListDatabases() throws IOException {
        // Send row description
        LibpqMessage.FieldDescription[] fields = {
            new LibpqMessage.FieldDescription(0, "Database", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Owner", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Encoding", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Collate", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Ctype", (short)0, (short)0, 0, (short)255, (short)0, (short)0)
        };
        
        byte[] rowDesc = LibpqProtocol.encodeRowDescription(fields);
        output.write(rowDesc);
        
        // Send sample databases
        sendDataRow(new Object[]{"postgres", "postgres", "UTF8", "en_US.UTF-8", "en_US.UTF-8"});
        sendDataRow(new Object[]{"template1", "postgres", "UTF8", "en_US.UTF-8", "en_US.UTF-8"});
        sendDataRow(new Object[]{"template0", "postgres", "UTF8", "en_US.UTF-8", "en_US.UTF-8"});
        
        // Send command complete
        byte[] complete = LibpqProtocol.encodeCommandComplete("SELECT 3");
        output.write(complete);
    }
    
    private void handleListTables() throws IOException {
        // Send row description
        LibpqMessage.FieldDescription[] fields = {
            new LibpqMessage.FieldDescription(0, "Schema", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Name", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Type", (short)0, (short)0, 0, (short)255, (short)0, (short)0)
        };
        
        byte[] rowDesc = LibpqProtocol.encodeRowDescription(fields);
        output.write(rowDesc);
        
        // Send sample tables
        sendDataRow(new Object[]{"public", "users", "table"});
        sendDataRow(new Object[]{"public", "orders", "table"});
        sendDataRow(new Object[]{"public", "products", "table"});
        
        // Send command complete
        byte[] complete = LibpqProtocol.encodeCommandComplete("SELECT 3");
        output.write(complete);
    }
    
    private void handleDescribeTable(String command) throws IOException {
        // Extract table name from command
        String tableName = command.substring(2).trim();
        if (tableName.startsWith("+")) {
            tableName = tableName.substring(1);
        }
        tableName = tableName.replaceAll("\\s+", "");
        
        // Send row description
        LibpqMessage.FieldDescription[] fields = {
            new LibpqMessage.FieldDescription(0, "Column", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Type", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Collation", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Nullable", (short)0, (short)0, 0, (short)255, (short)0, (short)0),
            new LibpqMessage.FieldDescription(0, "Default", (short)0, (short)0, 0, (short)255, (short)0, (short)0)
        };
        
        byte[] rowDesc = LibpqProtocol.encodeRowDescription(fields);
        output.write(rowDesc);
        
        // Send sample columns
        sendDataRow(new Object[]{"id", "integer", "", "NO", "nextval('users_id_seq'::regclass)"});
        sendDataRow(new Object[]{"name", "character varying(255)", "", "NO", null});
        sendDataRow(new Object[]{"email", "character varying(255)", "en_US.UTF-8", "YES", null});
        sendDataRow(new Object[]{"created_at", "timestamp without time zone", "", "NO", "now()"});
        
        // Send command complete
        byte[] complete = LibpqProtocol.encodeCommandComplete("SELECT 4");
        output.write(complete);
    }
    
    private void sendDataRow(Object[] row) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4); // Start with length field
        
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
        
        // Prepare final message
        byte[] payload = new byte[buffer.position()];
        buffer.flip();
        buffer.get(payload);
        
        byte[] message = LibpqProtocol.encodeMessage(LibpqProtocol.DATA_ROW, payload);
        output.write(message);
    }
    
    private String readNullTerminatedString(ByteBuffer buffer) {
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
            return new String(strBytes, StandardCharsets.UTF_8);
        }
        return "";
    }
    
    private LibpqMessage.FieldDescription[] createFieldDescriptions(Object result) {
        // In a real implementation, this would extract field information from the result
        // For now, return a placeholder
        return new LibpqMessage.FieldDescription[]{
            new LibpqMessage.FieldDescription(0, "column1", (short)0, (short)0, 0, (short)255, (short)0, (short)0)
        };
    }
    
    private String getCommandCompleteTag(String query) {
        String normalized = query.trim().toUpperCase();
        if (normalized.startsWith("SELECT")) {
            return "SELECT " + 1; // 1 row affected
        } else if (normalized.startsWith("INSERT")) {
            return "INSERT 0 " + 1; // 1 row inserted
        } else if (normalized.startsWith("UPDATE")) {
            return "UPDATE " + 1; // 1 row updated
        } else if (normalized.startsWith("DELETE")) {
            return "DELETE " + 1; // 1 row deleted
        } else {
            return "OK"; // Generic success
        }
    }
    
    private LibpqMessage readMessage() throws IOException {
        // Read message header
        byte[] header = new byte[5];
        int bytesRead = input.read(header);
        
        if (bytesRead == -1) {
            return null; // EOF
        }
        
        if (bytesRead < 5) {
            throw new IOException("Incomplete message header");
        }
        
        // Parse header
        byte type = header[0];
        int length = ((header[1] & 0xFF) << 24) | 
                     ((header[2] & 0xFF) << 16) | 
                     ((header[3] & 0xFF) << 8) | 
                     (header[4] & 0xFF);
        
        if (length < 5) {
            throw new IOException("Invalid message length: " + length);
        }
        
        // Read payload
        int payloadLength = length - 4;
        byte[] payload = new byte[payloadLength];
        bytesRead = input.read(payload);
        
        if (bytesRead != payloadLength) {
            throw new IOException("Incomplete message payload");
        }
        
        return new LibpqMessage(type, payload);
    }
}