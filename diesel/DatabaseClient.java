package diesel;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP client for a {@link DatabaseServer}: connects over a socket, sends SQL
 * queries as serialized {@link QueryMessage} objects and reads back the
 * results.
 *
 * <p>The client tracks the server-side transaction state: a "Transaction
 * started" response stores the transaction id and sends it with every
 * following query, while COMMIT/ROLLBACK reset it. Server error responses
 * (prefixed with {@code Error: }) are rethrown as {@link RuntimeException}.
 *
 * @see DatabaseServer
 * @see QueryMessage
 */
public class DatabaseClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);
    private final String host;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private UUID transactionId;

    /**
     * Creates a client for the given server endpoint. No connection is opened
     * until {@link #connect()} is called.
     *
     * @param host the server host name
     * @param port the server port
     */
    public DatabaseClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.transactionId = null;
    }

    /**
     * Connects to the server at the configured host and port, opening
     * the output and input streams and performing the compression handshake.
     */
    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            performHandshake();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to connect to " + host + ":" + port, e);
        }
    }

    /**
     * Performs compression handshake with server.
     * Reads server handshake message and stores agreed compression settings.
     */
    private void performHandshake() throws IOException, ClassNotFoundException {
        // Send client handshake
        out.writeObject(new CompressionHandshakeMessage(true, List.of("GZIP"), 6, 1024));
        out.flush();
        
        // Read server handshake response
        Object response = in.readObject();
        if (response instanceof CompressionHandshakeResponse serverResponse) {
            LOGGER.info("Received compression handshake response: {}", serverResponse);
            if (serverResponse.isCompressionEnabled()) {
                LOGGER.info("Server compression enabled with algorithm: {}, level: {}, threshold: {}",
                        serverResponse.getAgreedAlgorithm(),
                        serverResponse.getAgreedCompressionLevel(),
                        serverResponse.getAgreedThresholdBytes());
            } else {
                LOGGER.warn("Server compression disabled, using uncompressed responses");
            }
        } else {
            // Legacy server or unexpected response
            LOGGER.warn("Unexpected handshake response: {}. Using default compression settings.", response);
        }
    }

    /**
     * Sends a SQL query to the server and returns the result: a
     * {@code List<Map<String, Object>>} for SELECT, null for DML, or a status
     * String for transaction and DDL statements. The caller's transaction id
     * is attached to the message.
     *
     * @param query the SQL query to execute
     * @return the server result
     * @throws RuntimeException if the query fails on the server or the
     *                          communication breaks
     */
    public Object executeQuery(String query) {
        // Prompt 22 (java:S2259): out/in are only assigned by connect(); a
        // query before connect() would NPE on out.writeObject below and mask
        // the real cause, so fail with a clear message instead.
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            Object result;
            String normalizedQuery = query.trim();
            out.writeObject(new QueryMessage(normalizedQuery, transactionId));
            out.flush();
            // Read compression marker byte (0x00 = uncompressed, 0x01 = GZIP compressed)
            try {
                int marker = in.read();
                if (marker == 0x01) { // compressed
                    int dataLength = in.readInt();
                    byte[] compressedData = new byte[dataLength];
                    in.readFully(compressedData);
                    java.util.zip.GZIPInputStream gzis = new java.util.zip.GZIPInputStream(
                            new java.io.ByteArrayInputStream(compressedData));
                    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(gzis);
                    result = ois.readObject();
                    ois.close();
                } else { // uncompressed (marker 0x00 or any other value)
                    // Put the marker byte back by creating a wrapper - 
                    // instead we just read the object directly since marker was already consumed
                    // Actually, we need to re-read. Let's use a different approach.
                    // Read the length and the object
                    int dataLength = in.readInt();
                    byte[] uncompressedData = new byte[dataLength];
                    in.readFully(uncompressedData);
                    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                            new java.io.ByteArrayInputStream(uncompressedData));
                    result = ois.readObject();
                    ois.close();
                }
            } catch (IOException | ClassNotFoundException e) {
                throw new DieselIOException("Failed to read query result: " + e.getMessage(), e);
            }
            if (result instanceof String s && s.startsWith("Transaction started: ")) {
                transactionId = UUID.fromString(s.split(": ")[1]);
            } else if (result instanceof String s &&
                    (s.equals("Transaction committed") || s.equals("Transaction rolled back"))) {
                transactionId = null;
            }
            if (result instanceof String s && s.startsWith("Error: ")) {
                LOGGER.error("Server error for query '{}': {}", normalizedQuery, result);
                throw new DieselException(s);
            }
            LOGGER.info("Query executed: {}, Result: {}", normalizedQuery, result);
            return result;
        } catch (IOException e) {
            LOGGER.error("Query execution failed: {}, Error: {}", query, e.getMessage());
            throw new DieselIOException("Query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a batch of queries, allowing the server to execute independent queries in parallel.
     * 
     * @param queries the list of SQL queries to execute
     * @return list of query results in the same order as input queries
     * @throws RuntimeException if the batch fails on the server or the communication breaks
     */
    public List<Object> executeBatch(List<String> queries) {
        // Prompt 22 (java:S2259): out/in are only assigned by connect(); a
        // query before connect() would NPE on out.writeObject below and mask
        // the real cause, so fail with a clear message instead.
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            out.writeObject(new BatchQueryMessage(queries, transactionId));
            out.flush();
            Object result = in.readObject();
            
            // Handle transaction state changes from batch execution
            if (result instanceof String s && s.startsWith("Transaction started: ")) {
                transactionId = UUID.fromString(s.split(": ")[1]);
            } else if (result instanceof String s &&
                    (s.equals("Transaction committed") || s.equals("Transaction rolled back"))) {
                transactionId = null;
            }
            if (result instanceof String s && s.startsWith("Error: ")) {
                LOGGER.error("Server error for batch query: {}", result);
                throw new DieselException(s);
            }
            
            @SuppressWarnings("unchecked")
            List<Object> results = (List<Object>) result;
            LOGGER.info("Batch query executed: {} queries", queries.size());
            return results;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Batch query execution failed: {}, Error: {}", queries, e.getMessage());
            throw new DieselIOException("Batch query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Sends a {@link PrepareMessage} for the given SQL template to the server
     * and returns the server-assigned prepared-statement id (Prompt 79). The
     * statement is held on the server until {@link #closePrepared} reaps it.
     *
     * @param sqlTemplate the SQL template with {@code ?} placeholders
     * @return the server-assigned statement id
     */
    public String prepareStatement(String sqlTemplate) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new PrepareMessage(sqlTemplate, transactionId));
            out.flush();
            Object result = in.readObject();
            if (result instanceof String s && s.startsWith("Error: ")) {
                throw new DieselException(s);
            }
            return (String) result;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Prepare statement failed: {}, Error: {}", sqlTemplate, e.getMessage());
            throw new DieselIOException("Prepare statement failed: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a previously prepared statement on the server with the given
     * bound parameters, returning the server-serialized result (Prompt 79).
     *
     * @param statementId the statement id from {@link #prepareStatement}
     * @param params      the bound parameter values, in placeholder order
     * @return the query result (row list, null, or a status String)
     */
    public Object executePrepared(String statementId, List<Object> params) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new ExecutePreparedMessage(statementId, params, transactionId));
            out.flush();
            Object result = readResultFromStream();
            if (result instanceof String s && s.startsWith("Transaction started: ")) {
                transactionId = UUID.fromString(s.split(": ")[1]);
            } else if (result instanceof String s &&
                    (s.equals("Transaction committed") || s.equals("Transaction rolled back"))) {
                transactionId = null;
            }
            if (result instanceof String s && s.startsWith("Error: ")) {
                LOGGER.error("Server error for prepared statement '{}': {}", statementId, result);
                throw new DieselException(s);
            }
            return result;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Execute prepared statement failed: {}, Error: {}", statementId, e.getMessage());
            throw new DieselIOException("Execute prepared statement failed: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a prepared statement with varargs parameters.
     *
     * @param statementId the statement id from {@link #prepareStatement}
     * @param params      the bound parameter values, in placeholder order
     * @return the query result
     */
    public Object executePrepared(String statementId, Object... params) {
        List<Object> paramList = params == null ? Collections.emptyList() : Arrays.asList(params);
        return executePrepared(statementId, paramList);
    }

    /**
     * Closes a prepared statement on the server, releasing its cached parsed
     * AST.
     *
     * @param statementId the statement id from {@link #prepareStatement}
     */
    public void closePrepared(String statementId) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new ClosePreparedMessage(statementId, transactionId));
            out.flush();
            in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Close prepared statement failed: {}, Error: {}", statementId, e.getMessage());
            throw new DieselIOException("Close prepared statement failed: " + e.getMessage(), e);
        }
    }

    /**
     * Opens a server-side cursor (Prompt 81) over the given SELECT on the
     * server and returns its opaque id. The result can then be fetched in
     * paginated batches via {@link #fetchCursor(String)}.
     *
     * @param query     the SQL SELECT to run
     * @param fetchSize the number of rows each fetch returns (must be &gt; 0)
     * @return the server-assigned cursor id
     * @throws RuntimeException if the query fails on the server or the
     *                          communication breaks
     */
    public String openCursor(String query, int fetchSize) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new OpenCursorMessage(query, fetchSize, transactionId));
            out.flush();
            Object result = readResultFromStream();
            if (result instanceof String s && s.startsWith("Error: ")) {
                LOGGER.error("Server error for cursor open '{}': {}", query, result);
                throw new DieselException(s);
            }
            LOGGER.info("Opened cursor {} for query: {}, fetchSize={}", result, query, fetchSize);
            return (String) result;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Open cursor failed: {}, Error: {}", query, e.getMessage());
            throw new DieselIOException("Open cursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * Fetches the next batch of rows from an open server-side cursor
     * (Prompt 81). Returns an empty list when the cursor is exhausted.
     *
     * @param cursorId the cursor id from {@link #openCursor}
     * @return up to the cursor's fetch size rows, empty when exhausted
     * @throws RuntimeException if the cursor is unknown or the communication
     *                          breaks
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> fetchCursor(String cursorId) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new FetchCursorMessage(java.util.UUID.fromString(cursorId), transactionId));
            out.flush();
            Object result = readResultFromStream();
            if (result instanceof String s && s.startsWith("Error: ")) {
                LOGGER.error("Server error fetching cursor '{}': {}", cursorId, result);
                throw new DieselException(s);
            }
            return (List<Map<String, Object>>) result;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Fetch cursor failed: {}, Error: {}", cursorId, e.getMessage());
            throw new DieselIOException("Fetch cursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * Closes a server-side cursor (Prompt 81), releasing the server-held
     * iterator.
     *
     * @param cursorId the cursor id from {@link #openCursor}
     */
    public void closeCursor(String cursorId) {
        if (out == null || in == null) {
            throw new IllegalStateException("Client is not connected: call connect() first");
        }
        try {
            out.writeObject(new CloseCursorMessage(java.util.UUID.fromString(cursorId), transactionId));
            out.flush();
            in.readObject();
            LOGGER.info("Closed cursor {}", cursorId);
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Close cursor failed: {}, Error: {}", cursorId, e.getMessage());
            throw new DieselIOException("Close cursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * Reads a result object from the stream, handling the compression marker
     * byte protocol used by the server for query results.
     *
     * @return the deserialized result object
     * @throws IOException            on stream errors
     * @throws ClassNotFoundException if a deserialized type is unknown
     */
    private Object readResultFromStream() throws IOException, ClassNotFoundException {
        int marker = in.read();
        if (marker == 0x01) { // compressed
            int dataLength = in.readInt();
            byte[] compressedData = new byte[dataLength];
            in.readFully(compressedData);
            java.util.zip.GZIPInputStream gzis = new java.util.zip.GZIPInputStream(
                    new java.io.ByteArrayInputStream(compressedData));
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(gzis);
            Object result = ois.readObject();
            ois.close();
            return result;
        } else { // uncompressed (marker 0x00 or any other value)
            int dataLength = in.readInt();
            byte[] uncompressedData = new byte[dataLength];
            in.readFully(uncompressedData);
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                    new java.io.ByteArrayInputStream(uncompressedData));
            Object result = ois.readObject();
            ois.close();
            return result;
        }
    }

    /**
     * Closes the connection: an {@code EXIT} message is sent first, then the
     * streams and the socket are closed. Any active server-side transaction is
     * rolled back by the server.
     */
    public void disconnect() {
        try {
            if (out != null) {
                out.writeObject(ErrorMessages.EXIT_COMMAND);
                out.flush();
            }
        } catch (IOException e) {
            LOGGER.error("Error sending EXIT: {}", e.getMessage());
        }
        closeQuietly(in);
        closeQuietly(out);
        closeQuietly(socket);
        LOGGER.info("Disconnected from server");
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                LOGGER.error("Error closing resource: {}", e.getMessage());
            }
        }
    }

    /**
     * Demo entry point: connects to {@code localhost:3306} by default (host
     * and port may be given as arguments), sets the isolation level, runs a
     * small create/insert/update/select/commit scenario inside a transaction
     * and prints the SELECT result.
     *
     * @param args optional {@code host} and {@code port} arguments
     */
    public static void main(String[] args) {
        String host = "localhost";
        int port = 3306;
        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid port {}, using default {}", args[1], port);
            }
        }
        DatabaseClient client = new DatabaseClient(host, port);
        try {
            client.connect();

            // Try setting isolation level, but continue if unsupported
            try {
                client.executeQuery("SET TRANSACTION ISOLATION LEVEL READ_UNCOMMITTED");
            } catch (RuntimeException e) {
                LOGGER.warn("Failed to set isolation level: {}. Continuing with default.", e.getMessage());
            }

            client.executeQuery("BEGIN TRANSACTION");

            String createTable = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, INITIAL CHAR)";
            client.executeQuery(createTable);

            client.executeQuery("CREATE INDEX idx_age ON USERS(AGE)");
            client.executeQuery("CREATE HASH INDEX idx_name ON USERS(NAME)");

            String insertQuery1 = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, INITIAL) VALUES ('1', 'Alice', 25, TRUE, 'A')";
            client.executeQuery(insertQuery1);

            String insertQuery2 = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, INITIAL) VALUES ('2', 'Bob', 30, FALSE, 'B')";
            client.executeQuery(insertQuery2);

            String insertQuery3 = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, INITIAL) VALUES ('3', 'Alice', 28, TRUE, 'C')";
            client.executeQuery(insertQuery3);

            String updateQuery = "UPDATE USERS SET INITIAL = 'C' WHERE AGE < 30 OR ACTIVE = FALSE";
            client.executeQuery(updateQuery);

            String selectQuery = "SELECT ID, NAME, AGE FROM USERS WHERE NAME = 'Alice'";
            Object result = client.executeQuery(selectQuery);

            if (result instanceof List<?> list) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> rows = (List<Map<String, Object>>) list;
                LOGGER.info("Query Results:");
                for (Map<String, Object> row : rows) {
                    LOGGER.info(row.toString());
                }
            }

            client.executeQuery(SqlKeywords.COMMIT_TRANSACTION);
        } catch (Exception e) {
            LOGGER.error("Client error: {}", e.getMessage(), e);
            try {
                if (client.transactionId != null) {
                    client.executeQuery(SqlKeywords.ROLLBACK_TRANSACTION);
                }
            } catch (Exception rollbackEx) {
                LOGGER.error("Rollback failed: {}", rollbackEx.getMessage());
            }
        } finally {
            client.disconnect();
        }
    }
}