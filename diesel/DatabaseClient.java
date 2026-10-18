package diesel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.net.CompressionHandshakeHandler;
import diesel.net.InvalidMessageException;
import diesel.net.Message;
import diesel.net.MessageCodec;
import diesel.net.MessageType;
import diesel.net.WireProtocol;
import diesel.net.messages.BatchQueryMessage;
import diesel.net.messages.CloseCursorMessage;
import diesel.net.messages.CloseCursorResponseMessage;
import diesel.net.messages.ClosePreparedMessage;
import diesel.net.messages.ClosePreparedResponseMessage;
import diesel.net.messages.CompressionHandshakeMessage;
import diesel.net.messages.CompressionHandshakeResponseMessage;
import diesel.net.messages.ExecutePreparedMessage;
import diesel.net.messages.FetchCursorMessage;
import diesel.net.messages.MessageCodecRegistry;
import diesel.net.messages.OpenCursorMessage;
import diesel.net.messages.PrepareMessage;
import diesel.net.messages.PrepareResponseMessage;
import diesel.net.messages.QueryMessage;
import diesel.net.messages.QueryResultMessage;

/**
 * TCP client for a {@link DatabaseServer}: speaks the binary v2 wire protocol
 * ({@link WireProtocol} frames + explicit field codecs) with ZSTD/GZIP payload
 * compression negotiated by the handshake (prompt4.md #27-#29). Java Object
 * Serialization is gone from this path — it was an RCE vector.
 *
 * <p>The client tracks the server-side transaction state: a "Transaction
 * started" response stores the transaction id and sends it with every
 * following query, while COMMIT/ROLLBACK reset it. Server error responses
 * (prefixed with {@code Error: }) are rethrown as {@link DieselException}.
 *
 * <p>The public API and the shapes it returns are unchanged from the v1 era:
 * SELECT comes back as {@code List<Map<String,Object>>}, transaction / DDL /
 * DML status messages come back as {@code String}, and {@code null} for DML
 * that produces no value.
 *
 * @see DatabaseServer
 * @see WireProtocol
 */
public class DatabaseClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);

    /** Algorithms this client can decode, most preferred first. */
    private static final List<String> SUPPORTED_ALGORITHMS = List.of("ZSTD", "GZIP");

    /** Compression level asked for in the handshake. */
    private static final int PREFERRED_LEVEL = 6;

    /**
     * Payload threshold asked for in the handshake; the server may lower or
     * raise it (and switches compression off entirely with a threshold of 0).
     */
    private static final int PREFERRED_THRESHOLD = 4096;

    private final String host;
    private final int port;
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private UUID transactionId;
    private CompressionHandshakeHandler compressionHandler;

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
     * Connects to the server, opens the v2 streams and runs the compression
     * handshake.
     */
    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            performHandshake();
        } catch (IOException e) {
            throw new QuerySyntaxException("", "Failed to connect to " + host + ":" + port);
        }
    }

    /**
     * Advertises the supported algorithms and stores what the server picked.
     *
     * @throws IOException if the server refuses the handshake or answers with
     *                     something that is not a v2 handshake response
     */
    private void performHandshake() throws IOException {
        CompressionHandshakeMessage handshake = new CompressionHandshakeMessage(
                true, SUPPORTED_ALGORITHMS, PREFERRED_LEVEL, PREFERRED_THRESHOLD, null);
        writeMessage(handshake);

        Message response = readMessage();
        if (!(response instanceof CompressionHandshakeResponseMessage handshakeResponse)) {
            throw new InvalidMessageException("Expected a compression handshake response, got "
                    + response.getType());
        }
        compressionHandler = new CompressionHandshakeHandler(
                handshakeResponse.getChosenAlgorithm(),
                handshakeResponse.getActualCompressionLevel(),
                handshakeResponse.getActualCompressionThresholdBytes());
        LOGGER.info("Negotiated compression: algorithm={}, level={}, threshold={} bytes",
                handshakeResponse.getChosenAlgorithm(),
                handshakeResponse.getActualCompressionLevel(),
                handshakeResponse.getActualCompressionThresholdBytes());
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
        ensureConnected();
        try {
            String normalizedQuery = query.trim();
            writeMessage(new QueryMessage(normalizedQuery, transactionId));
            return readResult();
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
        ensureConnected();
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            writeMessage(new BatchQueryMessage(queries, transactionId));
            Object result = readResult();
            @SuppressWarnings("unchecked")
            List<Object> results = (List<Object>) result;
            LOGGER.info("Batch query executed: {} queries", queries.size());
            return results;
        } catch (IOException e) {
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
        ensureConnected();
        try {
            writeMessage(new PrepareMessage(sqlTemplate, transactionId));
            Message response = readMessage();
            if (response instanceof PrepareResponseMessage prepareResponse) {
                return prepareResponse.getPreparedId();
            }
            if (response instanceof QueryResultMessage result) {
                Object value = toJavaResult(result);
                if (value instanceof String error && error.startsWith(ErrorMessages.ERROR_PREFIX)) {
                    throw new DieselException(error);
                }
            }
            throw new InvalidMessageException("Unexpected prepare response: " + response.getType());
        } catch (IOException e) {
            LOGGER.error("Prepare statement failed: {}, Error: {}", sqlTemplate, e.getMessage());
            throw new DieselIOException("Prepare statement failed: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a previously prepared statement on the server with the given
     * bound parameters, returning the server result (Prompt 79).
     *
     * @param statementId the statement id from {@link #prepareStatement}
     * @param params      the bound parameter values, in placeholder order
     * @return the query result (row list, null, or a status String)
     */
    public Object executePrepared(String statementId, List<Object> params) {
        ensureConnected();
        try {
            writeMessage(new ExecutePreparedMessage(statementId, params, transactionId));
            return readResult();
        } catch (IOException e) {
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
        ensureConnected();
        try {
            writeMessage(new ClosePreparedMessage(statementId, transactionId));
            Message response = readMessage();
            if (!(response instanceof ClosePreparedResponseMessage)) {
                throw new InvalidMessageException("Unexpected close-prepared response: " + response.getType());
            }
        } catch (IOException e) {
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
        ensureConnected();
        try {
            writeMessage(new OpenCursorMessage(query, fetchSize, transactionId));
            Object result = readResult();
            LOGGER.info("Opened cursor {} for query: {}, fetchSize={}", result, query, fetchSize);
            return (String) result;
        } catch (IOException e) {
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
        ensureConnected();
        try {
            writeMessage(new FetchCursorMessage(UUID.fromString(cursorId), transactionId));
            Object result = readResult();
            return (List<Map<String, Object>>) result;
        } catch (IOException e) {
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
        ensureConnected();
        try {
            writeMessage(new CloseCursorMessage(UUID.fromString(cursorId), transactionId));
            Message response = readMessage();
            if (!(response instanceof CloseCursorResponseMessage)) {
                throw new InvalidMessageException("Unexpected close-cursor response: " + response.getType());
            }
            LOGGER.info("Closed cursor {}", cursorId);
        } catch (IOException e) {
            LOGGER.error("Close cursor failed: {}, Error: {}", cursorId, e.getMessage());
            throw new DieselIOException("Close cursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * Closes the connection: streams and socket are closed (the v2 protocol
     * has no farewell message — the server notices the EOF). Any active
     * server-side transaction is rolled back by the server.
     */
    public void disconnect() {
        closeQuietly(in);
        closeQuietly(out);
        closeQuietly(socket);
        LOGGER.info("Disconnected from server");
    }

    /**
     * Reads one framed message from the server, transparently decompressing a
     * payload that arrived with {@link WireProtocol#COMPRESSED_FLAG} set.
     *
     * @return the decoded message
     * @throws IOException on framing, decompression or codec failure
     */
    private Message readMessage() throws IOException {
        WireProtocol.Frame frame = WireProtocol.decodeFrame(in);
        byte[] payload = frame.payload();
        if (frame.compressed()) {
            if (compressionHandler == null) {
                throw new InvalidMessageException(
                        "Compressed frame received before the compression handshake");
            }
            payload = compressionHandler.decompress(payload);
        }
        MessageCodec codec = MessageCodecRegistry.getCodec(frame.type());
        Message message = codec.decode(new DataInputStream(new ByteArrayInputStream(payload)));
        if (message.getType() != frame.type()) {
            throw new InvalidMessageException("Payload type tag " + message.getType()
                    + " does not match frame header type " + frame.type());
        }
        return message;
    }

    /**
     * Encodes one message, compresses it when the negotiated handler asks for
     * it and the result shrinks, then writes it as a single frame.
     *
     * @param message the message to send
     * @throws IOException on codec or stream failure
     */
    private void writeMessage(Message message) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
        try (DataOutputStream payloadOut = new DataOutputStream(buffer)) {
            MessageCodecRegistry.getCodec(message.getType()).encode(message, payloadOut);
        }
        byte[] payload = buffer.toByteArray();
        byte[] wirePayload = payload;
        boolean compressed = false;
        if (compressionHandler != null && compressionHandler.shouldCompress(payload.length)) {
            byte[] candidate = compressionHandler.compress(payload);
            if (candidate.length < payload.length) {
                wirePayload = candidate;
                compressed = true;
            }
        }
        WireProtocol.encodeFrame(message.getType(), wirePayload, compressed, out);
        out.flush();
    }

    /**
     * Reads the next query result and applies the protocol-level rules the
     * caller relies on: transaction bookkeeping and rethrowing server errors.
     *
     * @return the reconstructed result object
     * @throws IOException if the peer did not answer with a query result
     */
    private Object readResult() throws IOException {
        Message message = readMessage();
        if (!(message instanceof QueryResultMessage result)) {
            throw new InvalidMessageException("Expected a query result, got " + message.getType());
        }
        Object value = toJavaResult(result);
        if (value instanceof String s && s.startsWith(ErrorMessages.TRANSACTION_STARTED)) {
            transactionId = UUID.fromString(s.split(": ")[1]);
        } else if (value instanceof String s
                && (s.equals(ErrorMessages.TRANSACTION_COMMITTED) || s.equals(ErrorMessages.TRANSACTION_ROLLED_BACK))) {
            transactionId = null;
        }
        if (value instanceof String s && s.startsWith(ErrorMessages.ERROR_PREFIX)) {
            LOGGER.error("Server error: {}", s);
            throw new DieselException(s);
        }
        LOGGER.debug("Result: {}", value);
        return value;
    }

    /**
     * Rebuilds the Java object the v1 protocol used to hand over verbatim.
     *
     * <p>{@link QueryResultMessage#STATUS_SCALAR} and
     * {@link QueryResultMessage#STATUS_ERROR} collapse to their single value
     * (a status String, {@code null} for plain DML, or the error message);
     * {@link QueryResultMessage#STATUS_ROWS} rebuilds the row list keyed by
     * column name.
     *
     * @param result the decoded result message
     * @return the reconstructed object
     */
    public static Object toJavaResult(QueryResultMessage result) {
        List<String> columns = result.getColumnNames();
        List<List<Object>> rows = result.getRows();
        String status = result.getStatus();

        if (QueryResultMessage.STATUS_SCALAR.equals(status)
                || QueryResultMessage.STATUS_ERROR.equals(status)) {
            if (rows.isEmpty() || rows.get(0).isEmpty()) {
                return null;
            }
            return rows.get(0).get(0);
        }

        List<Map<String, Object>> list = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            Map<String, Object> record = new LinkedHashMap<>(columns.size());
            for (int i = 0; i < columns.size() && i < row.size(); i++) {
                record.put(columns.get(i), row.get(i));
            }
            list.add(record);
        }
        return list;
    }

    /** Prompt 22 (java:S2259): fail clearly instead of NPE-ing on a dead socket. */
    private void ensureConnected() {
        if (out == null || in == null) {
            throw new IllegalStateException(ErrorMessages.ERROR_NOT_CONNECTED);
        }
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
        String localHost = "localhost";
        int localPort = 3306;
        if (args.length > 0) {
            localHost = args[0];
        }
        if (args.length > 1) {
            try {
                localPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid port {}, using default {}", args[1], localPort);
            }
        }
        DatabaseClient client = new DatabaseClient(localHost, localPort);
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
