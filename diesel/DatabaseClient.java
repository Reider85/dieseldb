package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
     * Opens the socket connection and the object streams to the server.
     *
     * @throws RuntimeException if the connection cannot be established
     */
    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            LOGGER.info("Connected to server at " + host + ":" + port);
        } catch (IOException e) {
            closeQuietly(in);
            closeQuietly(out);
            closeQuietly(socket);
            throw new DieselIOException("Connection failed: " + e.getMessage(), e);
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
            String normalizedQuery = query.trim();
            out.writeObject(new QueryMessage(normalizedQuery, transactionId));
            out.flush();
            Object result = in.readObject();
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
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Query execution failed: {}, Error: {}", query, e.getMessage());
            throw new DieselIOException("Query failed: " + e.getMessage(), e);
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
                out.writeObject("EXIT");
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