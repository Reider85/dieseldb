package diesel;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Server side of the deprecated v1 protocol: plain Java Object Serialization
 * over a socket (prompt4.md #29).
 *
 * <p>The adapter listens on the separate {@code legacy.port} (default 5440) so
 * that pre-v2 clients keep working while the main port rejects Java
 * serialization outright. It reproduces the pre-prompt4#28 {@code ClientHandler}
 * byte for byte:
 *
 * <ul>
 *   <li>results travel as {@code [marker: 0x00|0x01][length: i32][bytes]} where
 *       {@code 0x01} means the bytes are GZIP compressed Java-serialized
 *       objects;</li>
 *   <li>prepare / close answers travel as bare serialized objects (no marker
 *       frame);</li>
 *   <li>the session opens with an optional
 *       {@link CompressionHandshakeMessage} answered by a
 *       {@link CompressionHandshakeResponse}.</li>
 * </ul>
 *
 * <p>This path is an acknowledged RCE vector (deserialization gadgets) and is
 * scheduled for removal in Phase 2 — see {@code KNOWN_LIMITATIONS.md}.
 */
final class LegacyProtocolAdapter {

    private static final Logger LOGGER = Logger.getLogger(LegacyProtocolAdapter.class.getName());

    /** Algorithm advertised by the legacy protocol (the only one it ever used). */
    private static final String DEFAULT_ALGORITHM = "GZIP";

    private static int defaultCompressionThreshold = 4096;
    private static int defaultCompressionLevel = 6;

    static {
        defaultCompressionThreshold = ConfigLoader.getInt("compression.threshold.bytes", defaultCompressionThreshold);
        defaultCompressionLevel = ConfigLoader.getInt("compression.level", defaultCompressionLevel);
    }

    /** Session-scoped prepared statements, keyed by server-assigned id. */
    private final Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();

    /** Session-scoped open cursors, keyed by cursor id. */
    private final Map<String, Cursor> cursors = new ConcurrentHashMap<>();

    private final Database database;
    private final Properties config;

    private Socket clientSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private UUID transactionId;

    private boolean compressionEnabled;
    private int compressionThreshold;
    private int compressionLevel;

    LegacyProtocolAdapter(Database database, Properties config) {
        this.database = database;
        this.config = config == null ? new Properties() : config;
    }

    /**
     * Serves one legacy client connection until the client sends {@code EXIT}
     * or the socket times out / closes.
     *
     * @param clientSocket  the accepted socket
     * @param socketTimeout per-read timeout in milliseconds, or {@code -1} to
     *                       keep the inherited value
     * @throws IOException on unrecoverable stream failure
     */
    void handleConnection(Socket clientSocket, int socketTimeout) throws IOException {
        this.clientSocket = clientSocket;
        LOGGER.log(Level.WARNING,
                "Legacy protocol (Java Object Serialization) connection from {0} - deprecated, "
                        + "migrate the client to the v2 binary protocol",
                clientSocket.getInetAddress());
        if (socketTimeout > 0) {
            try {
                clientSocket.setSoTimeout(socketTimeout);
            } catch (SocketException e) {
                LOGGER.log(Level.WARNING, "Failed to set legacy socket timeout: {0}", e.getMessage());
            }
        }

        this.compressionEnabled = true;
        this.compressionThreshold = Integer.parseInt(config.getProperty(
                "compression.threshold.bytes", String.valueOf(defaultCompressionThreshold)));
        this.compressionLevel = Integer.parseInt(config.getProperty(
                "compression.level", String.valueOf(defaultCompressionLevel)));

        try {
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(clientSocket.getInputStream());

            Object pending = in.readObject();
            boolean serving = true;
            if (pending instanceof CompressionHandshakeMessage handshake) {
                handleLegacyCompressionHandshake(handshake);
            } else {
                // The first message can already be EXIT: its verdict must end
                // the session here, not fall through into the read loop.
                serving = dispatch(pending);
            }

            while (serving && dispatch(in.readObject())) {
                // Keep serving until EXIT / EOF / timeout.
            }
        } catch (EOFException e) {
            LOGGER.log(Level.FINE, "Legacy client {0} closed the connection", clientSocket.getInetAddress());
        } catch (SocketTimeoutException e) {
            LOGGER.log(Level.WARNING, "Legacy client {0} timed out: {1}",
                    new Object[]{clientSocket.getInetAddress(), e.getMessage()});
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Legacy client sent an unknown class", e);
            writeError("Error: Unknown message type");
        } finally {
            cleanup();
        }
    }

    /**
     * Serves one already-read message, then reports whether the session is
     * still alive. {@code false} (or a null message) ends the loop.
     */
    private boolean dispatch(Object message) throws IOException, ClassNotFoundException {
        if (message == null || ErrorMessages.EXIT_COMMAND.equals(message)) {
            return false;
        }
        if (message instanceof CompressionHandshakeMessage handshake) {
            handleLegacyCompressionHandshake(handshake);
            return true;
        }
        if (message instanceof QueryMessage qm) {
            executeQuery(qm.getQuery(), qm.getTransactionId());
            return true;
        }
        if (message instanceof PrepareMessage pm) {
            handlePrepare(pm);
            return true;
        }
        if (message instanceof ExecutePreparedMessage epm) {
            handleExecutePrepared(epm);
            return true;
        }
        if (message instanceof ClosePreparedMessage cpm) {
            handleClosePrepared(cpm);
            return true;
        }
        if (message instanceof OpenCursorMessage ocm) {
            handleOpenCursor(ocm);
            return true;
        }
        if (message instanceof FetchCursorMessage fcm) {
            handleFetchCursor(fcm);
            return true;
        }
        if (message instanceof CloseCursorMessage ccm) {
            handleCloseCursor(ccm);
            return true;
        }
        if (message instanceof BatchQueryMessage bqm) {
            handleBatchQuery(bqm);
            return true;
        }

        writeError("Error: Invalid query message");
        return true;
    }

    /**
     * Reproduces the legacy handshake: the server advertises GZIP and honours
     * the client's level (1-9) and threshold when compression stays on.
     */
    private void handleLegacyCompressionHandshake(CompressionHandshakeMessage handshake) throws IOException {
        LOGGER.log(Level.INFO, "Legacy compression handshake from client: {0}", handshake);

        String agreedAlgorithm = DEFAULT_ALGORITHM;
        int agreedLevel = compressionLevel;
        int agreedThreshold = compressionThreshold;

        if (handshake.isClientSupportsCompression()
                && handshake.getSupportedAlgorithms().contains(DEFAULT_ALGORITHM)) {
            int clientLevel = handshake.getPreferredCompressionLevel();
            if (clientLevel >= 1 && clientLevel <= 9) {
                agreedLevel = clientLevel;
            }
            int clientThreshold = handshake.getCompressionThresholdBytes();
            if (clientThreshold > 0) {
                agreedThreshold = clientThreshold;
            }
            compressionEnabled = true;
        } else {
            compressionEnabled = false;
        }

        this.compressionLevel = agreedLevel;
        this.compressionThreshold = agreedThreshold;

        out.writeObject(new CompressionHandshakeResponse(
                compressionEnabled, agreedAlgorithm, agreedLevel, agreedThreshold));
        out.flush();
    }

    private void executeQuery(String query, UUID transactionId) throws IOException {
        this.transactionId = transactionId;
        try {
            sendSerializedResult(database.executeQuery(query, transactionId));
        } catch (OutOfMemoryError e) {
            handleOutOfMemory(query, e);
        } catch (Exception e) {
            sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}",
                    new Object[]{query, e.getMessage()});
        }
    }

    private void handlePrepare(PrepareMessage pm) throws IOException {
        String statementId = UUID.randomUUID().toString();
        PreparedStatement ps = database.prepareStatement(pm.getSqlTemplate());
        preparedStatements.put(statementId, ps);
        this.transactionId = pm.getTransactionId();
        out.writeObject(statementId);
        out.flush();
        LOGGER.log(Level.INFO, "Prepared statement {0} from template: {1}",
                new Object[]{statementId, pm.getSqlTemplate()});
    }

    private void handleExecutePrepared(ExecutePreparedMessage epm) throws IOException {
        String statementId = epm.getStatementId();
        PreparedStatement ps = preparedStatements.get(statementId);
        if (ps == null) {
            sendSerializedResult(String.format("Error: Unknown prepared statement: %s", statementId));
            return;
        }
        this.transactionId = epm.getTransactionId();
        try {
            ps.bindParameters(epm.getParams());
            sendSerializedResult(ps.execute(database, transactionId));
        } catch (OutOfMemoryError e) {
            handleOutOfMemory(ps.getSqlTemplate(), e);
        } catch (Exception e) {
            sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
            LOGGER.log(Level.SEVERE, "Prepared statement execution failed: {0}, Error: {1}",
                    new Object[]{statementId, e.getMessage()});
        }
    }

    private void handleClosePrepared(ClosePreparedMessage cpm) throws IOException {
        this.transactionId = cpm.getTransactionId();
        PreparedStatement removed = preparedStatements.remove(cpm.getStatementId());
        if (removed != null) {
            removed.clearCache();
        }
        out.writeObject("Prepared statement closed");
        out.flush();
        LOGGER.log(Level.INFO, "Closed prepared statement {0}", cpm.getStatementId());
    }

    private void handleOpenCursor(OpenCursorMessage ocm) throws IOException {
        this.transactionId = ocm.getTransactionId();
        try {
            int fetchSize = ocm.getFetchSize();
            if (fetchSize <= 0) {
                fetchSize = 1000;
            }
            Cursor cursor = database.executeCursor(ocm.getQuery(), fetchSize, transactionId);
            String cursorId = cursor.getId().toString();
            cursors.put(cursorId, cursor);
            sendSerializedResult(cursorId);
            LOGGER.log(Level.INFO, "Opened cursor {0} for query: {1} (fetchSize={2})",
                    new Object[]{cursorId, ocm.getQuery(), fetchSize});
        } catch (OutOfMemoryError e) {
            handleOutOfMemory(ocm.getQuery(), e);
        } catch (Exception e) {
            sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
            LOGGER.log(Level.SEVERE, "Open cursor failed: {0}, Error: {1}",
                    new Object[]{ocm.getQuery(), e.getMessage()});
        }
    }

    private void handleFetchCursor(FetchCursorMessage fcm) throws IOException {
        this.transactionId = fcm.getTransactionId();
        Cursor cursor = cursors.get(String.valueOf(fcm.getCursorId()));
        if (cursor == null) {
            sendSerializedResult(String.format("Error: Unknown or closed cursor: %s", fcm.getCursorId()));
            return;
        }
        try {
            List<Map<String, Object>> batch = cursor.fetch();
            sendSerializedResult(batch);
        } catch (OutOfMemoryError e) {
            handleOutOfMemory("cursor fetch: " + fcm.getCursorId(), e);
        } catch (Exception e) {
            sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
            LOGGER.log(Level.SEVERE, "Fetch cursor failed: {0}, Error: {1}",
                    new Object[]{fcm.getCursorId(), e.getMessage()});
        }
    }

    private void handleCloseCursor(CloseCursorMessage ccm) throws IOException {
        this.transactionId = ccm.getTransactionId();
        Cursor cursor = cursors.remove(String.valueOf(ccm.getCursorId()));
        if (cursor != null) {
            cursor.close();
            out.writeObject("Cursor closed");
        } else {
            out.writeObject("Cursor already closed");
        }
        out.flush();
        LOGGER.log(Level.INFO, "Closed cursor {0}", ccm.getCursorId());
    }

    /** Batches keep the legacy contract: a bare list of per-query results. */
    private void handleBatchQuery(BatchQueryMessage bqm) throws IOException {
        this.transactionId = bqm.getTransactionId();
        try {
            out.writeObject(database.executeBatch(bqm.getQueries(), transactionId));
            out.flush();
        } catch (Exception e) {
            writeError("Error: " + e.getMessage());
        }
    }

    private void handleOutOfMemory(String query, OutOfMemoryError e) {
        long usedBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        LOGGER.log(Level.SEVERE, "OutOfMemoryError while executing query: {0}", query);
        LOGGER.log(Level.SEVERE,
                "  Query context: rows produced={0}, peak memory used={1} bytes at {2} rows, heap used now={3} bytes, cause={4}",
                new Object[]{
                        SelectQuery.getLastQueryRowCount(),
                        SelectQuery.getLastQueryPeakMemoryBytes(),
                        SelectQuery.getLastQueryRowsAtPeak(),
                        usedBytes,
                        String.valueOf(e.getMessage())
                });
        try {
            sendSerializedResult("Error: Query exceeded memory limit. Consider adding LIMIT or indexes.");
        } catch (IOException io) {
            LOGGER.log(Level.SEVERE, "Error sending OOM response to legacy client: {0}", io.getMessage());
        }
    }

    /**
     * Writes a result in the legacy frame: {@code [marker][length][bytes]},
     * GZIP compressed above the negotiated threshold.
     */
    private void sendSerializedResult(Object result) throws IOException {
        byte[] serialized = serializeResult(result);
        byte[] toSend = compressWithMetrics(serialized);
        boolean isCompressed = toSend != serialized;
        out.writeByte(isCompressed ? 0x01 : 0x00);
        out.writeInt(toSend.length);
        out.write(toSend);
        out.flush();
    }

    /** Bare error String, used where the legacy protocol has no framed answer. */
    private void writeError(String message) throws IOException {
        out.writeObject(message);
        out.flush();
    }

    private byte[] serializeResult(Object result) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(result);
        }
        return baos.toByteArray();
    }

    private byte[] compressWithMetrics(byte[] data) {
        if (!compressionEnabled || data.length <= compressionThreshold) {
            return data;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                gzos.write(data);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Compression failed, sending uncompressed: {0}", e.getMessage());
            return data;
        }
    }

    /** Rolls back the session transaction and releases sockets / registries. */
    private void cleanup() {
        try {
            if (transactionId != null && database.isInTransaction(transactionId)) {
                database.executeQuery(SqlKeywords.ROLLBACK_TRANSACTION, transactionId);
            }
            preparedStatements.clear();
            for (Cursor cursor : cursors.values()) {
                cursor.close();
            }
            cursors.clear();
            closeQuietly(in);
            closeQuietly(out);
            closeQuietly(clientSocket);
            LOGGER.log(Level.INFO, "Legacy client disconnected: {0}", clientSocket.getInetAddress());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error cleaning up legacy client resources: {0}", e.getMessage());
        }
    }

    private void closeQuietly(java.io.Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing resource: {0}", e.getMessage());
            }
        }
    }
}
