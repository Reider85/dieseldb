package diesel;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import diesel.net.*;
import diesel.net.messages.*;
import diesel.security.AccessDeniedException;
import diesel.security.AuthException;
import diesel.security.Authenticator;
import diesel.security.Authorizer;
import diesel.security.Privilege;
import diesel.security.Session;

/**
 * TCP server that accepts client connections and executes their SQL queries
 * against a shared {@link Database}.
 *
 * <p>Each accepted client is handled on a worker thread from a fixed-size
 * pool ({@code poolSize} workers, bounded queue). Queries arrive as
 * serialized {@link QueryMessage} objects and the result object is written
 * back; server-side errors are returned as {@code Error: } prefixed Strings.
 * On startup the database is populated from the data directory, and on exit
 * the shutdown hook stops the server gracefully.
 *
 * @see DatabaseClient
 * @see Database
 */
public class DatabaseServer {
    private static final Logger LOGGER = Logger.getLogger(DatabaseServer.class.getName());

    static {
        try {
            java.util.Properties props = ConfigLoader.load();
            String val = props.getProperty("server.pool.size");
            if (val != null) poolSize = Integer.parseInt(val.trim());
            String val2 = props.getProperty("server.queue.capacity");
            if (val2 != null) queueCapacity = Integer.parseInt(val2.trim());
            String val3 = props.getProperty("server.backlog");
            if (val3 != null) backlog = Integer.parseInt(val3.trim());
        } catch (Exception ignored) {
            LOGGER.log(Level.WARNING, "Failed to load server config properties, using defaults", ignored);
        }
    }
    private static final String CONFIG_FILE = ErrorMessages.CONFIG_FILE;
    private static int poolSize = 100;
    private static int queueCapacity = 100;
    private static int backlog = 512;
    private final int port;
    private final Database database;
    private ServerSocket serverSocket;
    private ServerSocket legacyServerSocket;
    private volatile boolean running;
    private final ThreadPoolExecutor executor;
    private final int socketTimeout;
    private final int legacyPort;
    private final Properties configOverride;
    /** Shared session registry for all connections (prompt4 #31). */
    private volatile Authenticator authenticator;

    /**
     * Creates a server on the given port with the default socket timeout.
     *
     * @param port the port to listen on
     */
    public DatabaseServer(int port) {
        this(port, -1);
    }

    /**
     * Creates a server on the given port with the given socket timeout.
     *
     * @param port          the port to listen on
     * @param socketTimeout the per-client socket read timeout in milliseconds,
     *                      or -1 to load it from the configuration file
     */
    public DatabaseServer(int port, int socketTimeout) {
        this(port, socketTimeout, new Database());
    }

    /**
     * Creates a server on the given port with the given socket timeout and
     * database.
     *
     * @param port          the port to listen on
     * @param socketTimeout the per-client socket read timeout in milliseconds,
     *                      or -1 to load it from the configuration file
     * @param database      the database instance to serve
     */
    public DatabaseServer(int port, int socketTimeout, Database database) {
        this(port, socketTimeout, 0, database);
    }

    public DatabaseServer(int port, int socketTimeout, int legacyPort, Database database) {
        this(port, socketTimeout, legacyPort, database, null);
    }

    /**
     * Creates a server with an explicit configuration overlay (prompt4 #29).
     * Entries win over {@code config.properties} for this server instance only,
     * so a test or an embedding host can flip a single key (e.g.
     * {@code compression.threshold.bytes}) without mutating process-wide state.
     *
     * @param port           the port to listen on
     * @param socketTimeout  the per-client socket read timeout in milliseconds,
     *                       or -1 to load it from the configuration file
     * @param legacyPort     the deprecated v1-protocol port, 0 disables it
     * @param database       the database instance to serve
     * @param configOverride configuration entries applied on top of the file,
     *                       or null to use the file as-is
     */
    public DatabaseServer(int port, int socketTimeout, int legacyPort, Database database,
                          Properties configOverride) {
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.legacyPort = legacyPort;
        this.database = database;
        this.configOverride = configOverride;
        this.executor = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadPoolExecutor.AbortPolicy());
    }

    // Load configuration and return Properties object
    private static Properties loadConfig() {
        return ConfigLoader.load();
    }

    // Log configuration parameters
    private static void logConfig(Properties config) {
        LOGGER.log(Level.INFO, "Configuration parameters loaded from {0}:", CONFIG_FILE);
        if (config.isEmpty()) {
            LOGGER.log(Level.WARNING, "No configuration parameters found in {0}", CONFIG_FILE);
        } else {
            config.forEach((key, value) ->
                    LOGGER.log(Level.INFO, "Config: {0} = {1}", new Object[]{key, value}));
        }
    }

    // Get isolation level from Properties
    private static IsolationLevel getIsolationLevel(Properties config) {
        String isolationLevelStr = config.getProperty("transaction.isolation.level", "READ_UNCOMMITTED").toUpperCase();
        try {
            return IsolationLevel.valueOf(isolationLevelStr);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Invalid isolation level {0} in {1}, using default READ_UNCOMMITTED", new Object[]{isolationLevelStr, CONFIG_FILE});
            return IsolationLevel.READ_UNCOMMITTED;
        }
    }

    // Get socket timeout in milliseconds from Properties
    private static int getSocketTimeout(Properties config) {
        String timeoutStr = config.getProperty("server.socket.timeout", "30000");
        try {
            int timeout = Integer.parseInt(timeoutStr.trim());
            if (timeout < 0) {
                throw new NumberFormatException("negative timeout");
            }
            return timeout;
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid socket timeout {0} in {1}, using default 30000", new Object[]{timeoutStr, CONFIG_FILE});
            return 30000;
        }
    }

    /**
     * Loads the configuration, populates the database from disk and starts the
     * accept loop. Each connection is served on a worker thread; the loop runs
     * until {@link #stop()} is called.
     */
    public void start() {
        // Load and log configuration
        Properties config = loadConfig();
        // Instance-level overlay (prompt4 #29): wins over config.properties for
        // this server only, keeping tests free of process-wide property flips.
        if (configOverride != null) {
            config.putAll(configOverride);
        }
        logConfig(config);
        IsolationLevel isolationLevel = getIsolationLevel(config);
        int effectiveSocketTimeout = socketTimeout >= 0 ? socketTimeout : getSocketTimeout(config);
        LOGGER.log(Level.INFO, "Server configured with transaction isolation level: {0}", isolationLevel);

        running = true;
        database.loadCatalog();
        database.loadTablesFromDisk();
        // ARIES startup recovery (prompt4 #19): analysis → redo → undo before
        // the vacuum daemon starts and strictly before client connections are
        // accepted. No-op when the WAL is disabled.
        database.runRecovery();
        // Start background page flusher (prompt4 #20)
        database.startPageFlusher();
        // Start background checkpoint manager (prompt4 #21)
        database.startCheckpointManager();
        database.getVacuumManager().startAutoVacuum();
        // Bootstrap the admin account on the first run (prompt4 #31, #30 step 2/4)
        if (Boolean.parseBoolean(config.getProperty("security.auth.bootstrap-admin", "true"))) {
            database.getUserStore().ensureDefaultAdmin();
        }
        getAuthenticator();
        try {
            serverSocket = new ServerSocket(port, backlog);
            LOGGER.log(Level.INFO, "Database server started on port {0}", port);

            // Start legacy server if enabled
            if (legacyPort > 0) {
                startLegacyServer(database, effectiveSocketTimeout, config);
            }

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    LOGGER.log(Level.INFO, "New client connected: {0}", clientSocket.getInetAddress());
                    handleClientConnection(clientSocket, executor, database, effectiveSocketTimeout, config);
                } catch (IOException e) {
                    if (running) {
                        LOGGER.log(Level.SEVERE, "Error accepting client connection: {0}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server: {0}", e.getMessage());
        }
    }

    /**
     * Starts the legacy server on the specified port.
     */
    private void startLegacyServer(Database database, int effectiveSocketTimeout, Properties config) {
        Thread legacyThread = new Thread(() -> {
            try {
                legacyServerSocket = new ServerSocket(legacyPort, backlog);
                LOGGER.log(Level.WARNING, "Legacy protocol server started on deprecated port {0} - use v2 protocol instead", legacyPort);

                while (running) {
                    try {
                        Socket clientSocket = legacyServerSocket.accept();
                        LOGGER.log(Level.WARNING, "New legacy client connected: {0}", clientSocket.getInetAddress());
                        
                        // Handle legacy connection in a separate thread
                        Thread legacyHandlerThread = new Thread(() -> {
                            try {
                                LegacyProtocolAdapter adapter = new LegacyProtocolAdapter(database, config);
                                adapter.handleConnection(clientSocket, effectiveSocketTimeout);
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Legacy client handler error: {0}", e.getMessage());
                            }
                        });
                        legacyHandlerThread.setDaemon(true);
                        legacyHandlerThread.start();
                    } catch (IOException e) {
                        if (running) {
                            LOGGER.log(Level.SEVERE, "Error accepting legacy client connection: {0}", e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to start legacy server: {0}", e.getMessage());
            }
        });
        legacyThread.setDaemon(true);
        legacyThread.start();
    }

    /**
     * Stops accepting connections and shuts the worker pool down, giving the
     * workers up to 2 seconds to finish before forcing termination.
     */
    public void stop() {
        running = false;
        
        // Stop legacy server
        if (legacyServerSocket != null && !legacyServerSocket.isClosed()) {
            try {
                legacyServerSocket.close();
                LOGGER.log(Level.INFO, "Legacy server socket closed");
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error closing legacy server socket: {0}", e.getMessage());
            }
        }
        
        database.getVacuumManager().stop();
        // Stop background checkpoint manager (prompt4 #21)
        try {
            database.stopCheckpointManager();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error stopping checkpoint manager: {0}", e.getMessage());
        }
        // Stop background page flusher
        try {
            database.stopPageFlusher();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error stopping page flusher: {0}", e.getMessage());
        }
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            LOGGER.log(Level.INFO, "Database server stopped");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error stopping server: {0}", e.getMessage());
        }
        // Shut down the worker pool: give workers up to 2 seconds to finish, then force terminate
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "Worker threads did not finish within 2 seconds, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final Database database;
        private final Authenticator authenticator;
        /** Authenticated session for this connection, or null (prompt4 #31). */
        private Session session;
        /** When true, data-plane messages require an authenticated session. */
        private final boolean authRequired;
        private DataOutputStream out;
        private DataInputStream in;
        private UUID transactionId;

        /**
         * Session-scoped registry of prepared statements (Prompt 79): maps a
         * client-assigned statement id to the prepared statement created for
         * this connection. Ids are generated client-side as UUIDs so they never
         * collide across connections.
         */
        private final Map<String, PreparedStatement> preparedStatements = new java.util.concurrent.ConcurrentHashMap<>();
        
        /**
         * Session-scoped registry of open server-side cursors (Prompt 81):
         * maps a cursor id to the live {@link Cursor}. Cursors are removed on
         * explicit close and on client disconnect (see {@link #run}).
         */
        private final Map<String, Cursor> cursors = new java.util.concurrent.ConcurrentHashMap<>();
        
        private static int defaultCompressionThreshold = 1024;
        private static int defaultCompressionLevel = 6;

        /** Default rows per cursor fetch when the client does not specify one (Prompt 81). */
        private static final int DEFAULT_CURSOR_FETCH_SIZE = 1000;

        static {
            defaultCompressionThreshold = ConfigLoader.getInt("compression.threshold.bytes", defaultCompressionThreshold);
            defaultCompressionLevel = ConfigLoader.getInt("compression.level", defaultCompressionLevel);
        }

        private static final String DEFAULT_ALGORITHM = "GZIP";
        
        private int compressionThreshold;
        private int compressionLevel;
        private String compressionAlgorithm;
        private boolean compressionEnabled;
        private CompressionHandshakeHandler compressionHandler;
        
        // Metrics
        private long totalOriginalBytes = 0;
        private long totalCompressedBytes = 0;
        private long totalCompressionTimeNanos = 0;
        private int compressionCount = 0;

        public ClientHandler(Socket socket, Database database, int socketTimeout, Properties config,
                Authenticator authenticator) {
            this.clientSocket = socket;
            this.database = database;
            this.authenticator = authenticator;
            this.transactionId = null;
            this.compressionAlgorithm = DEFAULT_ALGORITHM;
            try {
                clientSocket.setSoTimeout(socketTimeout);
            } catch (SocketException e) {
                LOGGER.log(Level.WARNING, "Failed to set socket timeout: {0}", e.getMessage());
            }
            // Default values, will be updated by handshake
            this.compressionThreshold = Integer.parseInt(config.getProperty("compression.threshold.bytes", String.valueOf(defaultCompressionThreshold)));
            this.compressionLevel = Integer.parseInt(config.getProperty("compression.level", String.valueOf(defaultCompressionLevel)));
            this.compressionEnabled = true;
            // Auth is opt-out (prompt4 #31): unauthenticated data-plane queries
            // are rejected unless the deployment turns enforcement off.
            this.authRequired = Boolean.parseBoolean(config.getProperty("security.auth.required", "true"));
        }

        /**
         * Runs the startup handshake: an optional AUTH frame first (prompt4
         * #31), then the compression negotiation. Frames that are neither are
         * handed to the dispatch loop untouched for maximum compatibility.
         *
         * @return the pending message for the dispatch loop, or null
         */
        private Message performStartupHandshake() throws IOException {
            Message first = readNextMessage(null);
            if (first == null) {
                return null;
            }
            if (first.getType() == MessageType.AUTH) {
                handleAuthMessage((diesel.net.messages.AuthMessage) first);
                first = readNextMessage(null);
                if (first == null) {
                    return null;
                }
            }
            if (first.getType() == MessageType.COMPRESSION_HANDSHAKE) {
                return performHandshake((diesel.net.messages.CompressionHandshakeMessage) first);
            }
            return first;
        }

        /**
         * Verifies credentials through the {@link Authenticator} and answers
         * with an {@link AuthResponseMessage} (prompt4 #31). A failed attempt
         * leaves the session unauthenticated; the connection stays open so the
         * client receives the verdict.
         */
        private void handleAuthMessage(diesel.net.messages.AuthMessage message) {
            boolean success = false;
            java.util.UUID token = null;
            java.util.UUID userId = null;
            boolean mustChange = false;
            String text;
            try {
                Session authenticated = authenticator.authenticate(
                        message.getUserName(), message.getPassword());
                if (session != null) {
                    authenticator.invalidate(session.getToken());
                }
                session = authenticated;
                authenticated.setSourceIp(clientSocket.getInetAddress().getHostAddress());
                success = true;
                token = authenticated.getToken().getId();
                userId = authenticated.getUserId();
                mustChange = authenticated.isMustChangePassword();
                text = "Authenticated as " + authenticated.getUserName();
                LOGGER.log(Level.INFO, "Client {0} authenticated as user {1}",
                        new Object[]{clientSocket.getInetAddress(), authenticated.getUserName()});
            } catch (AuthException e) {
                text = e.getMessage();
                LOGGER.log(Level.WARNING, "Authentication failure for user ''{0}'' from {1}: {2}",
                        new Object[]{message.getUserName(), clientSocket.getInetAddress(), e.getMessage()});
            }
            try {
                AuthResponseMessage response = new AuthResponseMessage(
                        success, token, userId, mustChange, text, message.getTransactionId());
                MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.AUTH_RESPONSE);
                WireProtocol.writeMessage(response, codec, out);
                out.flush();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to send auth response: {0}", e.getMessage());
            }
        }

        /**
         * Gates data-plane messages behind an authenticated session
         * (prompt4 #31). Replies with an error when authentication is
         * required but missing; returns whether the caller may proceed.
         */
        private boolean ensureAuthenticated() throws IOException {
            if (session != null || !authRequired) {
                return true;
            }
            sendSerializedResult("Error: Authentication required");
            return false;
        }

/**
         * Performs compression handshake with client.
         * Reads client handshake message and sends server response with agreed settings.
         */
        private Message performHandshake() throws IOException {
            // Read client handshake message
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.COMPRESSION_HANDSHAKE);
            diesel.net.messages.CompressionHandshakeMessage handshake = 
                (diesel.net.messages.CompressionHandshakeMessage) WireProtocol.readMessage(codec, in);
            return performHandshake(handshake);
        }

        /**
         * Applies an already-read compression handshake message (prompt4 #31:
         * the startup flow may deliver it after AUTH) and replies with the
         * negotiated settings.
         */
        private Message performHandshake(diesel.net.messages.CompressionHandshakeMessage handshake)
                throws IOException {
            LOGGER.log(Level.INFO, "Received compression handshake from client: {0}", handshake);

            // Create compression handler with negotiated settings and keep it
            // for every payload of this session (both directions).
            compressionHandler = new CompressionHandshakeHandler(
                handshake, compressionThreshold, compressionLevel);
            
            // Update instance variables with agreed settings
            this.compressionEnabled = compressionHandler.isCompressionEnabled();
            this.compressionAlgorithm = compressionHandler.getChosenCodec().name();
            this.compressionLevel = compressionHandler.getActualLevel();
            this.compressionThreshold = compressionHandler.getActualThreshold();
            
            // Send proper response to client
            diesel.net.messages.CompressionHandshakeResponseMessage response = 
                compressionHandler.createResponse(handshake.getTransactionId());
            MessageCodec responseCodec = MessageCodecRegistry.getCodec(MessageType.COMPRESSION_HANDSHAKE_RESPONSE);
            WireProtocol.writeMessage(response, responseCodec, out);
            out.flush();
            
            return null; // Handshake is complete, no pending message
        }



        /**
         * Logs compression metrics.
         */
        private void logCompressionMetrics() {
            if (compressionCount == 0) return;
            
            double ratio = totalOriginalBytes > 0 ? 
                    (double) totalCompressedBytes / totalOriginalBytes : 0.0;
            double avgTimeMs = totalCompressionTimeNanos / 1_000_000.0 / compressionCount;
            
            LOGGER.log(Level.INFO,
                    "Compression metrics [client={0}]: count={1}, totalOriginal={2} bytes, totalCompressed={3} bytes, ratio={4}, avgTime={5}ms",
                    new Object[]{
                        clientSocket.getInetAddress(),
                        compressionCount,
                        totalOriginalBytes,
                        totalCompressedBytes,
                        ratio,
                        avgTimeMs
                    });
        }

        /**
         * Reports an OutOfMemoryError thrown while running a query: logs the
         * query text together with the query's own peak-memory and row metrics
         * ({@link SelectQuery}) plus the current heap usage, then answers the
         * client with a short, actionable error message instead of dropping the
         * connection.
         */
        private void handleOutOfMemory(String query, OutOfMemoryError e) {
            long usedBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            LOGGER.log(Level.SEVERE, "OutOfMemoryError while executing query: {0}", query);
            LOGGER.log(Level.SEVERE, "  Query context: rows produced={0}, peak memory used={1} bytes at {2} rows, heap used now={3} bytes, cause={4}",
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
                LOGGER.log(Level.SEVERE, "Error sending OOM response to client: {0}", io.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                out = new DataOutputStream(clientSocket.getOutputStream());
                in = new DataInputStream(clientSocket.getInputStream());

                // Startup handshake: optional AUTH (prompt4 #31) then compression
                Message pendingInput = performStartupHandshake();

                Message input = readNextMessage(pendingInput);
                while (input != null && dispatchMessage(input)) {
                    input = readNextMessage(null);
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Client handler error: {0}", e.getMessage());
            } finally {
                cleanupClientResources();
            }
        }

        private void cleanupClientResources() {
            try {
                if (transactionId != null && database.isInTransaction(transactionId)) {
                    database.executeQuery(SqlKeywords.ROLLBACK_TRANSACTION, transactionId);
                }
                preparedStatements.clear();
                for (Cursor cursor : cursors.values()) {
                    cursor.close();
                }
                cursors.clear();
                if (session != null) {
                    authenticator.invalidate(session.getToken());
                    session = null;
                }
                closeQuietly(out);
                closeQuietly(in);
                closeQuietly(clientSocket);
                LOGGER.log(Level.INFO, "Client disconnected: {0}", clientSocket.getInetAddress());
                logCompressionMetrics();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error closing client resources: {0}", e.getMessage());
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

        /**
         * Reads the next message, using the pending input from the handshake if
         * present, otherwise reading from the stream with socket timeout
         * handling. Returns null to signal the loop should terminate.
         */
        private Message readNextMessage(Message pendingMessage) throws IOException {
            if (pendingMessage != null) {
                return pendingMessage;
            }
            try {
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
                DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(payload));
                Message message = codec.decode(payloadIn);
                if (message.getType() != frame.type()) {
                    throw new InvalidMessageException("Payload type tag " + message.getType()
                            + " does not match frame header type " + frame.type());
                }
                return message;
            } catch (EOFException e) {
                LOGGER.log(Level.FINE, "Client {0} closed the connection", clientSocket.getInetAddress());
                return null;
            } catch (SocketTimeoutException e) {
                LOGGER.log(Level.WARNING, "Socket timeout while waiting for query from client {0}: {1}",
                        new Object[]{clientSocket.getInetAddress(), e.getMessage()});
                return null;
            } catch (InvalidMessageException e) {
                LOGGER.log(Level.WARNING, "Invalid message from client {0}: {1}", 
                        new Object[]{clientSocket.getInetAddress(), e.getMessage()});
                return null;
            }
        }

        /**
         * Dispatches an incoming message to its handler. Returns false when the
         * loop should terminate.
         */
        private boolean dispatchMessage(Message input) throws IOException {
            if (input == null) {
                return false;
            }

            switch (input.getType()) {
                case COMPRESSION_HANDSHAKE:
                    performHandshake();
                    return true;

                case AUTH:
                    handleAuthMessage((diesel.net.messages.AuthMessage) input);
                    return true;

                case PREPARE:
                    if (!ensureAuthenticated()) {
                        return true;
                    }
                    handlePrepare((diesel.net.messages.PrepareMessage) input);
                    return true;
                    
                case EXECUTE_PREPARED:
                    if (!ensureAuthenticated()) {
                        return true;
                    }
                    handleExecutePrepared((diesel.net.messages.ExecutePreparedMessage) input);
                    return true;
                    
                case CLOSE_PREPARED:
                    handleClosePrepared((diesel.net.messages.ClosePreparedMessage) input);
                    return true;
                    
                case OPEN_CURSOR:
                    if (!ensureAuthenticated()) {
                        return true;
                    }
                    handleOpenCursor((diesel.net.messages.OpenCursorMessage) input);
                    return true;
                    
                case FETCH_CURSOR:
                    if (!ensureAuthenticated()) {
                        return true;
                    }
                    handleFetchCursor((diesel.net.messages.FetchCursorMessage) input);
                    return true;
                    
                case CLOSE_CURSOR:
                    handleCloseCursor((diesel.net.messages.CloseCursorMessage) input);
                    return true;
                    
                case QUERY:
                    if (!ensureAuthenticated()) {
                        return true;
                    }
                    executeQueryMessage((diesel.net.messages.QueryMessage) input);
                    return true;
                    
                case HEALTH_CHECK:
                    handleHealthCheck((diesel.net.messages.HealthCheckMessage) input);
                    return true;
                    
                default:
                    sendSerializedResult("Error: Invalid message type: " + input.getType());
                    return true;
            }
        }

        /**
         * Executes a {@link QueryMessage} against the database and replies
         * with the serialized result or an error.
         */
        private void executeQueryMessage(diesel.net.messages.QueryMessage qm) throws IOException {
            String query = qm.getQuery();
            transactionId = qm.getTransactionId();

            // No ALTER USER pre-check here (prompt4 #32): cross-account
            // password changes are authorized by the Authorizer inside
            // Database.executeQuery - self-service, ADMIN privilege, or denial.

            try {
                Object result = database.executeQuery(query, transactionId, session);
                
                // Invalidate privilege cache after any GRANT or REVOKE (prompt4 #32)
                if (session != null && result instanceof String) {
                    String resultStr = (String) result;
                    if (resultStr.startsWith("Privilege granted:") || resultStr.startsWith("Privilege revoked:") ||
                        resultStr.startsWith("Role granted:")) {
                        database.getAuthorizer().invalidateAll();
                    }
                }
                
                if (session != null && AlterUserQuery.isAlterUser(query)
                        && !(result instanceof String error && error.startsWith(ErrorMessages.ERROR_PREFIX))) {
                    session.setMustChangePassword(false);
                }
                sendSerializedResult(result);
            } catch (OutOfMemoryError e) {
                handleOutOfMemory(query, e);
            } catch (Exception e) {
                sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
                LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}",
                        new Object[]{query, e.getMessage()});
            }
        }

        /**
         * Serializes a result and writes it to the client with compression.
         */
        private void sendSerializedResult(Object result) throws IOException {
            // Convert the raw result into the shape its status announces, so the
            // client can rebuild the exact object the legacy protocol delivered.
            String status;
            long duration = 0; // TODO: track actual execution duration
            
            // Convert result to appropriate format
            List<String> columnNames = new ArrayList<>();
            List<List<Object>> rows = new ArrayList<>();
            
            if (result instanceof String errorMessage && errorMessage.startsWith(ErrorMessages.ERROR_PREFIX)) {
                status = diesel.net.messages.QueryResultMessage.STATUS_ERROR;
                columnNames.add("error");
                rows.add(Collections.singletonList(errorMessage));
            } else if (result instanceof List) {
                status = diesel.net.messages.QueryResultMessage.STATUS_ROWS;
                @SuppressWarnings("unchecked")
                List<Object> resultList = (List<Object>) result;
                if (!resultList.isEmpty()) {
                    if (resultList.get(0) instanceof Map) {
                        // This is a query result with rows
                        @SuppressWarnings("unchecked")
                        List<?> rowList = (List<?>) resultList;
                        if (!rowList.isEmpty() && rowList.get(0) instanceof Map) {
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> typedRowList = (List<Map<String, Object>>) rowList;
                            // Extract column names from first row
                            Map<String, Object> firstRow = typedRowList.get(0);
                            columnNames.addAll(firstRow.keySet());
                            
                            // Convert rows to list format
                            for (Map<String, Object> row : typedRowList) {
                                List<Object> values = new ArrayList<>();
                                for (String col : columnNames) {
                                    values.add(row.get(col));
                                }
                                rows.add(values);
                            }
                        }
                    } else {
                        // Simple list result
                        columnNames.add("result");
                        for (Object item : resultList) {
                            rows.add(Collections.singletonList(item));
                        }
                    }
                }
            } else if (result instanceof Map) {
                // Single row result
                status = diesel.net.messages.QueryResultMessage.STATUS_ROWS;
                @SuppressWarnings("unchecked")
                Map<String, Object> resultMap = (Map<String, Object>) result;
                columnNames.addAll(resultMap.keySet());
                List<Object> values = new ArrayList<>();
                for (String col : columnNames) {
                    values.add(resultMap.get(col));
                }
                rows.add(values);
            } else {
                // Simple scalar result (String status messages, null for DML, ...)
                status = diesel.net.messages.QueryResultMessage.STATUS_SCALAR;
                columnNames.add("result");
                rows.add(Collections.singletonList(result));
            }
            
            diesel.net.messages.QueryResultMessage response = new diesel.net.messages.QueryResultMessage(status, duration, columnNames, rows, transactionId);
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.QUERY_RESULT);
            
            // Encode the message payload
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (DataOutputStream payloadOut = new DataOutputStream(baos)) {
                codec.encode(response, payloadOut);
            }
            byte[] payload = baos.toByteArray();
            
            // Compress only when the negotiated handler asks for it and the
            // result actually gets smaller: the frame header flag tells the peer
            // which of the two happened.
            byte[] compressed = payload;
            if (compressionHandler != null && compressionHandler.shouldCompress(payload.length)) {
                long compressStart = System.nanoTime();
                byte[] candidate = compressionHandler.compress(payload);
                totalCompressionTimeNanos += System.nanoTime() - compressStart;
                if (candidate.length < payload.length) {
                    compressed = candidate;
                    totalOriginalBytes += payload.length;
                    totalCompressedBytes += candidate.length;
                    compressionCount++;
                    LOGGER.log(Level.FINE, "Compressed query result: {0} -> {1} bytes",
                            new Object[]{payload.length, compressed.length});
                }
            }
            
            boolean isCompressed = compressed.length != payload.length;
            WireProtocol.encodeFrame(MessageType.QUERY_RESULT, compressed, isCompressed, out);
            out.flush();
        }

        /**
         * Handles a {@link PrepareMessage}: registers a prepared statement
         * (parsed lazily) under a fresh id and answers with the id. The id is
         * generated server-side so a hostile client cannot collide with an
         * existing statement.
         */
        private void handlePrepare(diesel.net.messages.PrepareMessage pm) throws IOException {
            String statementId = UUID.randomUUID().toString();
            PreparedStatement ps = database.prepareStatement(pm.getSqlTemplate());
            preparedStatements.put(statementId, ps);
            transactionId = pm.getTransactionId();
            
            // Send response with new protocol
            diesel.net.messages.PrepareResponseMessage response = new diesel.net.messages.PrepareResponseMessage(statementId, transactionId);
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.PREPARE_RESPONSE);
            WireProtocol.writeMessage(response, codec, out);
            out.flush();
            
            LOGGER.log(Level.INFO, "Prepared statement {0} from template: {1}",
                    new Object[]{statementId, pm.getSqlTemplate()});
        }

        /**
         * Handles an {@link ExecutePreparedMessage}: looks up the prepared
         * statement, binds the parameters and executes it, replying with the
         * serialized result.
         */
        private void handleExecutePrepared(diesel.net.messages.ExecutePreparedMessage epm) throws IOException {
            String statementId = epm.getPreparedId();
            PreparedStatement ps = preparedStatements.get(statementId);
            if (ps == null) {
                sendSerializedResult(String.format("Error: Unknown prepared statement: %s", statementId));
                return;
            }
            transactionId = epm.getTransactionId();
            try {
                ps.bindParameters(epm.getParameters());
                Object result = ps.executeWithSession(database, transactionId, session);
                sendSerializedResult(result);
            } catch (OutOfMemoryError e) {
                handleOutOfMemory(ps.getSqlTemplate(), e);
            } catch (Exception e) {
                sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
                LOGGER.log(Level.SEVERE, "Prepared statement execution failed: {0}, Error: {1}",
                        new Object[]{statementId, e.getMessage()});
            }
        }

        /**
         * Handles a {@link HealthCheckMessage}: responds with server health status.
         */
        private void handleHealthCheck(diesel.net.messages.HealthCheckMessage hcm) throws IOException {
            long responseTime = System.nanoTime() - hcm.getTimestamp();
            diesel.net.messages.HealthCheckResponse response = new diesel.net.messages.HealthCheckResponse("OK", hcm.getTimestamp(), responseTime, transactionId);
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.HEALTH_CHECK_RESPONSE);
            WireProtocol.writeMessage(response, codec, out);
            out.flush();
        }

        /**
         * Handles a {@link ClosePreparedMessage}: removes the prepared
         * statement from the registry, releasing its cached parsed AST.
         */
        private void handleClosePrepared(diesel.net.messages.ClosePreparedMessage cpm) throws IOException {
            String statementId = cpm.getStatementId();
            PreparedStatement removed = preparedStatements.remove(statementId);
            transactionId = cpm.getTransactionId();
            if (removed != null) {
                removed.clearCache();
            }
            
            // Send response with new protocol
            diesel.net.messages.ClosePreparedResponseMessage response = new diesel.net.messages.ClosePreparedResponseMessage(transactionId);
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.CLOSE_PREPARED_RESPONSE);
            WireProtocol.writeMessage(response, codec, out);
            out.flush();
            
            LOGGER.log(Level.INFO, "Closed prepared statement {0}", statementId);
        }

        /**
         * Handles an {@link OpenCursorMessage}: opens a server-side cursor
         * (Prompt 81) over the given SELECT and registers it under a fresh id,
         * replying with the id. Errors are returned as {@code Error: } strings.
         */
        private void handleOpenCursor(diesel.net.messages.OpenCursorMessage ocm) throws IOException {
            transactionId = ocm.getTransactionId();
            try {
                int fetchSize = ocm.getFetchSize();
                if (fetchSize <= 0) {
                    fetchSize = DEFAULT_CURSOR_FETCH_SIZE;
                }
                Cursor cursor = database.executeCursor(ocm.getQuery(), fetchSize, transactionId, session);
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

        /**
         * Handles a {@link FetchCursorMessage}: fetches the next batch of rows
         * from an open cursor and replies with the list (empty when exhausted
         * or the cursor is unknown/closed).
         */
        private void handleFetchCursor(diesel.net.messages.FetchCursorMessage fcm) throws IOException {
            transactionId = fcm.getTransactionId();
            // A nil cursor id decodes to null (0x00 sentinel); answer with the
            // unknown-cursor error instead of letting the NPE kill the session.
            java.util.UUID cursorId = fcm.getCursorId();
            Cursor cursor = cursorId == null ? null : cursors.get(cursorId.toString());
            if (cursor == null) {
                sendSerializedResult(String.format("Error: Unknown or closed cursor: %s", cursorId));
                return;
            }
            try {
                List<Map<String, Object>> batch = cursor.fetch();
                sendSerializedResult(batch);
                LOGGER.log(Level.FINE, "Fetched {0} rows from cursor {1}",
                        new Object[]{batch.size(), fcm.getCursorId()});
            } catch (OutOfMemoryError e) {
                handleOutOfMemory("cursor fetch: " + fcm.getCursorId(), e);
            } catch (Exception e) {
                sendSerializedResult(ErrorMessages.ERROR_PREFIX + e.getMessage());
                LOGGER.log(Level.SEVERE, "Fetch cursor failed: {0}, Error: {1}",
                        new Object[]{fcm.getCursorId(), e.getMessage()});
            }
        }

        /**
         * Handles a {@link CloseCursorMessage}: closes the given cursor and
         * removes it from the session registry.
         */
        private void handleCloseCursor(diesel.net.messages.CloseCursorMessage ccm) throws IOException {
            transactionId = ccm.getTransactionId();
            java.util.UUID closeCursorId = ccm.getCursorId();
            Cursor cursor = closeCursorId == null ? null : cursors.remove(closeCursorId.toString());
            if (cursor != null) {
                cursor.close();
            }
            // Always answer: the client blocks on this response even when the
            // cursor was already gone (closing twice must not hang the session).
            diesel.net.messages.CloseCursorResponseMessage response =
                new diesel.net.messages.CloseCursorResponseMessage(transactionId);
            MessageCodec codec = MessageCodecRegistry.getCodec(MessageType.CLOSE_CURSOR_RESPONSE);
            WireProtocol.writeMessage(response, codec, out);
            out.flush();
            LOGGER.log(Level.INFO, "Closed cursor {0}", closeCursorId);
        }
    }

    /**
      * Entry point: starts a server on the given port (default 3306) with an
     * optional data directory (default "."). A shutdown hook stops the server
     * gracefully on JVM exit.
     *
     * @param args optional {@code port} and {@code dataDir} arguments
     */
    public static void main(String[] args) {
        System.setProperty("diesel.inmemory.persist", "true");
        int localPort = 3306;
        if (args.length > 0) {
            try {
                localPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.SEVERE, "Invalid port {0}, using default {1}", new Object[]{args[0], localPort});
            }
        }
        String localDataDir = "data";
        if (args.length > 1) {
            localDataDir = args[1];
        }
        Database database = new Database(localDataDir);
        
        // Load config to check legacy port
        Properties config = ConfigLoader.load();
        int legacyPort = ConfigLoader.getInt("legacy.port", 0);
        
        DatabaseServer server = new DatabaseServer(localPort, -1, legacyPort, database);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.log(Level.INFO, "Shutdown hook triggered, stopping server gracefully");
            try {
                if (server.serverSocket != null && !server.serverSocket.isClosed()) {
                    server.serverSocket.close();
                    LOGGER.log(Level.INFO, "ServerSocket closed, no longer accepting new connections");
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error closing ServerSocket in shutdown hook: {0}", e.getMessage());
            }
            server.stop();
        }, "shutdown-hook"));
        server.start();
    }
    
    /**
     * Returns the shared authenticator, creating it on first use so that
     * servers with authentication disabled never touch the user store
     * (prompt4 #31).
     */
    Authenticator getAuthenticator() {
        Authenticator local = authenticator;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (authenticator == null) {
                authenticator = new Authenticator(database.getUserStore());
            }
            return authenticator;
        }
    }

    /**
     * Handles a client connection, submitting it to the executor or closing it if rejected.
     */
    private void handleClientConnection(Socket clientSocket, ThreadPoolExecutor executor, 
                                     Database database, int effectiveSocketTimeout, Properties config) {
        try {
            executor.execute(new ClientHandler(clientSocket, database, effectiveSocketTimeout,
                    config, getAuthenticator()));
        } catch (RejectedExecutionException e) {
            LOGGER.log(Level.SEVERE, "Rejected connection from {0}: worker pool full ({1})",
                    new Object[]{clientSocket.getInetAddress(), e.getMessage()});
            closeClientSocket(clientSocket);
        }
    }
    
    /**
     * Closes a client socket with proper error handling.
     */
    private void closeClientSocket(Socket clientSocket) {
        try {
            clientSocket.close();
        } catch (IOException io) {
            LOGGER.log(Level.SEVERE, "Error closing rejected client socket: {0}", io.getMessage());
        }
    }
}