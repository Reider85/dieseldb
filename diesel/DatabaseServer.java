package diesel;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Deflater;

/**
 * TCP server that accepts client connections and executes their SQL queries
 * against a shared {@link Database}.
 *
 * <p>Each accepted client is handled on a worker thread from a fixed-size
 * pool ({@code POOL_SIZE} workers, bounded queue). Queries arrive as
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
    private static final String CONFIG_FILE = ErrorMessages.CONFIG_FILE;
    private static final int POOL_SIZE = 100;
    private static final int QUEUE_CAPACITY = 100;
    private final int port;
    private final Database database;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final ThreadPoolExecutor executor;
    private final int socketTimeout;

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
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.database = database;
        this.executor = new ThreadPoolExecutor(
                POOL_SIZE, POOL_SIZE, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadPoolExecutor.AbortPolicy());
    }

    // Load configuration and return Properties object
    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = DatabaseServer.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                LOGGER.log(Level.SEVERE, "Configuration file {0} not found", CONFIG_FILE);
                return props; // Return empty Properties
            }
            props.load(input);
            return props;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load {0}: {1}", new Object[]{CONFIG_FILE, e.getMessage()});
            return props; // Return empty Properties
        }
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
        logConfig(config);
        IsolationLevel isolationLevel = getIsolationLevel(config);
        int effectiveSocketTimeout = socketTimeout >= 0 ? socketTimeout : getSocketTimeout(config);
        LOGGER.log(Level.INFO, "Server configured with transaction isolation level: {0}", isolationLevel);

        running = true;
        database.loadTablesFromDisk();
        try {
            serverSocket = new ServerSocket(port);
            LOGGER.log(Level.INFO, "Database server started on port {0}", port);

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    LOGGER.log(Level.INFO, "New client connected: {0}", clientSocket.getInetAddress());
                    try {
                        executor.execute(new ClientHandler(clientSocket, database, effectiveSocketTimeout, config));
                    } catch (RejectedExecutionException e) {
                        LOGGER.log(Level.SEVERE, "Rejected connection from {0}: worker pool full ({1})",
                                new Object[]{clientSocket.getInetAddress(), e.getMessage()});
                        try {
                            clientSocket.close();
                        } catch (IOException io) {
                            LOGGER.log(Level.SEVERE, "Error closing rejected client socket: {0}", io.getMessage());
                        }
                    }
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
     * Stops accepting connections and shuts the worker pool down, giving the
     * workers up to 2 seconds to finish before forcing termination.
     */
    public void stop() {
        running = false;
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
        private final int socketTimeout;
        private ObjectOutputStream out;
        private ObjectInputStream in;
        private UUID transactionId;
        
        private static final int DEFAULT_COMPRESSION_THRESHOLD = 1024;
        private static final int DEFAULT_COMPRESSION_LEVEL = 6;
        private static final String DEFAULT_ALGORITHM = "GZIP";
        
        private int compressionThreshold;
        private int compressionLevel;
        private String compressionAlgorithm;
        private boolean compressionEnabled;
        
        // Metrics
        private long totalOriginalBytes = 0;
        private long totalCompressedBytes = 0;
        private long totalCompressionTimeNanos = 0;
        private int compressionCount = 0;

        public ClientHandler(Socket socket, Database database, int socketTimeout, Properties config) {
            this.clientSocket = socket;
            this.database = database;
            this.socketTimeout = socketTimeout;
            this.transactionId = null;
            this.compressionAlgorithm = DEFAULT_ALGORITHM;
            try {
                clientSocket.setSoTimeout(socketTimeout);
            } catch (SocketException e) {
                LOGGER.log(Level.WARNING, "Failed to set socket timeout: {0}", e.getMessage());
            }
            // Default values, will be updated by handshake
            this.compressionThreshold = Integer.parseInt(config.getProperty("compression.threshold.bytes", String.valueOf(DEFAULT_COMPRESSION_THRESHOLD)));
            this.compressionLevel = Integer.parseInt(config.getProperty("compression.level", String.valueOf(DEFAULT_COMPRESSION_LEVEL)));
            this.compressionEnabled = true;
        }

    /**
     * Performs compression handshake with client.
     * Reads client handshake message and sends server response with agreed settings.
     */
    private void performHandshake() throws IOException, ClassNotFoundException {
        // Read client handshake
        Object input = in.readObject();
        if (input instanceof CompressionHandshakeMessage handshake) {
            LOGGER.log(Level.INFO, "Received compression handshake from client: {0}", handshake);
            
            // Determine agreed settings
            boolean clientWantsCompression = handshake.isClientSupportsCompression();
            String agreedAlgorithm = DEFAULT_ALGORITHM;
            int agreedLevel = compressionLevel;
            int agreedThreshold = compressionThreshold;
            
            // Check if client supports our algorithm
            if (clientWantsCompression && handshake.getSupportedAlgorithms().contains(DEFAULT_ALGORITHM)) {
                // Use client's preferred level if reasonable (1-9)
                int clientPreferredLevel = handshake.getPreferredCompressionLevel();
                if (clientPreferredLevel >= 1 && clientPreferredLevel <= 9) {
                    agreedLevel = clientPreferredLevel;
                }
                // Use client's threshold if reasonable
                int clientThreshold = handshake.getCompressionThresholdBytes();
                if (clientThreshold > 0) {
                    agreedThreshold = clientThreshold;
                }
                compressionEnabled = true;
            } else {
                compressionEnabled = false;
            }
            
            // Update instance variables with agreed settings
            this.compressionAlgorithm = agreedAlgorithm;
            this.compressionLevel = agreedLevel;
            this.compressionThreshold = agreedThreshold;
            
            // Send response to client
            CompressionHandshakeResponse response = new CompressionHandshakeResponse(
                    compressionEnabled, agreedAlgorithm, agreedLevel, agreedThreshold);
            out.writeObject(response);
            out.flush();
            
            LOGGER.log(Level.INFO, "Sent compression handshake response: {0}", response);
        } else {
            // Client doesn't support handshake - use defaults
            LOGGER.log(Level.INFO, "Client does not support compression handshake, using defaults");
            compressionEnabled = true;
            this.compressionAlgorithm = DEFAULT_ALGORITHM;
            // Send default response for compatibility
            CompressionHandshakeResponse response = new CompressionHandshakeResponse(
                    true, DEFAULT_ALGORITHM, compressionLevel, compressionThreshold);
            out.writeObject(response);
            out.flush();
        }
    }

        /**
         * Compresses data using GZIP and collects metrics.
         */
        private byte[] compressWithMetrics(byte[] data) {
            if (!compressionEnabled || data.length <= compressionThreshold) {
                return data; // No compression
            }
            
            long startTime = System.nanoTime();
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                    gzos.write(data);
                }
                byte[] compressed = baos.toByteArray();
                long endTime = System.nanoTime();
                
                // Collect metrics
                totalOriginalBytes += data.length;
                totalCompressedBytes += compressed.length;
                totalCompressionTimeNanos += (endTime - startTime);
                compressionCount++;
                
                // Log metrics periodically
                if (compressionCount % 10 == 0) {
                    logCompressionMetrics();
                }
                
                return compressed;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Compression failed, sending uncompressed: {0}", e.getMessage());
                return data;
            }
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
                    "Compression metrics [client=%s]: count=%d, totalOriginal=%d bytes, totalCompressed=%d bytes, ratio=%f, avgTime=%fms",
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
                out.writeObject("Error: Query exceeded memory limit. Consider adding LIMIT or indexes.");
                out.flush();
            } catch (IOException io) {
                LOGGER.log(Level.SEVERE, "Error sending OOM response to client: {0}", io.getMessage());
            }
        }

        private byte[] serializeResult(Object result) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(result);
                oos.close();
                return baos.toByteArray();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error serializing query result: {0}", e.getMessage());
                return new byte[0];
            }
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(clientSocket.getOutputStream());
                in = new ObjectInputStream(clientSocket.getInputStream());

                // Perform compression handshake before starting query loop
                performHandshake();

                while (true) {
                    Object input;
                    try {
                        input = in.readObject();
                    } catch (SocketTimeoutException e) {
                        LOGGER.log(Level.WARNING, "Socket timeout while waiting for query from client {0}: {1}",
                                new Object[]{clientSocket.getInetAddress(), e.getMessage()});
                        break;
                    }
                    if (input == null || input.equals(ErrorMessages.EXIT_COMMAND)) {
                        break;
                    }

                    if (input instanceof CompressionHandshakeMessage) {
                        performHandshake();
                        continue;
                    }

                    if (!(input instanceof QueryMessage qm)) {
                        out.writeObject("Error: Invalid query message");
                        out.flush();
                        continue;
                    }

                    String query = qm.getQuery();
                    transactionId = qm.getTransactionId();

                    try {
                        Object result = database.executeQuery(query, transactionId);
                        byte[] serialized = serializeResult(result);
                        if (serialized.length > compressionThreshold) {
                            out.writeByte(0x01); // compressed marker
                            out.writeInt(serialized.length);
                            out.write(serialized);
                        } else {
                            out.writeByte(0x00); // uncompressed marker
                            out.writeInt(serialized.length);
                            out.write(serialized);
                        }
                        out.flush();
                    } catch (OutOfMemoryError e) {
                        handleOutOfMemory(query, e);
                    } catch (Exception e) {
                        out.writeObject("Error: " + e.getMessage());
                        out.flush();
                        LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}",
                                new Object[]{query, e.getMessage()});
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.log(Level.SEVERE, "Client handler error: {0}", e.getMessage());
            } finally {
                try {
                    // Rollback any active transaction for this client
                    if (transactionId != null && database.isInTransaction(transactionId)) {
                        database.executeQuery(SqlKeywords.ROLLBACK_TRANSACTION, transactionId);
                    }
                    if (out != null) out.close();
                    if (in != null) in.close();
                    if (clientSocket != null) clientSocket.close();
                    LOGGER.log(Level.INFO, "Client disconnected: {0}", clientSocket.getInetAddress());
                    logCompressionMetrics();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error closing client resources: {0}", e.getMessage());
                }
            }
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
        int port = 3306;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.SEVERE, "Invalid port {0}, using default {1}", new Object[]{args[0], port});
            }
        }
        String dataDir = ".";
        if (args.length > 1) {
            dataDir = args[1];
        }
        Database database = new Database(dataDir);
        DatabaseServer server = new DatabaseServer(port, -1, database);
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
}