package diesel;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

public class DatabaseServer {
    private static final Logger LOGGER = Logger.getLogger(DatabaseServer.class.getName());
    private static final String CONFIG_FILE = ErrorMessages.CONFIG_FILE;
    private static final int POOL_SIZE = 100;
    private static final int QUEUE_CAPACITY = 100;
    private static final int MAX_CONNECTIONS = POOL_SIZE + QUEUE_CAPACITY;
    private static final int IDLE_TIMEOUT_MS = 30000; // 30 seconds idle timeout

    private int port;
    private Database database;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private volatile boolean running;
    private Thread selectorThread;
    private QueryExecutor queryExecutor;
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private int socketTimeout;

    // Per-connection state
    private static class ConnectionState {
        final SocketChannel channel;
        final Socket socket;
        final ByteBuffer readBuffer = ByteBuffer.allocate(8192);
        final ByteBuffer writeBuffer = ByteBuffer.allocate(8192);
        UUID transactionId;
        long lastActivityTime;
        boolean closed;

        ConnectionState(SocketChannel channel, Socket socket, UUID transactionId) {
            this.channel = channel;
            this.socket = socket;
            this.transactionId = transactionId;
            this.lastActivityTime = System.currentTimeMillis();
            this.closed = false;
        }
    }

    private final Map<SocketChannel, ConnectionState> connections = new HashMap<>();

    public DatabaseServer(int port) {
        this.port = port;
    }

    public DatabaseServer(int port, int socketTimeout) {
        this.port = port;
    }

    public DatabaseServer(int port, int socketTimeout, Database database) {
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.database = database;
        this.queryExecutor = new QueryExecutor(POOL_SIZE, database);
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

    // Handles OutOfMemoryError during query execution.
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
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
            selectorThread = new Thread(this::selectorLoop, "DatabaseServer-Selector");
            selectorThread.start();
            LOGGER.log(Level.INFO, "Database server started on port {0}", port);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server: {0}", e.getMessage());
            running = false;
        }
    }

    /**
     * The main selector event loop. Runs in a dedicated thread.
     * Handles accepting new connections, reading requests, writing responses,
     * and timing out idle connections.
     */
    private void selectorLoop() {
        try {
            while (running) {
                try {
                    // Select with timeout to allow checking running flag and idle connections
                    int readyCount = selector.select(IDLE_TIMEOUT_MS);
                    
                    // Process idle connections
                    if (running) {
                        processIdleConnections();
                    }
                    
                    if (readyCount == 0) {
                        // Timeout - just continue to check running flag and idle connections again
                        continue;
                    }
                    
                    // Process selected keys
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext() && running) {
                        SelectionKey key = keys.next();
                        keys.remove();
                        
                        if (!key.isValid()) {
                            continue;
                        }
                        
                        try {
                            if (key.isAcceptable()) {
                                acceptConnection(key);
                            } else if (key.isReadable()) {
                                readFromConnection(key);
                            } else if (key.isWritable()) {
                                writeToConnection(key);
                            }
                        } catch (IOException e) {
                            LOGGER.log(Level.WARNING, "I/O error on connection: {0}", e.getMessage());
                            closeConnection(key);
                        }
                    }
                } catch (Exception e) {
                    if (running) {
                        LOGGER.log(Level.SEVERE, "Selector error: {0}", e.getMessage());
                    }
                    // Continue loop if still running
                }
            }
        } finally {
            // Cleanup resources
            try {
                selector.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing selector: {0}", e.getMessage());
            }
            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing server socket channel: {0}", e.getMessage());
            }
            // Close all connections
            for (ConnectionState state : connections.values()) {
                try {
                    if (state.socket != null) {
                        state.socket.close();
                    }
                } catch (IOException e) {
                    // Ignore
                }
            }
            connections.clear();
            LOGGER.log(Level.INFO, "Selector loop terminated");
        }
    }

    /**
     * Processes idle connections and closes those that have exceeded IDLE_TIMEOUT_MS.
     */
    private void processIdleConnections() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<SocketChannel, ConnectionState>> iter = connections.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<SocketChannel, ConnectionState> entry = iter.next();
            ConnectionState state = entry.getValue();
            if (now - state.lastActivityTime > IDLE_TIMEOUT_MS) {
                try {
                    SocketAddress remoteAddr = state.channel.getRemoteAddress();
                    LOGGER.log(Level.FINE, "Closing idle connection: {0}", remoteAddr != null ? remoteAddr.toString() : "unknown");
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Error getting remote address: {0}", e.getMessage());
                    LOGGER.log(Level.FINE, "Closing idle connection: {0}", "unknown");
                }
                closeConnection(state.channel);
                iter.remove(); // Remove from map
            }
        }
    }

    /**
     * Accepts a new connection and registers it with the selector for reading.
     */
    private void acceptConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        
        if (clientChannel == null) {
            return; // No connection pending
        }
        
        // Check connection limit
        if (connectionCount.get() >= MAX_CONNECTIONS) {
            LOGGER.log(Level.WARNING, "Connection limit exceeded ({0}), rejecting connection from {1}",
                    new Object[]{MAX_CONNECTIONS, clientChannel.getRemoteAddress()});
            clientChannel.close();
            return;
        }
        
        // Configure the channel
        clientChannel.configureBlocking(false);
        
        // Get the socket and set options
        Socket socket = clientChannel.socket();
        if (socketTimeout >= 0) {
            socket.setSoTimeout(socketTimeout);
        }
        // Set TCP_NODELAY for better latency (disable Nagle's algorithm)
        socket.setTcpNoDelay(true);
        
        // Create connection state
        ConnectionState state = new ConnectionState(clientChannel, socket, null);
        
        // Register for reading
        SelectionKey clientKey = clientChannel.register(selector, SelectionKey.OP_READ);
        clientKey.attach(state);
        
        // Increment connection counter and store in map
        if (connectionCount.incrementAndGet() > MAX_CONNECTIONS) {
            // This shouldn't happen due to check above, but just in case
            connectionCount.decrementAndGet();
            clientChannel.close();
            return;
        }
        
        connections.put(clientChannel, state);
        
        LOGGER.log(Level.INFO, "New client connected: {0} (total: {1})",
                new Object[]{clientChannel.getRemoteAddress(), connectionCount.get()});
    }

    /**
     * Reads data from a connection and deserializes a QueryMessage.
     * Uses the Socket's ObjectInputStream for backward compatibility with DatabaseClient.
     * The selector tells us when data is available, and we read using the traditional streams.
     */
    private void readFromConnection(SelectionKey key) throws IOException, ClassNotFoundException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionState state = (ConnectionState) key.attachment();
        
        if (state == null || state.closed) {
            key.cancel();
            channel.close();
            return;
        }
        
        // Update last activity time
        state.lastActivityTime = System.currentTimeMillis();
        
        try {
            // For backward compatibility, use the socket's ObjectInputStream
            // The selector tells us when data is available, and we read using the traditional streams
            try (ObjectInputStream in = new ObjectInputStream(state.socket.getInputStream())) {
                Object input = in.readObject();
                
                // Process the QueryMessage
                if (input instanceof QueryMessage queryMessage) {
                    processQueryMessage(channel, state, queryMessage);
                } else if (input instanceof String && ((String) input).equals(ErrorMessages.EXIT_COMMAND)) {
                    // EXIT command
                    LOGGER.log(Level.FINE, "Received EXIT command from {0}", channel.getRemoteAddress());
                    closeConnection(key);
                } else {
                    // Invalid message type
                    LOGGER.log(Level.WARNING, "Invalid message type received from {0}: {1}",
                            new Object[]{channel.getRemoteAddress(), input.getClass().getName()});
                    writeError(channel, state, "Error: Invalid query message");
                }
            }
        } catch (EOFException e) {
            // End of stream - client closed connection
            LOGGER.log(Level.FINE, "Client closed connection: {0}", channel.getRemoteAddress());
            closeConnection(key);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Class not found reading from {0}: {1}", 
                    new Object[]{channel.getRemoteAddress(), e.getMessage()});
            closeConnection(key);
        }
    }

    /**
     * Processes a QueryMessage by submitting it to the query executor.
     */
    private void processQueryMessage(SocketChannel channel, ConnectionState state, QueryMessage queryMessage) {
        List<String> queries = queryMessage.getQueries();
        UUID transactionId = queryMessage.getTransactionId();
        
        // Store transaction ID in connection state
        state.transactionId = transactionId;
        
        // Execute queries in parallel when they are independent
        try {
            List<Object> results = queryExecutor.executeQueries(queries, transactionId);
            
            // For backward compatibility, if there was only one query, send just that result
            // Otherwise, send the list of results
            Object response;
            if (results.size() == 1) {
                response = results.get(0);
            } else {
                response = results;
            }
            
            // Serialize the response using ObjectOutputStream to the socket's output stream
            try (ObjectOutputStream out = new ObjectOutputStream(state.socket.getOutputStream())) {
                out.writeObject(response);
                out.flush();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error writing response to {0}: {1}", 
                        new Object[]{channel.getRemoteAddress(), e.getMessage()});
            }
        } catch (OutOfMemoryError e) {
            String firstQuery = queries.isEmpty() ? "" : queries.get(0);
            handleOutOfMemory(firstQuery, e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}",
                    new Object[]{queries.isEmpty() ? "" : queries.get(0), e.getMessage()});
            try {
                ObjectOutputStream out = new ObjectOutputStream(state.socket.getOutputStream());
                out.writeObject("Error: " + e.getMessage());
                out.flush();
            } catch (IOException io) {
                LOGGER.log(Level.SEVERE, "Error writing error response: {0}", io.getMessage());
            }
        }
    }

    /**
     * Writes an error message to the connection.
     */
    private void writeError(SocketChannel channel, ConnectionState state, String errorMessage) {
        try (ObjectOutputStream out = new ObjectOutputStream(state.socket.getOutputStream())) {
            out.writeObject(errorMessage);
            out.flush();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error preparing error message: {0}", e.getMessage());
            closeConnection(channel.keyFor(selector));
        }
    }

    /**
     * Writes data from a connection's write buffer to the socket.
     */
    private void writeToConnection(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionState state = (ConnectionState) key.attachment();
        
        if (state == null || state.closed) {
            key.cancel();
            channel.close();
            return;
        }
        
        // For this implementation, writes happen directly when processing messages
        // The OP_WRITE interest is not needed since we write after each operation
        // Just ensure the key remains readable
        int interestOps = key.interestOps();
        // Keep OP_READ enabled
        if ((interestOps & SelectionKey.OP_READ) == 0) {
            key.interestOps(interestOps | SelectionKey.OP_READ);
        }
    }

    /**
     * Closes a connection and cleans up associated resources.
     */
    private void closeConnection(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        closeConnection(channel);
    }

    /**
     * Closes a connection and cleans up associated resources.
     */
    private void closeConnection(SocketChannel channel) {
        ConnectionState state = connections.remove(channel);
        if (state != null) {
            state.closed = true;
            connectionCount.decrementAndGet();
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing channel: {0}", e.getMessage());
            }
            try {
                if (state.socket != null) {
                    state.socket.close();
                }
            } catch (IOException e) {
                // Ignore
            }
            LOGGER.log(Level.FINE, "Connection closed: {0} (remaining: {1})",
                    new Object[]{state.socket != null ? state.socket.getInetAddress().getHostAddress() : "unknown", connectionCount.get()});
        }
        
        // Cancel the selection key
        SelectionKey key = channel.keyFor(selector);
        if (key != null) {
            key.cancel();
        }
    }

    /**
     * Stops accepting connections, closes all existing connections, and shuts down
     * the selector thread and executors.
     */
    public void stop() {
        running = false;
        
        // Wake up the selector thread if it's blocked
        if (selector != null) {
            selector.wakeup();
        }
        
        try {
            if (selectorThread != null) {
                selectorThread.join(5000); // Wait up to 5 seconds for thread to terminate
                if (selectorThread.isAlive()) {
                    LOGGER.log(Level.WARNING, "Selector thread did not terminate within timeout");
                    selectorThread.interrupt();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Close server socket channel (will cause accept to return null)
        try {
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error closing server socket channel: {0}", e.getMessage());
        }
        
        // Close all connections
        for (SocketChannel channel : new ArrayList<>(connections.keySet())) {
            closeConnection(channel);
        }
        
        LOGGER.log(Level.INFO, "Database server stopped");
        
        // Shut down the query executor
        if (queryExecutor != null) {
            queryExecutor.shutdown();
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
        try {
            Database database = new Database(dataDir);
            DatabaseServer server = new DatabaseServer(port, -1, database);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.log(Level.INFO, "Shutdown hook triggered, stopping server gracefully");
                try {
                    if (server.serverSocketChannel != null && server.serverSocketChannel.isOpen()) {
                        server.serverSocketChannel.close();
                        LOGGER.log(Level.INFO, "ServerSocketChannel closed, no longer accepting new connections");
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error closing ServerSocketChannel in shutdown hook: {0}", e.getMessage());
                }
                server.stop();
            }, "shutdown-hook"));
            server.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create server: {0}", e.getMessage());
        }
    }
}