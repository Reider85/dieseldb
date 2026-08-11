package diesel;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DatabaseServer {
    private static final Logger LOGGER = Logger.getLogger(DatabaseServer.class.getName());
    private static final String CONFIG_FILE = "config.properties";
    private static final int POOL_SIZE = 100;
    private static final int QUEUE_CAPACITY = 100;
    private final int port;
    private final Database database;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final ThreadPoolExecutor executor;
    private final int socketTimeout;

    public DatabaseServer(int port) {
        this(port, -1);
    }

    public DatabaseServer(int port, int socketTimeout) {
        this(port, socketTimeout, new Database());
    }

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
                        executor.execute(new ClientHandler(clientSocket, database, effectiveSocketTimeout));
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

        public ClientHandler(Socket socket, Database database, int socketTimeout) {
            this.clientSocket = socket;
            this.database = database;
            this.socketTimeout = socketTimeout;
            this.transactionId = null;
            try {
                clientSocket.setSoTimeout(socketTimeout);
            } catch (SocketException e) {
                LOGGER.log(Level.WARNING, "Failed to set socket timeout: {0}", e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(clientSocket.getOutputStream());
                in = new ObjectInputStream(clientSocket.getInputStream());

                while (true) {
                    Object input;
                    try {
                        input = in.readObject();
                    } catch (SocketTimeoutException e) {
                        LOGGER.log(Level.WARNING, "Socket timeout while waiting for query from client {0}: {1}",
                                new Object[]{clientSocket.getInetAddress(), e.getMessage()});
                        break;
                    }
                    if (input == null || input.equals("EXIT")) {
                        break;
                    }

                    if (!(input instanceof QueryMessage)) {
                        out.writeObject("Error: Invalid query message");
                        out.flush();
                        continue;
                    }

                    QueryMessage queryMessage = (QueryMessage) input;
                    String query = queryMessage.getQuery();
                    transactionId = queryMessage.getTransactionId();

                    try {
                        Object result = database.executeQuery(query, transactionId);
                        out.writeObject(result);
                        out.flush();
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
                        database.executeQuery("ROLLBACK TRANSACTION", transactionId);
                    }
                    if (out != null) out.close();
                    if (in != null) in.close();
                    if (clientSocket != null) clientSocket.close();
                    LOGGER.log(Level.INFO, "Client disconnected: {0}", clientSocket.getInetAddress());
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error closing client resources: {0}", e.getMessage());
                }
            }
        }
    }

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