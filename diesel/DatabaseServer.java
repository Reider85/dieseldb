package diesel;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DatabaseServer {
    private static final Logger LOGGER = Logger.getLogger(DatabaseServer.class.getName());
    private static final String CONFIG_FILE = "config.properties";
    private final int port;
    private final Database database;
    private ServerSocket serverSocket;
    private boolean running;

    public DatabaseServer(int port) {
        this.port = port;
        this.database = new Database();
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

    public void start() {
        // Load and log configuration
        Properties config = loadConfig();
        logConfig(config);

        running = true;
        try {
            serverSocket = new ServerSocket(port);
            LOGGER.log(Level.INFO, "Database server started on port {0}", port);

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    LOGGER.log(Level.INFO, "New client connected: {0}", clientSocket.getInetAddress());
                    new Thread(new ClientHandler(clientSocket, database)).start();
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
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final Database database;
        private ObjectOutputStream out;
        private ObjectInputStream in;
        private UUID transactionId;

        public ClientHandler(Socket socket, Database database) {
            this.clientSocket = socket;
            this.database = database;
            this.transactionId = null;
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(clientSocket.getOutputStream());
                in = new ObjectInputStream(clientSocket.getInputStream());

                while (true) {
                    Object input = in.readObject();
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
        DatabaseServer server = new DatabaseServer(3306);
        server.start();
    }
}