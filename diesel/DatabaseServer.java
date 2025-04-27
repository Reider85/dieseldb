package diesel;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DatabaseServer {
    private static final Logger LOGGER = Logger.getLogger(DatabaseServer.class.getName());
    private final int port;
    private final Database database;
    private ServerSocket serverSocket;
    private boolean running;

    public DatabaseServer(int port) {
        this.port = port;
        this.database = new Database();
    }

    public void start() {
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

class QueryMessage implements Serializable {
    private final String query;
    private final UUID transactionId;

    public QueryMessage(String query, UUID transactionId) {
        this.query = query;
        this.transactionId = transactionId;
    }

    public String getQuery() {
        return query;
    }

    public UUID getTransactionId() {
        return transactionId;
    }
}