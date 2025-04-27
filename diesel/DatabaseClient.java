package diesel;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DatabaseClient {
    private static final Logger LOGGER = Logger.getLogger(DatabaseClient.class.getName());
    private final String host;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private UUID transactionId;

    public DatabaseClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.transactionId = null;
    }

    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            LOGGER.log(Level.INFO, "Connected to server at {0}:{1}", new Object[]{host, port});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to connect to server: {0}", e.getMessage());
            throw new RuntimeException("Connection failed: " + e.getMessage());
        }
    }

    public Object executeQuery(String query) {
        try {
            String normalizedQuery = query.trim();
            out.writeObject(new QueryMessage(normalizedQuery, transactionId));
            out.flush();
            Object result = in.readObject();
            if (result instanceof String && ((String) result).startsWith("Transaction started: ")) {
                transactionId = UUID.fromString(((String) result).split(": ")[1]);
            } else if (result instanceof String &&
                    (((String) result).equals("Transaction committed") || ((String) result).equals("Transaction rolled back"))) {
                transactionId = null;
            }
            if (result instanceof String && ((String) result).startsWith("Error: ")) {
                LOGGER.log(Level.SEVERE, "Server error for query '{0}': {1}", new Object[]{normalizedQuery, result});
                throw new RuntimeException((String) result);
            }
            LOGGER.log(Level.INFO, "Query executed: {0}, Result: {1}", new Object[]{normalizedQuery, result});
            return result;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}",
                    new Object[]{query, e.getMessage()});
            throw new RuntimeException("Query failed: " + e.getMessage());
        }
    }

    public void disconnect() {
        try {
            out.writeObject("EXIT");
            out.flush();
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
            LOGGER.log(Level.INFO, "Disconnected from server");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error disconnecting from server: {0}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        DatabaseClient client = new DatabaseClient("localhost", 3306);
        try {
            client.connect();

            // Try setting isolation level, but continue if unsupported
            try {
                client.executeQuery("SET TRANSACTION ISOLATION LEVEL READ_UNCOMMITTED");
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Failed to set isolation level: {0}. Continuing with default.", e.getMessage());
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

            if (result instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> rows = (List<Map<String, Object>>) result;
                System.out.println("Query Results:");
                for (Map<String, Object> row : rows) {
                    System.out.println(row);
                }
            }

            client.executeQuery("COMMIT TRANSACTION");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Client error: {0}", e.getMessage());
            e.printStackTrace();
            try {
                if (client.transactionId != null) {
                    client.executeQuery("ROLLBACK TRANSACTION");
                }
            } catch (Exception rollbackEx) {
                LOGGER.log(Level.SEVERE, "Rollback failed: {0}", rollbackEx.getMessage());
            }
        } finally {
            client.disconnect();
        }
    }
}