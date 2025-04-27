package diesel;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DatabaseClient {
    private static final Logger LOGGER = Logger.getLogger(DatabaseClient.class.getName());
    private final String host;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public DatabaseClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            LOGGER.log(Level.INFO, "Connected to server at {0}:{1}", new Object[]{host, port});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to connect to server at {0}:{1}: {2}", new Object[]{host, port, e.getMessage()});
            throw e;
        }
    }

    public void disconnect() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
            LOGGER.log(Level.INFO, "Disconnected from server at {0}:{1}", new Object[]{host, port});
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error while disconnecting: {0}", e.getMessage());
        }
    }

    public Object executeQuery(String query) {
        try {
            LOGGER.log(Level.INFO, "Executing query: {0}", query);
            out.writeObject(query);
            out.flush();
            Object response = in.readObject();
            if (response instanceof Exception) {
                LOGGER.log(Level.SEVERE, "Query failed: {0}, Error: {1}", new Object[]{query, ((Exception) response).getMessage()});
                throw new RuntimeException((Exception) response);
            }
            LOGGER.log(Level.INFO, "Query executed successfully: {0}", query);
            return response;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Error executing query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw new RuntimeException("Failed to execute query: " + query, e);
        }
    }

    public static void main(String[] args) {
        DatabaseClient client = new DatabaseClient("localhost", 3306);
        try {
            client.connect();
            // Create a table
            client.executeQuery("CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER)");
            LOGGER.log(Level.INFO, "Created table USERS");

            // Create a unique index
            client.executeQuery("CREATE UNIQUE INDEX idx_id ON USERS(ID)");
            LOGGER.log(Level.INFO, "Created unique index idx_id on USERS(ID)");

            // Insert a row
            client.executeQuery("INSERT INTO USERS (ID, NAME, AGE) VALUES ('1', 'Alice', 25)");
            LOGGER.log(Level.INFO, "Inserted user: ID=1, NAME=Alice, AGE=25");

            // Attempt to insert a duplicate ID (should fail)
            try {
                client.executeQuery("INSERT INTO USERS (ID, NAME, AGE) VALUES ('1', 'Bob', 30)");
                System.out.println("Error: Should have thrown unique constraint violation");
            } catch (RuntimeException e) {
                System.out.println("Success: " + e.getMessage());
                LOGGER.log(Level.INFO, "Caught expected unique constraint violation: {0}", e.getMessage());
            }

            // Insert a valid row
            client.executeQuery("INSERT INTO USERS (ID, NAME, AGE) VALUES ('2', 'Bob', 30)");
            LOGGER.log(Level.INFO, "Inserted user: ID=2, NAME=Bob, AGE=30");

            // Attempt to update Alice's ID to '2' (should fail due to unique constraint)
            try {
                client.executeQuery("UPDATE USERS SET ID = '2' WHERE NAME = 'Alice'");
                System.out.println("Error: Should have thrown unique constraint violation");
            } catch (RuntimeException e) {
                System.out.println("Success: " + e.getMessage());
                LOGGER.log(Level.INFO, "Caught expected unique constraint violation: {0}", e.getMessage());
            }

            // Query using the unique index
            Object result = client.executeQuery("SELECT ID, NAME, AGE FROM USERS WHERE ID = '1'");
            if (result instanceof List) {
                List<Map<String, Object>> rows = (List<Map<String, Object>>) result;
                System.out.println("Query Results:");
                for (Map<String, Object> row : rows) {
                    System.out.println(row);
                }
                LOGGER.log(Level.INFO, "Queried rows: {0}", rows);
            }

            // Test null values (should allow multiple nulls)
            client.executeQuery("INSERT INTO USERS (ID, NAME, AGE) VALUES (NULL, 'Charlie', 35)");
            client.executeQuery("INSERT INTO USERS (ID, NAME, AGE) VALUES (NULL, 'Dave', 40)");
            LOGGER.log(Level.INFO, "Inserted users with NULL IDs");

            // Query all users
            result = client.executeQuery("SELECT ID, NAME, AGE FROM USERS");
            if (result instanceof List) {
                List<Map<String, Object>> rows = (List<Map<String, Object>>) result;
                System.out.println("All Users:");
                for (Map<String, Object> row : rows) {
                    System.out.println(row);
                }
                LOGGER.log(Level.INFO, "Queried all users: {0}", rows);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test failed: {0}", e.getMessage());
            e.printStackTrace();
        } finally {
            client.disconnect();
        }
    }
}