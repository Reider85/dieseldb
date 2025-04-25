package diesel;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseTestClient {
    private static final Logger LOGGER = Logger.getLogger(DatabaseTestClient.class.getName());
    private final String host;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public DatabaseTestClient(String host, int port) {
        this.host = host;
        this.port = port;
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
            out.writeObject(query);
            out.flush();
            Object result = in.readObject();
            if (result instanceof String && ((String) result).startsWith("Error: ")) {
                throw new RuntimeException((String) result);
            }
            LOGGER.log(Level.INFO, "Query executed: {0}, Result: {1}", new Object[]{query, result});
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
        DatabaseTestClient client = new DatabaseTestClient("localhost", 3306);
        try {
            client.connect();

            // Create table
            String createTable = "CREATE TABLE USERS (ID, NAME, AGE)";
            client.executeQuery(createTable);

            // Insert data
            String insertQuery = "INSERT INTO USERS (ID, NAME, AGE) VALUES ('1', 'Alice', '25')";
            client.executeQuery(insertQuery);

            // Insert more data
            insertQuery = "INSERT INTO USERS (ID, NAME, AGE) VALUES ('2', 'Bob', '30')";
            client.executeQuery(insertQuery);

            // Update data
            String updateQuery = "UPDATE USERS SET AGE = '26' WHERE ID = '1'";
            client.executeQuery(updateQuery);

            // Select data
            String selectQuery = "SELECT NAME, AGE FROM USERS WHERE ID = '1'";
            List<Map<String, String>> result = (List<Map<String, String>>) client.executeQuery(selectQuery);
            System.out.println("Query Result:");
            for (Map<String, String> row : result) {
                System.out.println(row);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Client error: {0}", e.getMessage());
        } finally {
            client.disconnect();
        }
    }
}