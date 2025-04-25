package diesel;
import java.io.*;
import java.net.*;
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
        DatabaseClient client = new DatabaseClient("localhost", 3306);
        try {
            client.connect();

            // Create table with types
            String createTable = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, LAST_ACTION DATETIME_MS, USER_SCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR)";
            client.executeQuery(createTable);

            // Insert data
            String insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL) VALUES ('1', 'Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', '2023-10-15 14:30:00.123', 9223372036854775807, 100, 1, 123.45, 99.75, 123456.789012, 'A')";
            client.executeQuery(insertQuery);

            // Insert more data
            insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL) VALUES ('2', 'Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', '2023-10-16 09:00:00.456', 1000000000, 200, 2, 678.90, 88.50, 987654.321098, 'B')";
            client.executeQuery(insertQuery);

            // Update data
            String updateQuery = "UPDATE USERS SET INITIAL = 'C' WHERE ID = '1'";
            client.executeQuery(updateQuery);

            // Select data
            String selectQuery = "SELECT NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL FROM USERS WHERE INITIAL = 'B'";
            List<Map<String, Object>> result = (List<Map<String, Object>>) client.executeQuery(selectQuery);
            System.out.println("Query Result:");
            for (Map<String, Object> row : result) {
                System.out.println(row);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Client error: {0}", e.getMessage());
        } finally {
            client.disconnect();
        }
    }
}