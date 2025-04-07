import java.io.*;
import java.net.*;

public class DieselDBClient {
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;
    private static final String TABLE_NAME_1 = "users";
    private static final String TABLE_NAME_2 = "orders";

    public DieselDBClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setKeepAlive(true);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String welcomeMessage = in.readLine();
        System.out.println("Connected to server: " + welcomeMessage);
    }

    private void checkResponse(String response) throws IOException {
        if (response == null || response.startsWith("ERROR")) {
            throw new IOException("Server error: " + (response != null ? response : "No response from server"));
        }
    }

    private String sendCommandWithRetry(String command) throws IOException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                out.println(command);
                out.flush();
                return in.readLine();
            } catch (IOException e) {
                retries++;
                if (retries == MAX_RETRIES) {
                    throw new IOException("Failed to send command after " + MAX_RETRIES + " retries: " + e.getMessage());
                }
                System.err.println("Connection error, retrying (" + retries + "/" + MAX_RETRIES + "): " + e.getMessage());
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                    reconnect();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted: " + ie.getMessage());
                }
            }
        }
        return null;
    }

    private void reconnect() throws IOException {
        close();
        connect();
    }

    public void create(String tableName) throws IOException {
        String command = "CREATE§§§" + tableName;
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Create response: " + response);
    }

    public void insert(String tableName, String data) throws IOException {
        String command = "INSERT§§§" + tableName + "§§§" + data;
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Insert response: " + response);
    }

    public String select(String tableName, String condition) throws IOException {
        String command = "SELECT§§§" + tableName + (condition != null ? "§§§" + condition : "");
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        return response;
    }

    public void update(String tableName, String condition, String updates) throws IOException {
        String command = "UPDATE§§§" + tableName + "§§§" + condition + ";;;" + updates;
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Update response: " + response);
    }

    public void delete(String tableName, String condition) throws IOException {
        String command = "DELETE§§§" + tableName + (condition != null ? "§§§" + condition : "");
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Delete response: " + response);
    }

    public String join(String leftTable, String rightTable, String joinCondition, String whereCondition) throws IOException {
        String command = "SELECT§§§" + leftTable + "§§§JOIN§§§" + rightTable + "§§§" + joinCondition +
                (whereCondition != null ? "§§§" + whereCondition : "");
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        return response;
    }

    public void clearMemory() throws IOException {
        String command = "CLEAR_MEMORY";
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Memory cleanup response: " + response);
    }

    public void ping() throws IOException {
        String command = "PING";
        String response = sendCommandWithRetry(command);
        checkResponse(response);
        System.out.println("Ping response: " + response);
    }

    public void close() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    public void testDatabaseOperations() throws IOException {
        create(TABLE_NAME_1);
        create(TABLE_NAME_2);

        insert(TABLE_NAME_1, "id:::integer:1:::name:::string:Alice:::age:::integer:25");
        insert(TABLE_NAME_1, "id:::integer:2:::name:::string:Bob:::age:::integer:30");

        insert(TABLE_NAME_2, "order_id:::integer:101:::user_id:::integer:1:::amount:::bigdecimal:99.99");
        insert(TABLE_NAME_2, "order_id:::integer:102:::user_id:::integer:2:::amount:::bigdecimal:149.50");

        String selectUsers = select(TABLE_NAME_1, "age>=25");
        System.out.println("Select users (age >= 25): " + selectUsers);

        update(TABLE_NAME_1, "id=1", "age:::integer:26");
        String updatedUsers = select(TABLE_NAME_1, "id=1");
        System.out.println("Updated user: " + updatedUsers);

        String joinResult = join(TABLE_NAME_1, TABLE_NAME_2, "id=user_id", null);
        System.out.println("Join result: " + joinResult);

        delete(TABLE_NAME_2, "order_id=101");
        String remainingOrders = select(TABLE_NAME_2, null);
        System.out.println("Remaining orders: " + remainingOrders);

        clearMemory();
    }

    public static void main(String[] args) {
        try {
            DieselDBClient client = new DieselDBClient("localhost", 9090);
            try {
                client.ping();
                client.testDatabaseOperations();
            } finally {
                client.close();
            }
        } catch (IOException e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}