import java.io.*;
import java.net.Socket;
import java.util.*;

public class DieselDBClient {
    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader in;

    public DieselDBClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String welcome = in.readLine();
        if (welcome == null || !welcome.startsWith("OK:")) {
            throw new IOException("Failed to connect to server: " + welcome);
        }
        System.out.println(welcome);
    }

    public String create(String tableName, String schema) throws IOException {
        out.println("CREATE " + tableName + " " + schema);
        out.flush();
        return in.readLine();
    }

    public String insert(String tableName, String columns, String values) throws IOException {
        out.println("INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")");
        out.flush();
        return in.readLine();
    }

    public List<Map<String, String>> select(String tableName, String condition, String orderBy) throws IOException {
        String command = "SELECT " + tableName + (condition != null ? " " + condition : "") + (orderBy != null ? " ORDER BY " + orderBy : "");
        System.out.println("Sending command: " + command);
        out.println(command);
        out.flush();
        String response = in.readLine();
        System.out.println("Received response: " + response);
        if (response == null || !response.startsWith("OK: ")) {
            throw new IOException("Select failed: " + response);
        }
        if (response.equals("OK: 0 rows")) {
            return new ArrayList<>();
        }
        String[] parts = response.substring(4).split(";;;");
        String[] headers = parts[0].split(":::");
        List<Map<String, String>> result = new ArrayList<>();
        for (int i = 1; i < parts.length; i++) {
            String[] values = parts[i].split(":::");
            Map<String, String> row = new LinkedHashMap<>();
            for (int j = 0; j < headers.length; j++) {
                row.put(headers[j], values[j]);
            }
            result.add(row);
        }
        return result;
    }

    public String update(String tableName, String condition, Map<String, String> updates) throws IOException {
        StringBuilder command = new StringBuilder("UPDATE " + tableName + " " + condition);
        command.append(";;;");
        boolean first = true;
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            if (!first) command.append(":::");
            command.append(entry.getKey()).append(":::").append(entry.getValue());
            first = false;
        }
        out.println(command.toString());
        out.flush();
        return in.readLine();
    }

    public String delete(String tableName, String condition) throws IOException {
        String command = "DELETE " + tableName + (condition != null ? " " + condition : "");
        out.println(command);
        out.flush();
        return in.readLine();
    }

    public void close() throws IOException {
        if (out != null) out.close();
        if (in != null) in.close();
        if (socket != null) socket.close();
    }
}