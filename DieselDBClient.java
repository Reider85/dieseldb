import java.io.*;
import java.net.*;

public class DieselDBClient {
    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader in;

    public DieselDBClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println(in.readLine()); // Welcome message
    }

    public String create(String tableName) throws IOException {
        out.println("CREATE" + "§§§" + tableName);
        return in.readLine();
    }

    public String insert(String tableName, String data) throws IOException {
        out.println("INSERT" + "§§§" + tableName + "§§§" + data);
        return in.readLine();
    }

    public String select(String tableName, String condition, String orderBy) throws IOException {
        String query = condition != null ? condition : "";
        if (orderBy != null) query += " ORDER BY " + orderBy;
        out.println("SELECT" + "§§§" + tableName + "§§§" + query);
        return in.readLine();
    }

    public String update(String tableName, String condition, String updates) throws IOException {
        out.println("UPDATE" + "§§§" + tableName + "§§§" + condition + ";;;" + updates);
        return in.readLine();
    }

    public String delete(String tableName, String condition) throws IOException {
        out.println("DELETE" + "§§§" + tableName + "§§§" + condition);
        return in.readLine();
    }

    public String ping() throws IOException {
        out.println("PING");
        return in.readLine();
    }

    public void close() throws IOException {
        in.close();
        out.close();
        socket.close();
    }

    public static void main(String[] args) {
        try {
            DieselDBClient client = new DieselDBClient("localhost", 9090);
            System.out.println(client.create("test"));
            System.out.println(client.insert("test", "id:::integer:1:::name:::string:Alice"));
            System.out.println(client.select("test", null, "id ASC"));
            System.out.println(client.delete("test", "id=1"));
            System.out.println(client.select("test", null, null));
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}