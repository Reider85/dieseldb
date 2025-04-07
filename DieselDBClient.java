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

    public String create(String tableName, String schema) throws IOException {
        out.println("CREATE" + "§§§" + tableName + "§§§" + schema);
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
            System.out.println(client.create("users", "id:integer:primary,name:string:unique,age:integer"));
            System.out.println(client.insert("users", "id:::integer:1:::name:::string:Alice:::age:::integer:25"));
            System.out.println(client.insert("users", "id:::integer:2:::name:::string:Bob:::age:::integer:30"));
            System.out.println(client.insert("users", "id:::integer:1:::name:::string:Charlie:::age:::integer:35")); // Ошибка: дубликат PK
            System.out.println(client.insert("users", "id:::integer:3:::name:::string:Bob:::age:::integer:40")); // Ошибка: дубликат unique
            System.out.println(client.select("users", null, "id ASC"));
            System.out.println(client.update("users", "id=1", "name:::string:Bob")); // Ошибка: дубликат unique
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}