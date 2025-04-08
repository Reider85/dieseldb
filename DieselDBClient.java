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

    public String setIsolation(String level) throws IOException {
        out.println("SET_ISOLATION " + level);
        return in.readLine();
    }

    public String begin() throws IOException {
        out.println("BEGIN");
        return in.readLine();
    }

    public String commit() throws IOException {
        out.println("COMMIT");
        return in.readLine();
    }

    public String rollback() throws IOException {
        out.println("ROLLBACK");
        return in.readLine();
    }

    public String create(String tableName, String schema) throws IOException {
        // Преобразуем схему из "id:integer:primary,name:string:unique" в "(id INT PRIMARY, name VARCHAR UNIQUE, ...)"
        String[] columns = schema.split(",");
        StringBuilder formattedSchema = new StringBuilder("(");
        for (int i = 0; i < columns.length; i++) {
            String[] parts = columns[i].trim().split(":");
            String columnName = parts[0];
            String type = parts[1].toUpperCase().replace("INTEGER", "INT").replace("STRING", "VARCHAR");
            String constraint = parts.length > 2 ? parts[2].toUpperCase() : "";
            formattedSchema.append(columnName).append(" ").append(type);
            if (!constraint.isEmpty()) {
                formattedSchema.append(" ").append(constraint);
            }
            if (i < columns.length - 1) {
                formattedSchema.append(", ");
            }
        }
        formattedSchema.append(")");
        String command = "CREATE " + tableName + " " + formattedSchema.toString();
        out.println(command);
        return in.readLine();
    }

    public String insert(String tableName, String columns, String values) throws IOException {
        out.println("INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")");
        return in.readLine();
    }

    public String select(String tableName, String condition, String orderBy) throws IOException {
        String query = condition != null && !condition.isEmpty() ? "WHERE " + condition : "";
        if (orderBy != null && !orderBy.isEmpty()) {
            query += (query.isEmpty() ? "" : " ") + "ORDER BY " + orderBy;
        }
        out.println("SELECT " + tableName + " " + query);
        return in.readLine();
    }

    public String update(String tableName, String condition, String updates) throws IOException {
        out.println("UPDATE " + tableName + " " + condition + ";;;" + updates);
        return in.readLine();
    }

    public String delete(String tableName, String condition) throws IOException {
        String command = "DELETE FROM " + tableName;
        if (condition != null && !condition.isEmpty()) {
            command += " WHERE " + condition;
        }
        out.println(command);
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
            DieselDBClient client = new DieselDBClient("localhost", DieselDBConfig.PORT);
            System.out.println(client.create("users", "id:integer:primary,name:string:unique,age:integer"));
            System.out.println(client.insert("users", "id, name, age", "1, Alice, 25"));
            System.out.println(client.insert("users", "id, name, age", "2, Bob, 30"));
            System.out.println(client.insert("users", "id, name, age", "1, Charlie, 35")); // Ошибка: дубликат PK
            System.out.println(client.insert("users", "id, name, age", "3, Bob, 40")); // Ошибка: дубликат unique
            System.out.println(client.select("users", null, "id ASC"));
            System.out.println(client.update("users", "id=1", "name:::string:Bob")); // Ошибка: дубликат unique
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}