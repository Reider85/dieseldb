package diesel;
import java.util.*;
// Main class for demonstration
public class DieselDatabase {
    public static void main(String[] args) {
        Database db = new Database();

        // Create table
        List<String> columns = Arrays.asList("ID", "NAME", "AGE");
        db.createTable("USERS", columns);

        // Insert data
        Map<String, String> row1 = new HashMap<>();
        row1.put("ID", "1");
        row1.put("NAME", "Alice");
        row1.put("AGE", "25");
        db.insert("USERS", row1);

        Map<String, String> row2 = new HashMap<>();
        row2.put("ID", "2");
        row2.put("NAME", "Bob");
        row2.put("AGE", "30");
        db.insert("USERS", row2);

        // Execute query
        String query = "SELECT NAME, AGE FROM USERS WHERE AGE = '25'";
        List<Map<String, String>> result = db.executeQuery(query);

        // Print result
        System.out.println("Query Result:");
        for (Map<String, String> row : result) {
            System.out.println(row);
        }
    }
}
