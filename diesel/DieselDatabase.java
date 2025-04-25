package diesel;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
// Main class for demonstration
public class DieselDatabase {
    private static final Logger LOGGER = Logger.getLogger(DieselDatabase.class.getName());

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
        try {
            List<Map<String, String>> result = db.executeQuery(query);
            LOGGER.log(Level.INFO, "Query Result: {0}", new Object[]{result});
            System.out.println("Query zondag: ");
            for (Map<String, String> row : result) {
                System.out.println(row);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Main execution failed: {0}", new Object[]{e.getMessage()});
        }
    }
}

