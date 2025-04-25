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

        // Insert data via query
        String insertQuery = "INSERT INTO USERS (ID, NAME, AGE) VALUES ('1', 'Alice', '25')";
        try {
            db.executeQuery(insertQuery);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert query failed: {0}", new Object[]{e.getMessage()});
        }

        // Insert more data via query
        insertQuery = "INSERT INTO USERS (ID, NAME, AGE) VALUES ('2', 'Bob', '30')";
        try {
            db.executeQuery(insertQuery);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert query failed: {0}", new Object[]{e.getMessage()});
        }

        // Execute select query
        String selectQuery = "SELECT NAME, AGE FROM USERS WHERE AGE = '25'";
        try {
            List<Map<String, String>> result = db.executeQuery(selectQuery);
            LOGGER.log(Level.INFO, "Query Result: {0}", new Object[]{result});
            System.out.println("Query Result:");
            for (Map<String, String> row : result) {
                System.out.println(row);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Main execution failed: {0}", new Object[]{e.getMessage()});
        }
    }
}

