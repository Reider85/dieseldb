package diesel;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class DieselDatabase {
    private static final Logger LOGGER = Logger.getLogger(DieselDatabase.class.getName());

    public static void main(String[] args) {
        Database db = new Database();

        // Create table with types
        String createTableQuery = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME)";
        try {
            db.executeQuery(createTableQuery);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Create table query failed: {0}", new Object[]{e.getMessage()});
        }

        // Insert data via query
        String insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN) VALUES ('1', 'Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00')";
        try {
            db.executeQuery(insertQuery);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert query failed: {0}", new Object[]{e.getMessage()});
        }

        // Insert more data via query
        insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN) VALUES ('2', 'Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00')";
        try {
            db.executeQuery(insertQuery);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Insert query failed: {0}", new Object[]{e.getMessage()});
        }

        // Update data via query
        String updateQuery = "UPDATE USERS SET LAST_LOGIN = '2023-10-15 15:00:00' WHERE ID = '1'";
        try {
            db.executeQuery(updateQuery);
            LOGGER.log(Level.INFO, "Update query executed: {0}", new Object[]{updateQuery});
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Update query failed: {0}", new Object[]{e.getMessage()});
        }

        // Execute select query to verify update
        String selectQuery = "SELECT NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN FROM USERS WHERE LAST_LOGIN = '2023-10-16 09:00:00'";
        try {
            List<Map<String, Object>> result = (List<Map<String, Object>>) db.executeQuery(selectQuery);
            LOGGER.log(Level.INFO, "Query Result: {0}", new Object[]{result});
            System.out.println("Query Result after Update:");
            for (Map<String, Object> row : result) {
                System.out.println(row);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Main execution failed: {0}", new Object[]{e.getMessage()});
        }
    }
}