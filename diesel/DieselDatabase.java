package diesel;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DieselDatabase {
    private static final Logger LOGGER = Logger.getLogger(DieselDatabase.class.getName());
    private static final String CONFIG_FILE = "config.properties";

    // Load configuration and return Properties object
    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = DieselDatabase.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                LOGGER.log(Level.SEVERE, "Configuration file {0} not found", CONFIG_FILE);
                return props; // Return empty Properties
            }
            props.load(input);
            return props;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load {0}: {1}", new Object[]{CONFIG_FILE, e.getMessage()});
            return props; // Return empty Properties
        }
    }

    // Get isolation level from Properties
    private static IsolationLevel getIsolationLevel(Properties props) {
        String isolationLevelStr = props.getProperty("transaction.isolation.level", "READ_UNCOMMITTED").toUpperCase();
        try {
            return IsolationLevel.valueOf(isolationLevelStr);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Invalid isolation level {0} in {1}, using default READ_UNCOMMITTED", new Object[]{isolationLevelStr, CONFIG_FILE});
            return IsolationLevel.READ_UNCOMMITTED;
        }
    }

    public static void main(String[] args) {
        // Load and log configuration parameters
        Properties config = loadConfig();
        LOGGER.log(Level.INFO, "Configuration parameters loaded from {0}:", CONFIG_FILE);
        if (config.isEmpty()) {
            LOGGER.log(Level.WARNING, "No configuration parameters found in {0}", CONFIG_FILE);
        } else {
            config.forEach((key, value) ->
                    LOGGER.log(Level.INFO, "Config: {0} = {1}", new Object[]{key, value}));
        }

        // Initialize database and transaction
        Database db = new Database();
        IsolationLevel isolationLevel = getIsolationLevel(config);
        LOGGER.log(Level.INFO, "Starting transaction with isolation level: {0}", isolationLevel);
        UUID transactionId = db.beginTransaction(isolationLevel); // Start transaction with configured isolation level
        try {
            // Create table with types
            String createTableQuery = "CREATE TABLE USERS (ID STRING, NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, LAST_ACTION DATETIME_MS, USER_SCORE LONG, LEVEL SHORT, RANK BYTE, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)";
            db.executeQuery(createTableQuery, transactionId);

            // Insert data via query
            String insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('1', 'Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', '2023-10-15 14:30:00.123', 9223372036854775807, 100, 1, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')";
            db.executeQuery(insertQuery, transactionId);

            // Insert more data via query
            insertQuery = "INSERT INTO USERS (ID, NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('2', 'Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', '2023-10-16 09:00:00.456', 1000000000, 200, 2, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000')";
            db.executeQuery(insertQuery, transactionId);

            // Update data via query
            String updateQuery = "UPDATE USERS SET INITIAL = 'C' WHERE AGE < 30";
            db.executeQuery(updateQuery, transactionId);
            LOGGER.log(Level.INFO, "Update query executed: {0}", new Object[]{updateQuery});

            // Execute select query to verify update
            String selectQuery = "SELECT NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID FROM USERS WHERE AGE > 25";
            List<Map<String, Object>> result = (List<Map<String, Object>>) db.executeQuery(selectQuery, transactionId);
            LOGGER.log(Level.INFO, "Query Result: {0}", new Object[]{result});
            System.out.println("Query Result after Update:");
            for (Map<String, Object> row : result) {
                System.out.println(row);
            }

            // Commit transaction
            db.executeQuery("COMMIT TRANSACTION", transactionId);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Main execution failed: {0}", new Object[]{e.getMessage()});
            try {
                db.executeQuery("ROLLBACK TRANSACTION", transactionId);
            } catch (Exception rollbackEx) {
                LOGGER.log(Level.SEVERE, "Rollback failed: {0}", rollbackEx.getMessage());
            }
            e.printStackTrace();
        }
    }
}