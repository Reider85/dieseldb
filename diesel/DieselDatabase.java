package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * Demo entry point that runs a small database scenario: it loads
 * {@code config.properties} for the transaction isolation level, starts a
 * transaction, creates a USERS table with a wide schema, inserts two rows,
 * updates and selects them, then commits. On failure the transaction is rolled
 * back.
 *
 * @see Database
 * @see IsolationLevel
 */
public class DieselDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(DieselDatabase.class);
    private static final String CONFIG_FILE = "config.properties";

    // Load configuration and return Properties object
    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = DieselDatabase.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                LOGGER.error("Configuration file {} not found", CONFIG_FILE);
                return props; // Return empty Properties
            }
            props.load(input);
            return props;
        } catch (IOException e) {
            LOGGER.error("Failed to load {}: {}", CONFIG_FILE, e.getMessage());
            return props; // Return empty Properties
        }
    }

    // Get isolation level from Properties
    private static IsolationLevel getIsolationLevel(Properties props) {
        String isolationLevelStr = props.getProperty("transaction.isolation.level", "READ_UNCOMMITTED").toUpperCase();
        try {
            return IsolationLevel.valueOf(isolationLevelStr);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid isolation level {} in {}, using default READ_UNCOMMITTED", isolationLevelStr, CONFIG_FILE);
            return IsolationLevel.READ_UNCOMMITTED;
        }
    }

    /**
     * Runs the demo scenario described in the class documentation.
     *
     * @param args not used
     */
    public static void main(String[] args) {
        // Load and log configuration parameters
        Properties config = loadConfig();
        LOGGER.info("Configuration parameters loaded from " + CONFIG_FILE + ":");
        if (config.isEmpty()) {
            LOGGER.warn("No configuration parameters found in {}", CONFIG_FILE);
        } else {
            config.forEach((key, value) ->
                    LOGGER.info("Config: {} = {}", key, value));
        }

        // Initialize database and transaction
        Database db = new Database();
        IsolationLevel isolationLevel = getIsolationLevel(config);
        LOGGER.info("Starting transaction with isolation level: {}", isolationLevel);
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
            LOGGER.info("Update query executed: {}", updateQuery);

            // Execute select query to verify update
            String selectQuery = "SELECT NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, LAST_ACTION, USER_SCORE, LEVEL, RANK, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID FROM USERS WHERE AGE > 25";
            List<Map<String, Object>> result = (List<Map<String, Object>>) db.executeQuery(selectQuery, transactionId);
            LOGGER.info("Query Result: {}", result);
            LOGGER.info("Query Result after Update:");
            for (Map<String, Object> row : result) {
                LOGGER.info(row.toString());
            }

            // Commit transaction
            db.executeQuery(SqlKeywords.COMMIT_TRANSACTION, transactionId);
        } catch (Exception e) {
            LOGGER.error("Main execution failed: {}", e.getMessage());
            try {
                db.executeQuery(SqlKeywords.ROLLBACK_TRANSACTION, transactionId);
            } catch (Exception rollbackEx) {
                LOGGER.error("Rollback failed: {}", rollbackEx.getMessage());
            }
            LOGGER.error("Main execution failed: {}", e.getMessage(), e);
        }
    }
}