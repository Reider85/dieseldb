package diesel;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Locale;

public class OrderByTest {
    private static final Logger LOGGER = Logger.getLogger(OrderByTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;
    private static final SimpleDateFormat TIMESTAMP_MS_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public OrderByTest() {
        this.database = new Database();
    }

    public void runTests() {
        try {
            // Step 1: Create tables and indexes, insert records
            createTable();
            createUniqueIndex();
            createBTreeIndex();
            createHashIndex();
            createUniqueClusteredIndex();
            createJoinTable();
            insertRecords();
            insertJoinRecords();

            // Step 2: Run SELECT queries with ORDER BY
            selectOrderByString();
            selectOrderByStringDesc();
            selectOrderByInteger();
            selectOrderByIntegerDesc();
            selectOrderByLong();
            selectOrderByLongDesc();
            selectOrderByByte();
            selectOrderByByteDesc();
            selectOrderByShort();
            selectOrderByShortDesc();
            selectOrderByFloat();
            selectOrderByFloatDesc();
            selectOrderByDouble();
            selectOrderByDoubleDesc();
            selectOrderByBigDecimal();
            selectOrderByBigDecimalDesc();
            selectOrderByChar();
            selectOrderByCharDesc();
            selectOrderByDate();
            selectOrderByDateDesc();
            selectOrderByDateTime();
            selectOrderByDateTimeDesc();
            selectOrderByDateTimeMillis();
            selectOrderByDateTimeMillisDesc();
            selectJoinPrimaryKeyOrderByPrimaryKey();
            selectJoinBTreeIndexOrderByBTree();
            selectJoinHashIndexOrderByHash();
            selectJoinUniqueIndexOrderByUnique();
            selectJoinNonIndexedOrderByNonIndexed();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createTable");
            dropTable();
            String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, BYTE_FIELD BYTE, SHORT_FIELD SHORT, FLOAT_FIELD FLOAT, DOUBLE_FIELD DOUBLE, CHAR_FIELD CHAR, DATE_FIELD DATE, DATETIME_FIELD DATETIME, DATETIME_MILLIS_FIELD DATETIME_MS)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Test createTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,  "Test createTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createJoinTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createJoinTable");
            dropJoinTable();
            String createTableQuery = "CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Test createJoinTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createJoinTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
            LOGGER.log(Level.INFO, "Table USERS dropped");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table USERS not found for dropping");
        }
    }

    private void dropJoinTable() {
        try {
            database.dropTable("PROFILES");
            LOGGER.log(Level.INFO, "Table PROFILES dropped");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Table PROFILES not found for dropping");
        }
    }

    private void createUniqueIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUniqueIndex");
            String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createUniqueIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createUniqueIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createBTreeIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createBTreeIndex");
            String createIndexQuery = "CREATE INDEX ON USERS (AGE)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createBTreeIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createBTreeIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createHashIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createHashIndex");
            String createIndexQuery = "CREATE HASH INDEX ON USERS (NAME)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createHashIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createHashIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createUniqueClusteredIndex() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createUniqueClusteredIndex");
            String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (USER_CODE)";
            LOGGER.log(Level.INFO, "Executing: {0}", createIndexQuery);
            database.executeQuery(createIndexQuery, null);
            LOGGER.log(Level.INFO, "Test createUniqueClusteredIndex: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createUniqueClusteredIndex: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertRecords() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertRecords");
            LOGGER.log(Level.INFO, "Inserting {0} records", RECORD_COUNT);
            Table table = database.getTable("USERS");

            long startTime = System.nanoTime();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                Timestamp datetime = new Timestamp(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000));
                Timestamp datetimeMillis = new Timestamp(System.currentTimeMillis() - (i * 1000) + (i % 1000));
                String query = String.format(Locale.US,
                        "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, BYTE_FIELD, SHORT_FIELD, FLOAT_FIELD, DOUBLE_FIELD, CHAR_FIELD, DATE_FIELD, DATETIME_FIELD, DATETIME_MILLIS_FIELD) " +
                                "VALUES ('CODE%d', 'User%d', %d, %s, %d, %d, %f, %f, '%c', '%s', '%s', '%s')",
                        i, i, 18 + (i % 82),
                        new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP),
                        (byte) (i % 127), (short) (i % 32767), (float) (i % 1000) / 10.0, (double) (i % 1000) / 10.0,
                        (char) ('A' + (i % 26)),
                        new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)),
                        TIMESTAMP_MS_FORMATTER.format(datetime),
                        TIMESTAMP_MS_FORMATTER.format(datetimeMillis)
                );
                database.executeQuery(query, null);
            }
            table.saveToFile("USERS");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
            LOGGER.log(Level.INFO, "Test insertRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertRecords: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void insertJoinRecords() {
        try {
            LOGGER.log(Level.INFO, "Starting test: insertJoinRecords");
            LOGGER.log(Level.INFO, "Inserting {0} records into PROFILES", RECORD_COUNT);
            Table table = database.getTable("PROFILES");

            long startTime = System.nanoTime();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                String query = String.format(
                        "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED) " +
                                "VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d')",
                        i, 18 + (i % 82), i, i, i
                );
                database.executeQuery(query, null);
            }
            table.saveToFile("PROFILES");
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            LOGGER.log(Level.INFO, "Inserted {0} records in {1} ms", new Object[]{RECORD_COUNT, String.format("%.3f", durationMs)});
            LOGGER.log(Level.INFO, "Test insertJoinRecords: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test insertJoinRecords: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private Object executeSelectQuery(String query, String orderByField) {
        LOGGER.log(Level.INFO, "Executing SELECT: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        List<?> resultList = (List<?>) result;
        LOGGER.log(Level.INFO, "Result: {0} rows, Time: {1} ms", new Object[]{resultList.size(), String.format("%.3f", durationMs)});
        if (!resultList.isEmpty()) {
            Object first = extractField(resultList.get(0), orderByField);
            Object last = extractField(resultList.get(resultList.size() - 1), orderByField);
            LOGGER.log(Level.INFO, "First: {0}, Last: {1}", new Object[]{first, last});
        }
        return result;
    }

    private Object extractField(Object row, String field) {
        if (row instanceof Map) {
            return ((Map<?, ?>) row).get(field);
        }
        return null; // Adjust based on actual row structure
    }

    private void selectOrderByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByString");
            String query = "SELECT ID, NAME FROM USERS ORDER BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectOrderByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByStringDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByStringDesc");
            String query = "SELECT ID, NAME FROM USERS ORDER BY NAME DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectOrderByStringDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByStringDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByInteger() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByInteger");
            String query = "SELECT ID, AGE FROM USERS ORDER BY AGE";
            executeSelectQuery(query, "AGE");
            LOGGER.log(Level.INFO, "Test selectOrderByInteger: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByInteger: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByIntegerDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByIntegerDesc");
            String query = "SELECT ID, AGE FROM USERS ORDER BY AGE DESC";
            executeSelectQuery(query, "AGE");
            LOGGER.log(Level.INFO, "Test selectOrderByIntegerDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByIntegerDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByLong() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByLong");
            String query = "SELECT ID, ID FROM USERS ORDER BY ID";
            executeSelectQuery(query, "ID");
            LOGGER.log(Level.INFO, "Test selectOrderByLong: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByLong: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByLongDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByLongDesc");
            String query = "SELECT ID, ID FROM USERS ORDER BY ID DESC";
            executeSelectQuery(query, "ID");
            LOGGER.log(Level.INFO, "Test selectOrderByLongDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByLongDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByByte() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByByte");
            String query = "SELECT ID, BYTE_FIELD FROM USERS ORDER BY BYTE_FIELD";
            executeSelectQuery(query, "BYTE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByByte: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByByte: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByByteDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByByteDesc");
            String query = "SELECT ID, BYTE_FIELD FROM USERS ORDER BY BYTE_FIELD DESC";
            executeSelectQuery(query, "BYTE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByByteDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByByteDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByShort() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByShort");
            String query = "SELECT ID, SHORT_FIELD FROM USERS ORDER BY SHORT_FIELD";
            executeSelectQuery(query, "SHORT_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByShort: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByShort: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByShortDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByShortDesc");
            String query = "SELECT ID, SHORT_FIELD FROM USERS ORDER BY SHORT_FIELD DESC";
            executeSelectQuery(query, "SHORT_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByShortDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByShortDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByFloat() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByFloat");
            String query = "SELECT ID, FLOAT_FIELD FROM USERS ORDER BY FLOAT_FIELD";
            executeSelectQuery(query, "FLOAT_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByFloat: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByFloat: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByFloatDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByFloatDesc");
            String query = "SELECT ID, FLOAT_FIELD FROM USERS ORDER BY FLOAT_FIELD DESC";
            executeSelectQuery(query, "FLOAT_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByFloatDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByFloatDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDouble() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDouble");
            String query = "SELECT ID, DOUBLE_FIELD FROM USERS ORDER BY DOUBLE_FIELD";
            executeSelectQuery(query, "DOUBLE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDouble: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDouble: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDoubleDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDoubleDesc");
            String query = "SELECT ID, DOUBLE_FIELD FROM USERS ORDER BY DOUBLE_FIELD DESC";
            executeSelectQuery(query, "DOUBLE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDoubleDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDoubleDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByBigDecimal() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByBigDecimal");
            String query = "SELECT ID, BALANCE FROM USERS ORDER BY BALANCE";
            executeSelectQuery(query, "BALANCE");
            LOGGER.log(Level.INFO, "Test selectOrderByBigDecimal: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByBigDecimal: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByBigDecimalDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByBigDecimalDesc");
            String query = "SELECT ID, BALANCE FROM USERS ORDER BY BALANCE DESC";
            executeSelectQuery(query, "BALANCE");
            LOGGER.log(Level.INFO, "Test selectOrderByBigDecimalDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByBigDecimalDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByChar() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByChar");
            String query = "SELECT ID, CHAR_FIELD FROM USERS ORDER BY CHAR_FIELD";
            executeSelectQuery(query, "CHAR_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByChar: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByChar: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByCharDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByCharDesc");
            String query = "SELECT ID, CHAR_FIELD FROM USERS ORDER BY CHAR_FIELD DESC";
            executeSelectQuery(query, "CHAR_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByCharDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByCharDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDate");
            String query = "SELECT ID, DATE_FIELD FROM USERS ORDER BY DATE_FIELD";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDateDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDateDesc");
            String query = "SELECT ID, DATE_FIELD FROM USERS ORDER BY DATE_FIELD DESC";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDateDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDateDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDateTime() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDateTime");
            String query = "SELECT ID, DATETIME_FIELD FROM USERS ORDER BY DATETIME_FIELD";
            executeSelectQuery(query, "DATETIME_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDateTime: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDateTime: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDateTimeDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDateTimeDesc");
            String query = "SELECT ID, DATETIME_FIELD FROM USERS ORDER BY DATETIME_FIELD DESC";
            executeSelectQuery(query, "DATETIME_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDateTimeDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDateTimeDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDateTimeMillis() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDateTimeMillis");
            String query = "SELECT ID, DATETIME_MILLIS_FIELD FROM USERS ORDER BY DATETIME_MILLIS_FIELD";
            executeSelectQuery(query, "DATETIME_MILLIS_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDateTimeMillis: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDateTimeMillis: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectOrderByDateTimeMillisDesc() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectOrderByDateTimeMillisDesc");
            String query = "SELECT ID, DATETIME_MILLIS_FIELD FROM USERS ORDER BY DATETIME_MILLIS_FIELD DESC";
            executeSelectQuery(query, "DATETIME_MILLIS_FIELD");
            LOGGER.log(Level.INFO, "Test selectOrderByDateTimeMillisDesc: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectOrderByDateTimeMillisDesc: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectJoinPrimaryKeyOrderByPrimaryKey() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectJoinPrimaryKeyOrderByPrimaryKey");
            String query = "SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID";
            executeSelectQuery(query, "ID");
            LOGGER.log(Level.INFO, "Test selectJoinPrimaryKeyOrderByPrimaryKey: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectJoinPrimaryKeyOrderByPrimaryKey: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectJoinBTreeIndexOrderByBTree() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectJoinBTreeIndexOrderByBTree");
            String query = "SELECT USERS.ID, USERS.AGE, PROFILES.PROFILE_AGE FROM USERS JOIN PROFILES ON USERS.AGE = PROFILES.PROFILE_AGE AND PROFILES.PROFILE_AGE > 18 OR PROFILES.PROFILE_AGE < 100 ORDER BY USERS.AGE";
            executeSelectQuery(query, "AGE");
            LOGGER.log(Level.INFO, "Test selectJoinBTreeIndexOrderByBTree: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectJoinBTreeIndexOrderByBTree: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectJoinHashIndexOrderByHash() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectJoinHashIndexOrderByHash");
            String query = "SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.NAME = PROFILES.PROFILE_NAME AND PROFILES.PROFILE_NAME LIKE 'Profile%' OR PROFILES.PROFILE_NAME IS NOT NULL ORDER BY USERS.NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectJoinHashIndexOrderByHash: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectJoinHashIndexOrderByHash: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectJoinUniqueIndexOrderByUnique() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectJoinUniqueIndexOrderByUnique");
            String query = "SELECT USERS.ID, USERS.USER_CODE, PROFILES.PROFILE_CODE FROM USERS JOIN PROFILES ON USERS.USER_CODE = PROFILES.PROFILE_CODE AND PROFILES.PROFILE_CODE LIKE 'PCODE%' OR PROFILES.PROFILE_CODE IS NOT NULL ORDER BY USERS.USER_CODE";
            executeSelectQuery(query, "USER_CODE");
            LOGGER.log(Level.INFO, "Test selectJoinUniqueIndexOrderByUnique: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectJoinUniqueIndexOrderByUnique: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectJoinNonIndexedOrderByNonIndexed() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectJoinNonIndexedOrderByNonIndexed");
            String query = "SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.BALANCE = CAST(PROFILES.NON_INDEXED AS BIGDECIMAL) AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE";
            executeSelectQuery(query, "BALANCE");
            LOGGER.log(Level.INFO, "Test selectJoinNonIndexedOrderByNonIndexed: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectJoinNonIndexedOrderByNonIndexed: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        OrderByTest test = new OrderByTest();
        test.runTests();
    }
}