package diesel;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class GroupByTest {
    private static final Logger LOGGER = Logger.getLogger(GroupByTest.class.getName());
    private static final int RECORD_COUNT = 1000;
    private final Database database;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    public GroupByTest() {
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

            // Step 2: Run SELECT queries with GROUP BY
            // Integer with GROUP BY String
            selectMinMaxAvgIntegerGroupByString();
            selectSumCountIntegerGroupByString();
            // Long with GROUP BY String
            selectMinMaxAvgLongGroupByString();
            selectSumCountLongGroupByString();
            // Short with GROUP BY String
            selectMinMaxAvgShortGroupByString();
            selectSumCountShortGroupByString();
            // Float with GROUP BY String
            selectMinMaxAvgFloatGroupByString();
            selectSumCountFloatGroupByString();
            // Double with GROUP BY String
            selectMinMaxAvgDoubleGroupByString();
            selectSumCountDoubleGroupByString();
            // BigDecimal with GROUP BY String
            selectMinMaxAvgBigDecimalGroupByString();
            selectSumCountBigDecimalGroupByString();
            // Integer with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgIntegerJoinGroupByStringDate();
            selectSumCountIntegerJoinGroupByStringDate();
            // Long with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgLongJoinGroupByStringDate();
            selectSumCountLongJoinGroupByStringDate();
            // Short with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgShortJoinGroupByStringDate();
            selectSumCountShortJoinGroupByStringDate();
            // Float with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgFloatJoinGroupByStringDate();
            selectSumCountFloatJoinGroupByStringDate();
            // Double with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgDoubleJoinGroupByStringDate();
            selectSumCountDoubleJoinGroupByStringDate();
            // BigDecimal with INNER JOIN and GROUP BY String, Date
            selectMinMaxAvgBigDecimalJoinGroupByStringDate();
            selectSumCountBigDecimalJoinGroupByStringDate();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error running tests: {0}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void createTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createTable");
            dropTable();
            String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, BYTE_FIELD BYTE, SHORT_FIELD SHORT, FLOAT_FIELD FLOAT, DOUBLE_FIELD DOUBLE, CHAR_FIELD CHAR, DATE_FIELD DATE)";
            LOGGER.log(Level.INFO, "Executing: {0}", createTableQuery);
            database.executeQuery(createTableQuery, null);
            LOGGER.log(Level.INFO, "Test createTable: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test createTable: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void createJoinTable() {
        try {
            LOGGER.log(Level.INFO, "Starting test: createJoinTable");
            dropJoinTable();
            String createTableQuery = "CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)";
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
                String query = String.format(Locale.US,
                        "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, BYTE_FIELD, SHORT_FIELD, FLOAT_FIELD, DOUBLE_FIELD, CHAR_FIELD, DATE_FIELD) " +
                                "VALUES ('CODE%d', 'User%d', %d, %s, %d, %d, %f, %f, '%c', '%s')",
                        i, i, 18 + (  i % 82),
                new BigDecimal(100 + (i % 9000)).setScale(2, BigDecimal.ROUND_HALF_UP),
                        (byte) (i % 127), (short) (i % 32767), (float) (i % 1000) / 10.0, (double) (i % 1000) / 10.0,
                        (char) ('A' + (i % 26)),
                        DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)))
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
                        "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED, PROFILE_DATE) " +
                                "VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d', '%s')",
                        i, 18 + (i % 82), i, i, i,
                        DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)))
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

    private Object executeSelectQuery(String query, String groupByField) {
        LOGGER.log(Level.INFO, "Executing SELECT: {0}", query);
        long startTime = System.nanoTime();
        Object result = database.executeQuery(query, null);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        List<?> resultList = (List<?>) result;
        LOGGER.log(Level.INFO, "Result: {0} rows, Time: {1} ms", new Object[]{resultList.size(), String.format("%.3f", durationMs)});
        if (!resultList.isEmpty()) {
            Object firstGroup = extractField(resultList.get(0), groupByField);
            LOGGER.log(Level.INFO, "First group: {0}", firstGroup);
        }
        return result;
    }

    private Object extractField(Object row, String field) {
        if (row instanceof Map) {
            return ((Map<?, ?>) row).get(field);
        }
        return null;
    }

    // Integer with GROUP BY String
    private void selectMinMaxAvgIntegerGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgIntegerGroupByString");
            String query = "SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgIntegerGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgIntegerGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountIntegerGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountIntegerGroupByString");
            String query = "SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountIntegerGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountIntegerGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Long with GROUP BY String
    private void selectMinMaxAvgLongGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgLongGroupByString");
            String query = "SELECT NAME, MIN(ID), MAX(ID), AVG(ID) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgLongGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgLongGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountLongGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountLongGroupByString");
            String query = "SELECT NAME, SUM(ID), COUNT(ID) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountLongGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountLongGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Short with GROUP BY String
    private void selectMinMaxAvgShortGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgShortGroupByString");
            String query = "SELECT NAME, MIN(SHORT_FIELD), MAX(SHORT_FIELD), AVG(SHORT_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgShortGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgShortGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountShortGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountShortGroupByString");
            String query = "SELECT NAME, SUM(SHORT_FIELD), COUNT(SHORT_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountShortGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountShortGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Float with GROUP BY String
    private void selectMinMaxAvgFloatGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgFloatGroupByString");
            String query = "SELECT NAME, MIN(FLOAT_FIELD), MAX(FLOAT_FIELD), AVG(FLOAT_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgFloatGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgFloatGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountFloatGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountFloatGroupByString");
            String query = "SELECT NAME, SUM(FLOAT_FIELD), COUNT(FLOAT_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountFloatGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountFloatGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Double with GROUP BY String
    private void selectMinMaxAvgDoubleGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgDoubleGroupByString");
            String query = "SELECT NAME, MIN(DOUBLE_FIELD), MAX(DOUBLE_FIELD), AVG(DOUBLE_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgDoubleGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgDoubleGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountDoubleGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountDoubleGroupByString");
            String query = "SELECT NAME, SUM(DOUBLE_FIELD), COUNT(DOUBLE_FIELD) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountDoubleGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountDoubleGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // BigDecimal with GROUP BY String
    private void selectMinMaxAvgBigDecimalGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgBigDecimalGroupByString");
            String query = "SELECT NAME, MIN(BALANCE), MAX(BALANCE), AVG(BALANCE) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgBigDecimalGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgBigDecimalGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountBigDecimalGroupByString() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountBigDecimalGroupByString");
            String query = "SELECT NAME, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY NAME";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountBigDecimalGroupByString: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountBigDecimalGroupByString: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Integer with GROUP BY Date and HAVING
    private void selectMinMaxAvgIntegerGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgIntegerGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgIntegerGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgIntegerGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountIntegerGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountIntegerGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountIntegerGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountIntegerGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Long with GROUP BY Date and HAVING
    private void selectMinMaxAvgLongGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgLongGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(ID), MAX(ID), AVG(ID) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgLongGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgLongGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountLongGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountLongGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(ID), COUNT(ID) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountLongGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountLongGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Short with GROUP BY Date and HAVING
    private void selectMinMaxAvgShortGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgShortGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(SHORT_FIELD), MAX(SHORT_FIELD), AVG(SHORT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgShortGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgShortGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountShortGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountShortGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(SHORT_FIELD), COUNT(SHORT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountShortGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountShortGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Float with GROUP BY Date and HAVING
    private void selectMinMaxAvgFloatGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgFloatGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(FLOAT_FIELD), MAX(FLOAT_FIELD), AVG(FLOAT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgFloatGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgFloatGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountFloatGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountFloatGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(FLOAT_FIELD), COUNT(FLOAT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountFloatGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountFloatGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Double with GROUP BY Date and HAVING
    private void selectMinMaxAvgDoubleGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgDoubleGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(DOUBLE_FIELD), MAX(DOUBLE_FIELD), AVG(DOUBLE_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgDoubleGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgDoubleGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountDoubleGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountDoubleGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(DOUBLE_FIELD), COUNT(DOUBLE_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountDoubleGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountDoubleGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // BigDecimal with GROUP BY Date and HAVING
    private void selectMinMaxAvgBigDecimalGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgBigDecimalGroupByDateHaving");
            String query = "SELECT DATE_FIELD, MIN(BALANCE), MAX(BALANCE), AVG(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgBigDecimalGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgBigDecimalGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountBigDecimalGroupByDateHaving() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountBigDecimalGroupByDateHaving");
            String query = "SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0";
            executeSelectQuery(query, "DATE_FIELD");
            LOGGER.log(Level.INFO, "Test selectSumCountBigDecimalGroupByDateHaving: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountBigDecimalGroupByDateHaving: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Integer with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgIntegerJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgIntegerJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.AGE), MAX(USERS.AGE), AVG(USERS.AGE) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgIntegerJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgIntegerJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountIntegerJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountIntegerJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.AGE), COUNT(USERS.AGE) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountIntegerJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountIntegerJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Long with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgLongJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgLongJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.ID), MAX(USERS.ID), AVG(USERS.ID) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgLongJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgLongJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountLongJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountLongJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.ID), COUNT(USERS.ID) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountLongJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountLongJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Short with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgShortJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgShortJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.SHORT_FIELD), MAX(USERS.SHORT_FIELD), AVG(USERS.SHORT_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgShortJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgShortJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountShortJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountShortJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.SHORT_FIELD), COUNT(USERS.SHORT_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountShortJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountShortJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Float with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgFloatJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgFloatJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.FLOAT_FIELD), MAX(USERS.FLOAT_FIELD), AVG(USERS.FLOAT_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgFloatJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgFloatJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountFloatJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountFloatJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.FLOAT_FIELD), COUNT(USERS.FLOAT_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountFloatJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountFloatJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // Double with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgDoubleJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgDoubleJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.DOUBLE_FIELD), MAX(USERS.DOUBLE_FIELD), AVG(USERS.DOUBLE_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgDoubleJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgDoubleJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountDoubleJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountDoubleJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.DOUBLE_FIELD), COUNT(USERS.DOUBLE_FIELD) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountDoubleJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountDoubleJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    // BigDecimal with INNER JOIN and GROUP BY String, Date
    private void selectMinMaxAvgBigDecimalJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectMinMaxAvgBigDecimalJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.BALANCE), MAX(USERS.BALANCE), AVG(USERS.BALANCE) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectMinMaxAvgBigDecimalJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectMinMaxAvgBigDecimalJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    private void selectSumCountBigDecimalJoinGroupByStringDate() {
        try {
            LOGGER.log(Level.INFO, "Starting test: selectSumCountBigDecimalJoinGroupByStringDate");
            String query = "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) " +
                    "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                    "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC";
            executeSelectQuery(query, "NAME");
            LOGGER.log(Level.INFO, "Test selectSumCountBigDecimalJoinGroupByStringDate: OK");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Test selectSumCountBigDecimalJoinGroupByStringDate: FAIL - {0}", e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        GroupByTest test = new GroupByTest();
        test.runTests();
    }
}