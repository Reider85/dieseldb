package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class OrderByTest {

    private static final int RECORD_COUNT = 100;
    private static final SimpleDateFormat TIMESTAMP_MS_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createTable();
        createUniqueIndex();
        createBTreeIndex();
        createHashIndex();
        createUniqueClusteredIndex();
        createJoinTable();
        insertRecords();
        insertJoinRecords();
    }

    private void createTable() {
        dropTable();
        String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, BYTE_FIELD BYTE, SHORT_FIELD SHORT, FLOAT_FIELD FLOAT, DOUBLE_FIELD DOUBLE, CHAR_FIELD CHAR, DATE_FIELD DATE, DATETIME_FIELD DATETIME, DATETIME_MILLIS_FIELD DATETIME_MS)";
        database.executeQuery(createTableQuery, null);
    }

    private void createJoinTable() {
        dropJoinTable();
        String createTableQuery = "CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING)";
        database.executeQuery(createTableQuery, null);
    }

    private void dropTable() {
        try {
            database.dropTable("USERS");
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void dropJoinTable() {
        try {
            database.dropTable("PROFILES");
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void createUniqueIndex() {
        String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (ID)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createBTreeIndex() {
        String createIndexQuery = "CREATE INDEX ON USERS (AGE)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createHashIndex() {
        String createIndexQuery = "CREATE HASH INDEX ON USERS (NAME)";
        database.executeQuery(createIndexQuery, null);
    }

    private void createUniqueClusteredIndex() {
        String createIndexQuery = "CREATE UNIQUE INDEX ON USERS (USER_CODE)";
        database.executeQuery(createIndexQuery, null);
    }

    private void insertRecords() {
        Table table = database.getTable("USERS");
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
    }

    private void insertJoinRecords() {
        Table table = database.getTable("PROFILES");
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(
                    "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED) " +
                            "VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d')",
                    i, 18 + (i % 82), i, i, i
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("PROFILES");
    }

    @Test
    void selectOrderByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS ORDER BY NAME", null), "selectOrderByString");
    }

    @Test
    void selectOrderByStringDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, NAME FROM USERS ORDER BY NAME DESC", null), "selectOrderByStringDesc");
    }

    @Test
    void selectOrderByInteger() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, AGE FROM USERS ORDER BY AGE", null), "selectOrderByInteger");
    }

    @Test
    void selectOrderByIntegerDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, AGE FROM USERS ORDER BY AGE DESC", null), "selectOrderByIntegerDesc");
    }

    @Test
    void selectOrderByLong() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, ID FROM USERS ORDER BY ID", null), "selectOrderByLong");
    }

    @Test
    void selectOrderByLongDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, ID FROM USERS ORDER BY ID DESC", null), "selectOrderByLongDesc");
    }

    @Test
    void selectOrderByByte() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, BYTE_FIELD FROM USERS ORDER BY BYTE_FIELD", null), "selectOrderByByte");
    }

    @Test
    void selectOrderByByteDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, BYTE_FIELD FROM USERS ORDER BY BYTE_FIELD DESC", null), "selectOrderByByteDesc");
    }

    @Test
    void selectOrderByShort() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, SHORT_FIELD FROM USERS ORDER BY SHORT_FIELD", null), "selectOrderByShort");
    }

    @Test
    void selectOrderByShortDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, SHORT_FIELD FROM USERS ORDER BY SHORT_FIELD DESC", null), "selectOrderByShortDesc");
    }

    @Test
    void selectOrderByFloat() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, FLOAT_FIELD FROM USERS ORDER BY FLOAT_FIELD", null), "selectOrderByFloat");
    }

    @Test
    void selectOrderByFloatDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, FLOAT_FIELD FROM USERS ORDER BY FLOAT_FIELD DESC", null), "selectOrderByFloatDesc");
    }

    @Test
    void selectOrderByDouble() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DOUBLE_FIELD FROM USERS ORDER BY DOUBLE_FIELD", null), "selectOrderByDouble");
    }

    @Test
    void selectOrderByDoubleDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DOUBLE_FIELD FROM USERS ORDER BY DOUBLE_FIELD DESC", null), "selectOrderByDoubleDesc");
    }

    @Test
    void selectOrderByBigDecimal() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, BALANCE FROM USERS ORDER BY BALANCE", null), "selectOrderByBigDecimal");
    }

    @Test
    void selectOrderByBigDecimalDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, BALANCE FROM USERS ORDER BY BALANCE DESC", null), "selectOrderByBigDecimalDesc");
    }

    @Test
    void selectOrderByChar() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, CHAR_FIELD FROM USERS ORDER BY CHAR_FIELD", null), "selectOrderByChar");
    }

    @Test
    void selectOrderByCharDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, CHAR_FIELD FROM USERS ORDER BY CHAR_FIELD DESC", null), "selectOrderByCharDesc");
    }

    @Test
    void selectOrderByDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATE_FIELD FROM USERS ORDER BY DATE_FIELD", null), "selectOrderByDate");
    }

    @Test
    void selectOrderByDateDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATE_FIELD FROM USERS ORDER BY DATE_FIELD DESC", null), "selectOrderByDateDesc");
    }

    @Test
    void selectOrderByDateTime() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATETIME_FIELD FROM USERS ORDER BY DATETIME_FIELD", null), "selectOrderByDateTime");
    }

    @Test
    void selectOrderByDateTimeDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATETIME_FIELD FROM USERS ORDER BY DATETIME_FIELD DESC", null), "selectOrderByDateTimeDesc");
    }

    @Test
    void selectOrderByDateTimeMillis() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATETIME_MILLIS_FIELD FROM USERS ORDER BY DATETIME_MILLIS_FIELD", null), "selectOrderByDateTimeMillis");
    }

    @Test
    void selectOrderByDateTimeMillisDesc() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT ID, DATETIME_MILLIS_FIELD FROM USERS ORDER BY DATETIME_MILLIS_FIELD DESC", null), "selectOrderByDateTimeMillisDesc");
    }

    @Test
    void selectJoinPrimaryKeyOrderByPrimaryKey() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID", null), "selectJoinPrimaryKeyOrderByPrimaryKey");
    }

    @Test
    void selectJoinBTreeIndexOrderByBTree() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.AGE, PROFILES.PROFILE_AGE FROM USERS JOIN PROFILES ON USERS.AGE = PROFILES.PROFILE_AGE AND PROFILES.PROFILE_AGE > 18 OR PROFILES.PROFILE_AGE < 100 ORDER BY USERS.AGE", null), "selectJoinBTreeIndexOrderByBTree");
    }

    @Test
    void selectJoinHashIndexOrderByHash() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.NAME = PROFILES.PROFILE_NAME AND PROFILES.PROFILE_NAME LIKE 'Profile%' OR PROFILES.PROFILE_NAME IS NOT NULL ORDER BY USERS.NAME", null), "selectJoinHashIndexOrderByHash");
    }

    @Test
    void selectJoinUniqueIndexOrderByUnique() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.USER_CODE, PROFILES.PROFILE_CODE FROM USERS JOIN PROFILES ON USERS.USER_CODE = PROFILES.PROFILE_CODE AND PROFILES.PROFILE_CODE LIKE 'PCODE%' OR PROFILES.PROFILE_CODE IS NOT NULL ORDER BY USERS.USER_CODE", null), "selectJoinUniqueIndexOrderByUnique");
    }

    @Test
    void selectJoinNonIndexedOrderByNonIndexed() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE", null), "selectJoinNonIndexedOrderByNonIndexed");
    }
}
