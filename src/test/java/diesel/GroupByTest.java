package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class GroupByTest {

    private static final int RECORD_COUNT = 100;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
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
        String createTableQuery = "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, BYTE_FIELD BYTE, SHORT_FIELD SHORT, FLOAT_FIELD FLOAT, DOUBLE_FIELD DOUBLE, CHAR_FIELD CHAR, DATE_FIELD DATE)";
        database.executeQuery(createTableQuery, null);
    }

    private void createJoinTable() {
        dropJoinTable();
        String createTableQuery = "CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)";
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
    }

    private void insertJoinRecords() {
        Table table = database.getTable("PROFILES");
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
    }

    @Test
    void selectMinMaxAvgIntegerGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgIntegerGroupByString");
    }

    @Test
    void selectSumCountIntegerGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME", null), "selectSumCountIntegerGroupByString");
    }

    @Test
    void selectMinMaxAvgLongGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(ID), MAX(ID), AVG(ID) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgLongGroupByString");
    }

    @Test
    void selectSumCountLongGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(ID), COUNT(ID) FROM USERS GROUP BY NAME", null), "selectSumCountLongGroupByString");
    }

    @Test
    void selectMinMaxAvgShortGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(SHORT_FIELD), MAX(SHORT_FIELD), AVG(SHORT_FIELD) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgShortGroupByString");
    }

    @Test
    void selectSumCountShortGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(SHORT_FIELD), COUNT(SHORT_FIELD) FROM USERS GROUP BY NAME", null), "selectSumCountShortGroupByString");
    }

    @Test
    void selectMinMaxAvgFloatGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(FLOAT_FIELD), MAX(FLOAT_FIELD), AVG(FLOAT_FIELD) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgFloatGroupByString");
    }

    @Test
    void selectSumCountFloatGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(FLOAT_FIELD), COUNT(FLOAT_FIELD) FROM USERS GROUP BY NAME", null), "selectSumCountFloatGroupByString");
    }

    @Test
    void selectMinMaxAvgDoubleGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(DOUBLE_FIELD), MAX(DOUBLE_FIELD), AVG(DOUBLE_FIELD) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgDoubleGroupByString");
    }

    @Test
    void selectSumCountDoubleGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(DOUBLE_FIELD), COUNT(DOUBLE_FIELD) FROM USERS GROUP BY NAME", null), "selectSumCountDoubleGroupByString");
    }

    @Test
    void selectMinMaxAvgBigDecimalGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, MIN(BALANCE), MAX(BALANCE), AVG(BALANCE) FROM USERS GROUP BY NAME", null), "selectMinMaxAvgBigDecimalGroupByString");
    }

    @Test
    void selectSumCountBigDecimalGroupByString() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT NAME, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY NAME", null), "selectSumCountBigDecimalGroupByString");
    }

    @Test
    void selectMinMaxAvgIntegerGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgIntegerGroupByDateHaving");
    }

    @Test
    void selectSumCountIntegerGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountIntegerGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgLongGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(ID), MAX(ID), AVG(ID) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgLongGroupByDateHaving");
    }

    @Test
    void selectSumCountLongGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(ID), COUNT(ID) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountLongGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgShortGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(SHORT_FIELD), MAX(SHORT_FIELD), AVG(SHORT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgShortGroupByDateHaving");
    }

    @Test
    void selectSumCountShortGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(SHORT_FIELD), COUNT(SHORT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountShortGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgFloatGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(FLOAT_FIELD), MAX(FLOAT_FIELD), AVG(FLOAT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgFloatGroupByDateHaving");
    }

    @Test
    void selectSumCountFloatGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(FLOAT_FIELD), COUNT(FLOAT_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountFloatGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgDoubleGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(DOUBLE_FIELD), MAX(DOUBLE_FIELD), AVG(DOUBLE_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgDoubleGroupByDateHaving");
    }

    @Test
    void selectSumCountDoubleGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(DOUBLE_FIELD), COUNT(DOUBLE_FIELD) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountDoubleGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgBigDecimalGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, MIN(BALANCE), MAX(BALANCE), AVG(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectMinMaxAvgBigDecimalGroupByDateHaving");
    }

    @Test
    void selectSumCountBigDecimalGroupByDateHaving() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", null), "selectSumCountBigDecimalGroupByDateHaving");
    }

    @Test
    void selectMinMaxAvgIntegerJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.AGE), MAX(USERS.AGE), AVG(USERS.AGE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgIntegerJoinGroupByStringDate");
    }

    @Test
    void selectSumCountIntegerJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.AGE), COUNT(USERS.AGE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountIntegerJoinGroupByStringDate");
    }

    @Test
    void selectMinMaxAvgLongJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.ID), MAX(USERS.ID), AVG(USERS.ID) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgLongJoinGroupByStringDate");
    }

    @Test
    void selectSumCountLongJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.ID), COUNT(USERS.ID) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountLongJoinGroupByStringDate");
    }

    @Test
    void selectMinMaxAvgShortJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.SHORT_FIELD), MAX(USERS.SHORT_FIELD), AVG(USERS.SHORT_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgShortJoinGroupByStringDate");
    }

    @Test
    void selectSumCountShortJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.SHORT_FIELD), COUNT(USERS.SHORT_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountShortJoinGroupByStringDate");
    }

    @Test
    void selectMinMaxAvgFloatJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.FLOAT_FIELD), MAX(USERS.FLOAT_FIELD), AVG(USERS.FLOAT_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgFloatJoinGroupByStringDate");
    }

    @Test
    void selectSumCountFloatJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.FLOAT_FIELD), COUNT(USERS.FLOAT_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountFloatJoinGroupByStringDate");
    }

    @Test
    void selectMinMaxAvgDoubleJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.DOUBLE_FIELD), MAX(USERS.DOUBLE_FIELD), AVG(USERS.DOUBLE_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgDoubleJoinGroupByStringDate");
    }

    @Test
    void selectSumCountDoubleJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.DOUBLE_FIELD), COUNT(USERS.DOUBLE_FIELD) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountDoubleJoinGroupByStringDate");
    }

    @Test
    void selectMinMaxAvgBigDecimalJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, MIN(USERS.BALANCE), MAX(USERS.BALANCE), AVG(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectMinMaxAvgBigDecimalJoinGroupByStringDate");
    }

    @Test
    void selectSumCountBigDecimalJoinGroupByStringDate() {
        assertDoesNotThrow(() -> database.executeQuery("SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", null), "selectSumCountBigDecimalJoinGroupByStringDate");
    }
}
