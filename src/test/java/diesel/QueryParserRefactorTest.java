package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class QueryParserRefactorTest {

    private Database database;
    private QueryParser parser;

    @BeforeEach
    void setUp() {
        database = new Database();
        parser = new QueryParser();
        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, BYTE_FIELD BYTE, SHORT_FIELD SHORT, FLOAT_FIELD FLOAT, DOUBLE_FIELD DOUBLE, CHAR_FIELD CHAR, DATE_FIELD DATE)", null);
        database.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE STRING, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)", null);
    }

    @Test
    void parsesAllDispatchBranchesReturnsNonNull() {
        String[] queries = {
                "SELECT ID, NAME FROM USERS WHERE AGE > 18 GROUP BY NAME ORDER BY ID LIMIT 5",
                "SELECT u.NAME, p.PROFILE_NAME FROM USERS u JOIN PROFILES p ON u.ID = p.USER_ID WHERE u.AGE > 18",
                "SELECT u.NAME FROM USERS u LEFT JOIN PROFILES p ON u.ID = p.USER_ID",
                "SELECT u.NAME FROM USERS u CROSS JOIN PROFILES p",
                "INSERT INTO USERS (NAME, AGE) VALUES ('a', 20)",
                "UPDATE USERS SET NAME = 'b' WHERE ID = 1",
                "DELETE FROM USERS WHERE ID = 1",
                "CREATE TABLE TEST_T (X LONG PRIMARY KEY, Y STRING)",
                "CREATE UNIQUE INDEX ON USERS (ID)",
                "CREATE HASH INDEX ON USERS (NAME)",
                "CREATE INDEX ON USERS (AGE)",
                "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
                "COMMIT",
                "ROLLBACK",
                "SET AUTOCOMMIT ON",
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
                "ANALYZE TABLE USERS",
                "EXPLAIN SELECT * FROM USERS"
        };
        for (String sql : queries) {
            Query<?> q = parser.parse(sql, database);
            assertNotNull(q, "Parse returned null for: " + sql);
        }
    }
}
