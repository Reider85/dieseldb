package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryCacheTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        dropTable();
        database.executeQuery("CREATE TABLE CACHE_TEST (ID LONG PRIMARY KEY, NAME STRING)", null);
    }

    @AfterEach
    void tearDown() {
        dropTable();
    }

    private void dropTable() {
        try {
            database.dropTable("CACHE_TEST");
        } catch (TableNotFoundException ignored) {
            // Ignore: table may not have been created
        }
    }

    private QueryCache cache() {
        return database.getQueryCache();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> rows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    @Test
    void repeatedQueryHitsCacheAndRecordsMetrics() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (2, 'beta')", null);

        Object first = database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha'", null);
        Object second = database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha'", null);
        Object third = database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha'", null);

        assertNotNull(first);
        assertEquals(first, second);
        assertEquals(first, third);
        assertEquals(1, rows(first).size());

        assertEquals(1, cache().size());
        assertEquals(2, cache().getHitCount());
        assertEquals(1, cache().getMissCount());
        assertEquals(2.0 / 3.0, cache().getHitRate(), 0.0001);
        assertTrue(cache().getParseTimeSavedNanos() > 0,
                "parse time saved should be positive after a hit");
        assertTrue(cache().getAverageParseTimeSavedNanos() > 0,
                "average parse time saved should be positive after a hit");
        assertTrue(cache().getSummary().contains("hitRate=0.6667"));
    }

    @Test
    void sameStructureDifferentLiteralsAreMissesWithCorrectData() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (2, 'beta')", null);

        Object first = database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha'", null);
        Object second = database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'beta'", null);

        assertEquals(1, rows(first).size());
        assertEquals("alpha", rows(first).get(0).get("NAME"));
        assertEquals(1, rows(second).size());
        assertEquals("beta", rows(second).get(0).get("NAME"));

        assertEquals(1, cache().size());
        assertEquals(0, cache().getHitCount());
        assertEquals(2, cache().getMissCount());
    }

    @Test
    void differentStructureUsesSeparateEntries() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha'", null);
        database.executeQuery("SELECT * FROM CACHE_TEST WHERE NAME = 'alpha' ORDER BY ID DESC", null);

        assertEquals(2, cache().size());
        assertEquals(2, cache().getMissCount());
    }

    @Test
    void dataMutationKeepsCachedPlanFresh() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (2, 'beta')", null);

        Object result = database.executeQuery("SELECT * FROM CACHE_TEST", null);

        assertEquals(2, rows(result).size(), "cached plan must see the freshly inserted row");
        assertEquals(1, cache().getHitCount(), "the repeated SELECT is served from the cache");
        assertEquals(1, cache().getMissCount());
        assertEquals(1, cache().size(), "DML must not clear the cache");
    }

    @Test
    void createIndexInvalidatesCache() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        database.executeQuery("CREATE INDEX idx_cache_test_name ON CACHE_TEST(NAME)", null);
        database.executeQuery("SELECT * FROM CACHE_TEST", null);

        assertEquals(1, cache().size());
        assertEquals(0, cache().getHitCount());
        assertEquals(2, cache().getMissCount(), "CREATE INDEX must invalidate the cache");
    }

    @Test
    void analyzeInvalidatesCache() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        database.executeQuery("ANALYZE TABLE CACHE_TEST", null);
        database.executeQuery("SELECT * FROM CACHE_TEST", null);

        assertEquals(1, cache().size());
        assertEquals(0, cache().getHitCount());
        assertEquals(2, cache().getMissCount(), "ANALYZE TABLE must invalidate the cache");
    }

    @Test
    void createAndDropTableInvalidateCache() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        database.executeQuery("CREATE TABLE CACHE_TEST2 (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        assertEquals(2, cache().getMissCount(), "CREATE TABLE must invalidate the cache");

        database.dropTable("CACHE_TEST2");
        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        assertEquals(3, cache().getMissCount(), "DROP TABLE must invalidate the cache");
    }

    @Test
    void maxRowsHintIsNeverCached() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT /* MAX_ROWS=100 */ * FROM CACHE_TEST", null);
        assertEquals(0, cache().size(), "MAX_ROWS hint queries must not be cached");

        database.executeQuery("SELECT * FROM CACHE_TEST", null);
        assertEquals(1, cache().size());

        database.executeQuery("SELECT /* MAX_ROWS=1 */ * FROM CACHE_TEST", null);
        assertEquals(1, cache().size(), "hinted query must not overwrite the cached plan");
        assertEquals(1, cache().getMissCount(), "the hint query is always a cache miss");
    }

    @Test
    void nonSelectStatementsAreNotCached() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);
        database.executeQuery("UPDATE CACHE_TEST SET NAME = 'gamma' WHERE ID = 1", null);
        database.executeQuery("DELETE FROM CACHE_TEST WHERE ID = 1", null);

        assertEquals(0, cache().size(), "DML statements must not be cached");
        assertEquals(0, cache().getMissCount(), "non-SELECT statements must not touch the cache");
    }

    @Test
    void derivedTableQueryIsNotCachedButInnerSelectIs() {
        database.executeQuery("INSERT INTO CACHE_TEST (ID, NAME) VALUES (1, 'alpha')", null);

        database.executeQuery("SELECT d.NAME FROM (SELECT * FROM CACHE_TEST) AS d", null);

        assertEquals(1, cache().size(), "the outer derived-table query must not be cached");

        Object result = database.executeQuery("SELECT * FROM CACHE_TEST", null);
        assertEquals(1, cache().getHitCount(), "the inner plain SELECT is cached and reused");
        assertEquals(1, rows(result).size());
    }

    @Test
    void normalizeStripsLiteralsAndPreservesStructure() {
        QueryCache.NormalizedSql n = QueryCache.normalize("SELECT * FROM T WHERE A = 1 AND B = 'x'");
        assertEquals("SELECT * FROM T WHERE A = ? AND B = ?", n.key);
        assertEquals(List.of("1", "'x'"), n.literals);

        QueryCache.NormalizedSql n2 = QueryCache.normalize("select * from t where a = 2 and b = 'y';");
        assertEquals(n.key, n2.key, "case and trailing semicolon must not change the structure key");
        assertEquals(List.of("2", "'y'"), n2.literals);

        QueryCache.NormalizedSql n3 = QueryCache.normalize("SELECT \"Name\" FROM T");
        assertEquals("SELECT \"Name\" FROM T", n3.key, "quoted identifiers keep their exact case");
        assertEquals(0, n3.literals.size());

        QueryCache.NormalizedSql n4 = QueryCache.normalize("SELECT * FROM T WHERE F = TRUE AND N = NULL");
        assertEquals("SELECT * FROM T WHERE F = TRUE AND N = NULL", n4.key,
                "SQL literals TRUE/FALSE/NULL are part of the structure");
        assertEquals(0, n4.literals.size());

        QueryCache.NormalizedSql n5 = QueryCache.normalize("SELECT * FROM T WHERE ID IN (1, 2, 3)");
        assertEquals("SELECT * FROM T WHERE ID IN ( ? , ? , ? )", n5.key);
        assertEquals(List.of("1", "2", "3"), n5.literals);
    }
}
