package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import diesel.Database;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that EXCHANGE PARTITION swaps real table data atomically and that
 * no rows are lost or duplicated (prompt4 #65, ExchangeWithDataTest).
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExchangeWithDataTest {
    private static Database database;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUpDatabase() {
        database = new Database(tempDir.toString());
    }

    @AfterAll
    static void tearDownDatabase() {
        if (database != null) {
            database.close();
        }
    }

    @AfterEach
    void cleanup() {
        dropIfPresent("SALES_DATA");
        dropIfPresent("STAGING_DATA");
    }

    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE IF EXISTS " + table, null);
        } catch (Exception ignored) {
            // Absent tables are fine.
        }
    }

    @SuppressWarnings("unchecked")
    private void createAndPopulate() {
        database.executeQuery("CREATE TABLE sales_data ("
                + "id LONG PRIMARY KEY, order_date DATE, amount DECIMAL(10,2)"
                + ") PARTITION BY RANGE (order_date) ("
                + "PARTITION q1 VALUES LESS THAN ('2024-04-01'),"
                + "PARTITION q2 VALUES LESS THAN ('2024-07-01'),"
                + "PARTITION future VALUES LESS THAN MAXVALUE)", null);
        database.executeQuery("CREATE TABLE staging_data ("
                + "id LONG PRIMARY KEY, order_date DATE, amount DECIMAL(10,2))", null);

        database.executeQuery("INSERT INTO sales_data (id, order_date, amount) VALUES (1, '2024-01-10', 10.00)", null);
        database.executeQuery("INSERT INTO sales_data (id, order_date, amount) VALUES (2, '2024-02-10', 20.00)", null);
        database.executeQuery("INSERT INTO sales_data (id, order_date, amount) VALUES (3, '2024-05-10', 30.00)", null);
        database.executeQuery("INSERT INTO staging_data (id, order_date, amount) VALUES (10, '2024-03-10', 40.00)", null);
    }

    @SuppressWarnings("unchecked")
    private Set<Long> ids(String table) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT id FROM " + table + " ORDER BY id", null);
        return rows.stream().map(row -> ((Number) row.get("id")).longValue()).collect(Collectors.toSet());
    }

    @Test
    void dataIsSwappedBetweenPartitionAndExternalTable() {
        createAndPopulate();

        database.executeQuery("ALTER TABLE sales_data EXCHANGE PARTITION q1 WITH TABLE staging_data", null);

        assertEquals(Set.of(3L, 10L), ids("sales_data"));
        assertEquals(Set.of(1L, 2L), ids("staging_data"));
    }

    @Test
    void exchangePreservesTotalRowCount() {
        createAndPopulate();
        long before = database.getTable("SALES_DATA").rowCount() + database.getTable("STAGING_DATA").rowCount();

        database.exchangePartition("SALES_DATA", "q1", "STAGING_DATA");

        long after = database.getTable("SALES_DATA").rowCount() + database.getTable("STAGING_DATA").rowCount();
        assertEquals(before, after, "EXCHANGE must not lose or duplicate rows");
        assertEquals(4, after);
    }

    @Test
    void emptyPartitionExchangeMovesDataIntoParent() {
        database.executeQuery("CREATE TABLE sales_data ("
                + "id LONG PRIMARY KEY, region STRING"
                + ") PARTITION BY LIST (region) (PARTITION us VALUES IN ('US'))", null);
        database.executeQuery("CREATE TABLE staging_data (id LONG PRIMARY KEY, region STRING)", null);
        database.executeQuery("INSERT INTO staging_data (id, region) VALUES (7, 'US')", null);

        database.exchangePartition("SALES_DATA", "us", "STAGING_DATA");

        assertEquals(Set.of(7L), ids("sales_data"));
        assertTrue(ids("staging_data").isEmpty());
    }
}
