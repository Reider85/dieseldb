package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import diesel.Database;
import diesel.partition.PartitionExchange.ExchangeResult;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance tests for {@code ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...}
 * (prompt4 #65). The swap must be a metadata-scale operation (&lt;100 ms).
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
public class ExchangeTest {
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
        dropIfPresent("SALES_EXCHANGE");
        dropIfPresent("STAGING_EXCHANGE");
        dropIfPresent("PLAIN_EXCHANGE");
        dropIfPresent("BAD_SCHEMA_EXCHANGE");
    }

    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE IF EXISTS " + table, null);
        } catch (Exception ignored) {
            // Absent tables are fine.
        }
    }

    private void createTables() {
        database.executeQuery("CREATE TABLE sales_exchange ("
                + "id LONG PRIMARY KEY, region STRING, amount DECIMAL(10,2)"
                + ") PARTITION BY LIST (region) ("
                + "PARTITION us VALUES IN ('US'),"
                + "PARTITION eu VALUES IN ('EU'),"
                + "PARTITION other VALUES IN (NULL))", null);
        database.executeQuery("CREATE TABLE staging_exchange ("
                + "id LONG PRIMARY KEY, region STRING, amount DECIMAL(10,2))", null);
    }

    @Test
    void exchangePartitionIsUnder100Milliseconds() {
        createTables();

        long start = System.nanoTime();
        Object result = database.executeQuery(
                "ALTER TABLE sales_exchange EXCHANGE PARTITION us WITH TABLE staging_exchange", null);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMs < 100, "EXCHANGE PARTITION took " + elapsedMs + " ms, expected < 100 ms");
        assertTrue(result.toString().contains("exchanged"), "Unexpected result: " + result);
    }

    @Test
    void exchangeThroughApiReportsRowCounts() {
        createTables();
        database.executeQuery("INSERT INTO sales_exchange (id, region, amount) VALUES (1, 'US', 10.00)", null);
        database.executeQuery("INSERT INTO staging_exchange (id, region, amount) VALUES (2, 'US', 20.00)", null);

        ExchangeResult result = database.exchangePartition(
                "SALES_EXCHANGE", "us", "STAGING_EXCHANGE");

        assertEquals(1, result.rowsToOther());
        assertEquals(1, result.rowsToParent());
        assertTrue(result.durationNanos() > 0);
    }

    @Test
    void unknownPartitionIsRejected() {
        createTables();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery(
                        "ALTER TABLE sales_exchange EXCHANGE PARTITION missing WITH TABLE staging_exchange", null));
        assertTrue(error.getMessage().contains("does not exist"));
    }

    @Test
    void missingTableIsRejected() {
        createTables();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery(
                        "ALTER TABLE sales_exchange EXCHANGE PARTITION us WITH TABLE no_such_table", null));
        assertTrue(error.getMessage().contains("does not exist"));
    }

    @Test
    void nonPartitionedParentIsRejected() {
        createTables();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery("ALTER TABLE staging_exchange EXCHANGE PARTITION us"
                        + " WITH TABLE sales_exchange", null));
        assertTrue(error.getMessage().contains("not partitioned"));
    }

    @Test
    void incompatibleSchemaIsRejected() {
        createTables();
        database.executeQuery("CREATE TABLE bad_schema_exchange (id LONG PRIMARY KEY, note STRING)", null);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery("ALTER TABLE sales_exchange EXCHANGE PARTITION us"
                        + " WITH TABLE bad_schema_exchange", null));
        assertTrue(error.getMessage().contains("incompatible"));
    }

    @Test
    void exchangingTableWithItselfIsRejected() {
        createTables();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.exchangePartition("SALES_EXCHANGE", "us", "SALES_EXCHANGE"));
        assertTrue(error.getMessage().contains("same table"));
    }
}
