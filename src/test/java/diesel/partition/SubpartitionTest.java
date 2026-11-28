package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import diesel.Database;
import diesel.partition.PartitionManager.PartitionType;
import diesel.partition.PartitionManager.TwoLevelPruningResult;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two-level (RANGE then LIST / HASH) partitioning acceptance tests
 * (prompt4 #65, SubpartitionTest).
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SubpartitionTest {
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
        try {
            database.executeQuery("DROP TABLE IF EXISTS ORDERS_SUB", null);
        } catch (Exception ignored) {
            // Absent table is fine.
        }
    }

    private void createRangeTable() {
        database.executeQuery("CREATE TABLE orders_sub ("
                + "id LONG PRIMARY KEY, order_date DATE, region STRING"
                + ") PARTITION BY RANGE (order_date) ("
                + "PARTITION q1 VALUES LESS THAN ('2024-04-01'),"
                + "PARTITION q2 VALUES LESS THAN ('2024-07-01'),"
                + "PARTITION future VALUES LESS THAN MAXVALUE)", null);
    }

    private PartitionManager manager() {
        return database.getTable("ORDERS_SUB").getPartitionManager();
    }

    @Test
    void rangeToListSchemeIsRegistered() {
        createRangeTable();
        Subpartition sub = database.createSubpartition("ORDERS_SUB", "q1", PartitionType.LIST, "region",
                Arrays.asList(List.of("US"), List.of("EU"), Arrays.asList((Object) null)), null);

        PartitionManager manager = manager();
        assertTrue(manager.hasSubpartitions());
        assertSame(sub, manager.getSubpartition("q1"));
        assertEquals(PartitionType.LIST, sub.getType());
        assertEquals("region", sub.getColumn());
        assertEquals(2, sub.getPartitions().size());
        assertNotNull(sub.getDefaultPartition());
    }

    @Test
    void rangeToListPrunesAtBothLevels() {
        createRangeTable();
        database.createSubpartition("ORDERS_SUB", "q1", PartitionType.LIST, "region",
                Arrays.asList(List.of("US"), List.of("EU"), Arrays.asList((Object) null)), null);

        TwoLevelPruningResult result = manager().routeSelectWithTwoLevelPruning(Map.of(
                "order_date_eq", LocalDate.of(2024, 1, 15),
                "region_eq", "US"));

        assertEquals(1, result.getLevel1().getPartitionsToScan().size());
        assertEquals("q1", result.getLevel1().getPartitionsToScan().get(0).getName());
        assertEquals(2, result.getLevel1().getPartitionsSkipped());

        assertEquals(1, result.getLevel2().size());
        Subpartition.PruningResult level2 = result.getLevel2().get("q1");
        assertNotNull(level2);
        assertEquals(3, level2.getTotalPartitions());
        assertEquals(1, level2.getPartitionsSkipped());
        assertEquals(2, level2.getPartitionsToScan().size());
        assertEquals(1, result.getSubpartitionsSkipped());
    }

    @Test
    void rangeToHashCannotPruneAtSecondLevel() {
        createRangeTable();
        database.createSubpartition("ORDERS_SUB", "q2", PartitionType.HASH, "region", null, 4);

        TwoLevelPruningResult result = manager().routeSelectWithTwoLevelPruning(Map.of(
                "order_date_eq", LocalDate.of(2024, 5, 15),
                "region_eq", "US"));

        assertEquals(1, result.getLevel1().getPartitionsToScan().size());
        assertEquals("q2", result.getLevel1().getPartitionsToScan().get(0).getName());
        assertEquals(4, result.getTotalSubpartitions());
        assertEquals(0, result.getSubpartitionsSkipped());
        assertEquals(0.0, result.getSubpartitionPruningRatio());
    }

    @Test
    void explainReportsBothPruningLevels() {
        createRangeTable();
        database.createSubpartition("ORDERS_SUB", "q1", PartitionType.LIST, "region",
                Arrays.asList(List.of("US"), List.of("EU"), Arrays.asList((Object) null)), null);

        Object plan = database.executeQuery(
                "EXPLAIN SELECT * FROM orders_sub WHERE order_date = '2024-01-15' AND region = 'US'", null);

        assertNotNull(plan);
        assertTrue(plan.toString().contains("Subpartition pruning"), "Plan was: " + plan);
        assertTrue(plan.toString().contains("LIST on region"), "Plan was: " + plan);
    }

    @Test
    void unsupportedSubpartitionTypeIsRejected() {
        createRangeTable();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                database.createSubpartition("ORDERS_SUB", "q1", PartitionType.RANGE, "region", null, null));
        assertTrue(error.getMessage().contains("LIST or HASH"));
    }
}
