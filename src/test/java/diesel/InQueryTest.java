package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@Tag("query-full")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InQueryTest extends AbstractDieselTest {

    @BeforeEach
    public void setup() throws IOException {
        super.setupCommonTables();
        database.executeQuery("CREATE TABLE IN_UPD (ID LONG PRIMARY KEY, CODE STRING, FLAG STRING)", null);
        database.executeQuery("CREATE HASH INDEX ON IN_UPD (CODE)", null);
        database.executeQuery("INSERT INTO IN_UPD (ID, CODE, FLAG) VALUES (1, 'A', 'X')", null);
        database.executeQuery("INSERT INTO IN_UPD (ID, CODE, FLAG) VALUES (2, 'B', 'Y')", null);
        database.executeQuery("INSERT INTO IN_UPD (ID, CODE, FLAG) VALUES (3, 'C', 'Z')", null);
    }

    @Test
    public void updateInWithDuplicatedValuesReportsEachRowOnce() {
        // Test with duplicated values
        UpdateQuery query = (UpdateQuery) new QueryParser().parse("UPDATE IN_UPD SET FLAG = 'done' WHERE CODE IN ('A','A','B')", database);
        query.execute(database.getTable("IN_UPD"));
        assertEquals(2L, query.getLastAffectedRows());
        database.getTable("IN_UPD").saveToFile("IN_UPD");

        // Test with single value duplicated
        query = (UpdateQuery) new QueryParser().parse("UPDATE IN_UPD SET FLAG = 'done2' WHERE CODE IN ('x','x')", database);
        query.execute(database.getTable("IN_UPD"));
        assertEquals(0L, query.getLastAffectedRows());
        database.getTable("IN_UPD").saveToFile("IN_UPD");

        // Verify actual data by running a SELECT query
        SelectQuery select = (SelectQuery) new QueryParser().parse("SELECT ID, CODE, FLAG FROM IN_UPD ORDER BY ID", database);
        List<Map<String, Object>> results = select.execute(database.getTable("IN_UPD"));
        
        // Check that only rows with CODE 'A' and 'B' were updated
        assertEquals("done", results.get(0).get("FLAG"));  // ID=1, CODE='A'
        assertEquals("done", results.get(1).get("FLAG"));  // ID=2, CODE='B' 
        assertEquals("Z", results.get(2).get("FLAG"));     // ID=3, CODE='C' - unchanged
    }
}