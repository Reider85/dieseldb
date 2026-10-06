package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MVCC DELETE semantics (prompt4.md #4, acceptance): the deleter loses the
 * row immediately, concurrent readers keep seeing it until COMMIT, ROLLBACK
 * brings it back, REPEATABLE READ keeps rows deleted after BEGIN — and a
 * unique key (clustered PK or unique secondary index) can be re-inserted
 * once the delete has committed, because stale unique slots are recycled.
 */
@Tag("query-full")
class MvccDeleteTest {

    private Database setup(Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE t (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO t (id, val) VALUES (1, 'old')", null);
        return db;
    }

    private UUID begin(Database db) {
        String result = (String) db.executeQuery("BEGIN TRANSACTION", null);
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    private UUID beginIsolation(Database db, String isolation) {
        String result = (String) db.executeQuery(
                "BEGIN TRANSACTION ISOLATION LEVEL " + isolation, null);
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(Database db, String sql, UUID tx) {
        return (List<Map<String, Object>>) db.executeQuery(sql, tx);
    }

    private String getString(Map<String, Object> row, String column) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(column)) {
                return (String) entry.getValue();
            }
        }
        throw new AssertionError("column " + column + " not found in " + row.keySet());
    }

    private long getLong(Map<String, Object> row, String column) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(column)) {
                return ((Number) entry.getValue()).longValue();
            }
        }
        throw new AssertionError("column " + column + " not found in " + row.keySet());
    }

    @Test
    void writerLosesRowWhileForeignReadersStillSeeIt(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID tx = begin(db);
        db.executeQuery("DELETE FROM t WHERE id = 1", tx);

        assertEquals(0, select(db, "SELECT * FROM t", tx).size(),
                "the deleter must no longer see its own deleted row");
        assertEquals(1, select(db, "SELECT * FROM t", null).size(),
                "a concurrent reader still sees the row while the delete is pending");

        db.executeQuery("ROLLBACK", tx);
        List<Map<String, Object>> after = select(db, "SELECT * FROM t", null);
        assertEquals(1, after.size(), "ROLLBACK must bring the row back");
        assertEquals("old", getString(after.get(0), "val"), "with its original values");
    }

    @Test
    void committedDeleteDisappearsForFreshReaders(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID tx = begin(db);
        db.executeQuery("DELETE FROM t WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertEquals(0, select(db, "SELECT * FROM t", null).size(),
                "a committed delete must be durable");
    }

    @Test
    void repeatableReadKeepsRowDeletedAfterBegin(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID rr = beginIsolation(db, "REPEATABLE READ");
        assertEquals(1, select(db, "SELECT * FROM t", rr).size(), "baseline");

        UUID tx = begin(db);
        db.executeQuery("DELETE FROM t WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertEquals(1, select(db, "SELECT * FROM t", rr).size(),
                "REPEATABLE READ must keep rows deleted after BEGIN");
        db.executeQuery("ROLLBACK", rr);

        assertEquals(0, select(db, "SELECT * FROM t", null).size(),
                "the delete itself must be durable for fresh readers");
    }

    @Test
    void insertAfterCommittedDeleteReusesPrimaryKey(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE pk (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO pk (id, val) VALUES (1, 'old')", null);

        UUID tx = begin(db);
        db.executeQuery("DELETE FROM pk WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertDoesNotThrow(
                () -> db.executeQuery("INSERT INTO pk (id, val) VALUES (1, 'fresh')", null),
                "the key of a committed MVCC delete must be reusable");

        List<Map<String, Object>> rows = select(db, "SELECT * FROM pk", null);
        assertEquals(1, rows.size(), "exactly the re-inserted row must remain");
        assertEquals("fresh", getString(rows.get(0), "val"));

        rows = select(db, "SELECT * FROM pk WHERE id = 1", null);
        assertEquals(1, rows.size(), "clustered index lookup must find the reused key");
        assertEquals("fresh", getString(rows.get(0), "val"));
    }

    @Test
    void insertAfterCommittedDeleteReusesUniqueSecondaryKey(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        // Indexed columns are registered uppercase (CREATE UNIQUE INDEX is
        // parsed from the normalized statement), so the schema must match.
        db.executeQuery("CREATE TABLE u (id LONG PRIMARY KEY, CODE STRING)", null);
        db.executeQuery("CREATE UNIQUE INDEX ON u (code)", null);
        db.executeQuery("INSERT INTO u (id, CODE) VALUES (1, 'x')", null);

        UUID tx = begin(db);
        db.executeQuery("DELETE FROM u WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertDoesNotThrow(
                () -> db.executeQuery("INSERT INTO u (id, CODE) VALUES (2, 'x')", null),
                "the unique secondary key of a committed MVCC delete must be reusable");

        List<Map<String, Object>> rows = select(db, "SELECT * FROM u", null);
        assertEquals(1, rows.size(), "exactly the re-inserted row must remain");
        assertEquals(2L, getLong(rows.get(0), "id"));

        assertThrows(IllegalStateException.class,
                () -> db.executeQuery("INSERT INTO u (id, CODE) VALUES (3, 'x')", null),
                "the recycled slot must block real duplicates again");
    }

    @Test
    void insertAfterRolledBackDeleteStillDuplicates(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE pk (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO pk (id, val) VALUES (1, 'old')", null);

        UUID tx = begin(db);
        db.executeQuery("DELETE FROM pk WHERE id = 1", tx);
        db.executeQuery("ROLLBACK", tx);

        assertThrows(IllegalStateException.class,
                () -> db.executeQuery("INSERT INTO pk (id, val) VALUES (1, 'other')", null),
                "a rolled-back delete must keep the key reserved");

        List<Map<String, Object>> rows = select(db, "SELECT * FROM pk", null);
        assertEquals(1, rows.size());
        assertEquals("old", getString(rows.get(0), "val"), "the original row must be intact");
    }
}
