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
 * MVCC UPDATE semantics (prompt4.md #4, acceptance): the writer reads its own
 * update, ROLLBACK restores the old value, concurrent readers see the
 * retained pre-image while the update is pending and the new value once it
 * commits; REPEATABLE READ keeps the pre-image of later commits.
 */
@Tag("query-full")
class MvccUpdateTest {

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

    private String selectVal(Database db, String sql, UUID tx) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) db.executeQuery(sql, tx);
        assertEquals(1, rows.size(), "expected exactly one row from: " + sql);
        for (Map.Entry<String, Object> entry : rows.get(0).entrySet()) {
            if (entry.getKey().equalsIgnoreCase("val")) {
                return (String) entry.getValue();
            }
        }
        throw new AssertionError("column val not found in " + rows.get(0).keySet());
    }

    @Test
    void writerReadsOwnUpdateAndRollbackRestoresOldValue(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID tx = begin(db);
        db.executeQuery("UPDATE t SET val = 'new' WHERE id = 1", tx);

        assertEquals("new", selectVal(db, "SELECT val FROM t WHERE id = 1", tx),
                "the writer must read its own update");

        db.executeQuery("ROLLBACK", tx);
        assertEquals("old", selectVal(db, "SELECT val FROM t WHERE id = 1", null),
                "ROLLBACK must restore the previous value");
    }

    @Test
    void committedUpdateIsVisibleToFreshReaders(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID tx = begin(db);
        db.executeQuery("UPDATE t SET val = 'new' WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertEquals("new", selectVal(db, "SELECT val FROM t WHERE id = 1", null),
                "the committed value must win for fresh readers");
    }

    @Test
    void foreignReaderSeesPreImageWhileUpdateIsPending(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID tx = begin(db);
        db.executeQuery("UPDATE t SET val = 'new' WHERE id = 1", tx);

        assertEquals("old", selectVal(db, "SELECT val FROM t WHERE id = 1", null),
                "a concurrent reader must not see another transaction's pending update");

        db.executeQuery("COMMIT", tx);
        assertEquals("new", selectVal(db, "SELECT val FROM t WHERE id = 1", null),
                "after COMMIT the new value becomes visible");
    }

    @Test
    void repeatableReadKeepsPreImageCommittedAfterBegin(@TempDir Path tempDir) {
        Database db = setup(tempDir);
        UUID rr = beginIsolation(db, "REPEATABLE READ");
        assertEquals("old", selectVal(db, "SELECT val FROM t WHERE id = 1", rr), "baseline");

        UUID tx = begin(db);
        db.executeQuery("UPDATE t SET val = 'new' WHERE id = 1", tx);
        db.executeQuery("COMMIT", tx);

        assertEquals("old", selectVal(db, "SELECT val FROM t WHERE id = 1", rr),
                "REPEATABLE READ must keep the pre-image of a commit that landed after BEGIN");
        db.executeQuery("ROLLBACK", rr);

        assertEquals("new", selectVal(db, "SELECT val FROM t WHERE id = 1", null),
                "the commit itself must be durable");
    }
}
