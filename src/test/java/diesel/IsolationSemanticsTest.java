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
 * Isolation-level snapshot semantics (prompt4.md #4, acceptance): READ
 * COMMITTED sees commits that landed before the current statement started,
 * REPEATABLE READ only sees commits that landed before BEGIN.
 */
@Tag("query-full")
class IsolationSemanticsTest {

    private UUID begin(Database db, String isolation) {
        String result = (String) db.executeQuery(
                "BEGIN TRANSACTION ISOLATION LEVEL " + isolation, null);
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> select(Database db, String sql, UUID tx) {
        return (List<Map<String, Object>>) db.executeQuery(sql, tx);
    }

    /** Commits a change through a separate explicit transaction (advances the commit CSN). */
    private void commitOther(Database db, String dml) {
        UUID other = begin(db, "READ COMMITTED");
        db.executeQuery(dml, other);
        db.executeQuery("COMMIT", other);
    }

    @Test
    void readCommittedSeesCommitsLandedBeforeTheStatement(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE iso (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (1, 'a')", null);

        UUID tx = begin(db, "READ COMMITTED");
        assertEquals(1, select(db, "SELECT * FROM iso", tx).size(), "baseline");

        commitOther(db, "INSERT INTO iso (id, val) VALUES (2, 'b')");

        assertEquals(2, select(db, "SELECT * FROM iso", tx).size(),
                "READ COMMITTED must refresh its snapshot at every statement");
        db.executeQuery("ROLLBACK", tx);
    }

    @Test
    void repeatableReadKeepsBeginSnapshot(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE iso (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (1, 'a')", null);

        UUID tx = begin(db, "REPEATABLE READ");
        assertEquals(1, select(db, "SELECT * FROM iso", tx).size(), "baseline");

        commitOther(db, "INSERT INTO iso (id, val) VALUES (2, 'b')");

        assertEquals(1, select(db, "SELECT * FROM iso", tx).size(),
                "REPEATABLE READ must not see commits that landed after BEGIN");
        db.executeQuery("ROLLBACK", tx);
    }

    @Test
    void readCommittedSeesCommittedDeletesFromLaterStatements(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE iso (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (1, 'a')", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (2, 'b')", null);

        UUID tx = begin(db, "READ COMMITTED");
        assertEquals(2, select(db, "SELECT * FROM iso", tx).size(), "baseline");

        commitOther(db, "DELETE FROM iso WHERE id = 2");

        assertEquals(1, select(db, "SELECT * FROM iso", tx).size(),
                "READ COMMITTED must see the committed delete in the next statement");
        db.executeQuery("ROLLBACK", tx);
    }

    @Test
    void repeatableReadKeepsRowsDeletedAfterBegin(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        db.executeQuery("CREATE TABLE iso (id LONG PRIMARY KEY, val STRING)", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (1, 'a')", null);
        db.executeQuery("INSERT INTO iso (id, val) VALUES (2, 'b')", null);

        UUID tx = begin(db, "REPEATABLE READ");
        assertEquals(2, select(db, "SELECT * FROM iso", tx).size(), "baseline");

        commitOther(db, "DELETE FROM iso WHERE id = 2");

        assertEquals(2, select(db, "SELECT * FROM iso", tx).size(),
                "REPEATABLE READ must keep rows deleted after BEGIN");
        db.executeQuery("ROLLBACK", tx);

        assertEquals(1, select(db, "SELECT * FROM iso", null).size(),
                "the delete itself must be durable for fresh readers");
    }
}
