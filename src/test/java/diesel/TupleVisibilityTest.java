package diesel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.DisplayName;
import java.util.function.LongPredicate;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MVCC tuple visibility logic.
 * 
 * <p>Tests 24 scenarios (3 isolation levels × 8 cases) plus serialization and edge cases.
 * Pure unit test — no Database, no @TempDir.
 */
@Tag("smoke")
@Tag("concurrency")
class TupleVisibilityTest {

    // Helper: creates a predicate that returns true for the given txids
    private static LongPredicate committed(long... ids) {
        return txid -> {
            for (long id : ids) {
                if (txid == id) return true;
            }
            return false;
        };
    }

    // Helper: creates a row with explicit version metadata
    private static Row row(long xmin, long xmax, long commandId) {
        var values = new java.util.LinkedHashMap<String, Object>();
        values.put("id", 1L);
        values.put("name", "test");
        return new Row(values, xmin, xmax, commandId);
    }

    // Helper: creates a row with explicit version metadata and values
    private static Row row(long xmin, long xmax, long commandId, Map<String, Object> values) {
        return new Row(new java.util.LinkedHashMap<>(values), xmin, xmax, commandId);
    }

    // ======== READ COMMITTED TESTS ========

    @Test
    @DisplayName("RC: committed insert is visible")
    void rc_committedInsertIsVisible() {
        Row row = row(10, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RC: uncommitted insert by other is invisible")
    void rc_uncommittedInsertByOtherIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RC: own uncommitted insert is visible")
    void rc_ownUncommittedInsertIsVisible() {
        Row row = row(20, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RC: committed delete is invisible")
    void rc_committedDeleteIsInvisible() {
        Row row = row(10, 12, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10, 12)));
    }

    @Test
    @DisplayName("RC: uncommitted delete by other is visible")
    void rc_uncommittedDeleteByOtherIsVisible() {
        Row row = row(10, 13, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RC: own delete is invisible")
    void rc_ownDeleteIsInvisible() {
        Row row = row(10, 20, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RC: insert after snapshot is invisible")
    void rc_insertAfterSnapshotIsInvisible() {
        Row row = row(16, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(16)));
    }

    @Test
    @DisplayName("RC: rolled-back insert is invisible")
    void rc_rolledBackInsertIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10)));
    }

    // ======== REPEATABLE READ TESTS ========

    @Test
    @DisplayName("RR: committed insert is visible")
    void rr_committedInsertIsVisible() {
        Row row = row(10, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    @Test
    @DisplayName("RR: uncommitted insert by other is invisible")
    void rr_uncommittedInsertByOtherIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    @Test
    @DisplayName("RR: own uncommitted insert is visible")
    void rr_ownUncommittedInsertIsVisible() {
        Row row = row(20, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    @Test
    @DisplayName("RR: committed delete is invisible")
    void rr_committedDeleteIsInvisible() {
        Row row = row(10, 12, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10, 12)));
    }

    @Test
    @DisplayName("RR: uncommitted delete by other is visible")
    void rr_uncommittedDeleteByOtherIsVisible() {
        Row row = row(10, 13, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    @Test
    @DisplayName("RR: own delete is invisible")
    void rr_ownDeleteIsInvisible() {
        Row row = row(10, 20, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    @Test
    @DisplayName("RR: insert after snapshot is invisible")
    void rr_insertAfterSnapshotIsInvisible() {
        Row row = row(16, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(16)));
    }

    @Test
    @DisplayName("RR: rolled-back insert is invisible")
    void rr_rolledBackInsertIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.REPEATABLE_READ, committed(10)));
    }

    // ======== SERIALIZABLE TESTS ========

    @Test
    @DisplayName("SER: committed insert is visible")
    void ser_committedInsertIsVisible() {
        Row row = row(10, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    @Test
    @DisplayName("SER: uncommitted insert by other is invisible")
    void ser_uncommittedInsertByOtherIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    @Test
    @DisplayName("SER: own uncommitted insert is visible")
    void ser_ownUncommittedInsertIsVisible() {
        Row row = row(20, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    @Test
    @DisplayName("SER: committed delete is invisible")
    void ser_committedDeleteIsInvisible() {
        Row row = row(10, 12, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10, 12)));
    }

    @Test
    @DisplayName("SER: uncommitted delete by other is visible")
    void ser_uncommittedDeleteByOtherIsVisible() {
        Row row = row(10, 13, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    @Test
    @DisplayName("SER: own delete is invisible")
    void ser_ownDeleteIsInvisible() {
        Row row = row(10, 20, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    @Test
    @DisplayName("SER: insert after snapshot is invisible")
    void ser_insertAfterSnapshotIsInvisible() {
        Row row = row(16, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(16)));
    }

    @Test
    @DisplayName("SER: rolled-back insert is invisible")
    void ser_rolledBackInsertIsInvisible() {
        Row row = row(11, 0, 0);
        assertFalse(TupleVisibility.visible(row, 20, 15, IsolationLevel.SERIALIZABLE, committed(10)));
    }

    // ======== READ UNCOMMITTED TESTS (mirror of RC, all visible) ========

    @Test
    @DisplayName("RU: uncommitted insert by other is visible")
    void ru_uncommittedInsertByOtherIsVisible() {
        Row row = row(11, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_UNCOMMITTED, committed(10)));
    }

    @Test
    @DisplayName("RU: rolled-back insert is visible")
    void ru_rolledBackInsertIsVisible() {
        Row row = row(11, 0, 0);
        assertTrue(TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_UNCOMMITTED, committed(10)));
    }

    // ======== ADDITIONAL TESTS ========

    @Test
    @DisplayName("Row serialization survives restart")
    void rowSerializationSurvivesRestart() throws Exception {
        var originalValues = new java.util.LinkedHashMap<String, Object>();
        originalValues.put("id", 42L);
        originalValues.put("name", "test");
        originalValues.put("active", true);
        Row original = new Row(originalValues, 100, 0, 5);
        
        // Serialize
        var baos = new java.io.ByteArrayOutputStream();
        var oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();
        
        // Deserialize
        var bais = new java.io.ByteArrayInputStream(baos.toByteArray());
        var ois = new java.io.ObjectInputStream(bais);
        Row deserialized = (Row) ois.readObject();
        ois.close();
        
        // Assert all fields equal
        assertEquals(original.getValues(), deserialized.getValues());
        assertEquals(original.getXmin(), deserialized.getXmin());
        assertEquals(original.getXmax(), deserialized.getXmax());
        assertEquals(original.getCommandId(), deserialized.getCommandId());
    }

    @Test
    @DisplayName("Row defensive copy — mutating source map doesn't affect row")
    void rowDefensiveCopy() {
        var source = new java.util.LinkedHashMap<String, Object>();
        source.put("id", 1L);
        source.put("name", "original");
        
        Row row = new Row(source);
        source.put("name", "modified");
        
        assertEquals("original", row.getValues().get("name"));
    }

    @Test
    @DisplayName("Simple contract overload matches predicate overload for committed data")
    void simpleContractOverloadMatchesPredicateOverload() {
        Row row = row(10, 0, 0);
        
        // Simple contract (assumes committed ≤ snapshot)
        boolean simpleResult = TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED);
        
        // Full contract (explicit committed set)
        boolean fullResult = TupleVisibility.visible(row, 20, 15, IsolationLevel.READ_COMMITTED, committed(10));
        
        assertEquals(fullResult, simpleResult);
    }

    @Test
    @DisplayName("Own insert is visible across all isolation levels (except RU)")
    void ownInsertIsVisibleAcrossLevels() {
        Row row = row(20, 0, 0);
        long snapshot = 15;
        long currentTxid = 20;
        
        for (IsolationLevel level : new IsolationLevel[] {
            IsolationLevel.READ_COMMITTED,
            IsolationLevel.REPEATABLE_READ,
            IsolationLevel.SERIALIZABLE
        }) {
            assertTrue(TupleVisibility.visible(row, currentTxid, snapshot, level, committed(10)),
                       "Own insert should be visible in " + level);
        }
    }

    @Test
    @DisplayName("Own delete is invisible across all isolation levels")
    void ownDeleteIsInvisibleAcrossLevels() {
        Row row = row(10, 20, 0);
        long snapshot = 15;
        long currentTxid = 20;
        
        for (IsolationLevel level : IsolationLevel.values()) {
            if (level == IsolationLevel.READ_UNCOMMITTED) continue; // RU always visible
            assertFalse(TupleVisibility.visible(row, currentTxid, snapshot, level, committed(10)),
                       "Own delete should be invisible in " + level);
        }
    }
}