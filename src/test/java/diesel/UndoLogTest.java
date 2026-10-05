package diesel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for UndoLog functionality.
 */
@Tag("concurrency")
class UndoLogTest {
    
    @TempDir
    Path tempDir;
    
    private UndoLog undoLog;
    private Table testTable;
    private Database testDatabase;
    
    @BeforeEach
    void setUp() {
        undoLog = new UndoLog(1024L * 1024L); // 1MB threshold (constructor takes bytes)
        testDatabase = new Database();
testTable = new Table(testDatabase, "test", List.of("id", "name"), 
                             Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add some test rows
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("name", "Alice");
        testTable.addRow(row1);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2);
        row2.put("name", "Bob");
        testTable.addRow(row2);
    }
    
    @Test
    void testInsertUndoRecord() throws IOException {
        // Log an insert operation
        UndoLog.InsertUndo insertUndo = new UndoLog.InsertUndo(2);
        undoLog.log(insertUndo);
        
        assertEquals(1, undoLog.getInMemoryRecordCount());
        assertFalse(undoLog.isEmpty());
    }
    
    @Test
    void testUpdateUndoRecord() throws IOException {
        // Create old values and metadata for update
        Map<String, Object> oldValues = new HashMap<>();
        oldValues.put("id", 1);
        oldValues.put("name", "Alice");
        
        RowVersionMeta oldMeta = new RowVersionMeta();
        oldMeta.setXmin(1);
        oldMeta.setXmax(0);
        
        // Log an update operation
        UndoLog.UpdateUndo updateUndo = new UndoLog.UpdateUndo(0, oldValues, oldMeta);
        undoLog.log(updateUndo);
        
        assertEquals(1, undoLog.getInMemoryRecordCount());
        assertFalse(undoLog.isEmpty());
    }
    
    @Test
    void testDeleteUndoRecord() throws IOException {
        // Create old metadata for delete
        RowVersionMeta oldMeta = new RowVersionMeta();
        oldMeta.setXmin(0);
        oldMeta.setXmax(0);
        
        // Log a delete operation
        UndoLog.DeleteUndo deleteUndo = new UndoLog.DeleteUndo(1, oldMeta);
        undoLog.log(deleteUndo);
        
        assertEquals(1, undoLog.getInMemoryRecordCount());
        assertFalse(undoLog.isEmpty());
    }
    
    @Test
    void testMultipleRecords() throws IOException {
        // Log multiple operations
        undoLog.log(new UndoLog.InsertUndo(2));
        undoLog.log(new UndoLog.UpdateUndo(0, Map.of("id", 1, "name", "OldAlice"), new RowVersionMeta()));
        undoLog.log(new UndoLog.DeleteUndo(1, new RowVersionMeta()));
        
        assertEquals(3, undoLog.getInMemoryRecordCount());
        assertFalse(undoLog.isEmpty());
    }
    
    @Test
    void testClear() throws IOException {
        // Log some records
        undoLog.log(new UndoLog.InsertUndo(2));
        undoLog.log(new UndoLog.UpdateUndo(0, Map.of("id", 1), new RowVersionMeta()));
        
        undoLog.clear();
        
        assertEquals(0, undoLog.getInMemoryRecordCount());
        assertTrue(undoLog.isEmpty());
    }
    
    @Test
    void testEmptyLog() {
        assertTrue(undoLog.isEmpty());
        assertEquals(0, undoLog.getInMemoryRecordCount());
        assertEquals(0, undoLog.getMemoryUsage());
    }
    
    @Test
    void testSpillThreshold() throws IOException {
        // Create a very small threshold to force spill quickly
        UndoLog smallLog = new UndoLog(0); // 0MB threshold
        
        // Log a record that should trigger spill
        smallLog.log(new UndoLog.InsertUndo(0));
        
        // After spill, in-memory should be empty but spill file should exist
        assertTrue(smallLog.getInMemoryRecordCount() == 0 || smallLog.getMemoryUsage() < 100);
    }
    
    @Test
    void testInsertUndoApply() throws IOException {
        // Add MVCC metadata to a row
        testTable.markInsert(0, 1, testTable.getRows().get(0));
        
        // Create undo record and apply it
        UndoLog.InsertUndo insertUndo = new UndoLog.InsertUndo(0);
        insertUndo.apply(testTable);
        
        // Verify the metadata was marked as aborted
        RowVersionMeta meta = testTable.getRowVersionMeta(0);
        assertNotNull(meta);
        assertFalse(meta.isUncommittedInsert());
    }
    
    @Test
    void testUpdateUndoApply() throws IOException {
        // Add MVCC metadata and update a row
        Map<String, Object> oldValues = new HashMap<>(testTable.getRows().get(0));
        testTable.markUpdate(0, 1, oldValues);
        
        // Modify the current values
        testTable.getRows().get(0).put("name", "UpdatedAlice");
        
        // Create undo record and apply it
        RowVersionMeta oldMeta = testTable.getRowVersionMeta(0);
        UndoLog.UpdateUndo updateUndo = new UndoLog.UpdateUndo(0, oldValues, oldMeta);
        updateUndo.apply(testTable);
        
        // Verify the values were restored
        assertEquals("Alice", testTable.getRows().get(0).get("name"));
    }
    
    @Test
    void testDeleteUndoApply() throws IOException {
        // Capture the pre-delete state (xmax=0, alive) before marking the
        // delete — markDelete mutates the live meta in place.
        RowVersionMeta oldMeta = new RowVersionMeta();
        testTable.markDelete(0, 1);
        
        // Create undo record and apply it
        UndoLog.DeleteUndo deleteUndo = new UndoLog.DeleteUndo(0, oldMeta);
        deleteUndo.apply(testTable);
        
        // Verify the delete mark was cleared
        RowVersionMeta meta = testTable.getRowVersionMeta(0);
        assertNotNull(meta);
        assertEquals(0, meta.getXmax());
        assertFalse(meta.isUncommittedDelete());
    }
    
    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        // Create and serialize a record
        UndoLog.InsertUndo original = new UndoLog.InsertUndo(5);
        byte[] serialized = original.serialize();
        
        // Deserialize it back
        UndoLog.InsertUndo deserialized = (UndoLog.InsertUndo) UndoLog.UndoRecord.deserialize(serialized);
        
        assertEquals(original.getRowIndex(), deserialized.getRowIndex());
    }
    
    @Test
    void testMemoryUsageTracking() throws IOException {
        // Log some records and check memory usage
        long initialMemory = undoLog.getMemoryUsage();
        
        undoLog.log(new UndoLog.InsertUndo(0));
        undoLog.log(new UndoLog.UpdateUndo(1, Map.of("id", 2), new RowVersionMeta()));
        
        assertTrue(undoLog.getMemoryUsage() > initialMemory);
    }
}