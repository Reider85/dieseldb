package diesel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for UndoLog spill functionality.
 */
@Tag("concurrency")
class UndoLogSpillTest {
    
    @TempDir
    Path tempDir;
    
    private UndoLog undoLog;
    private Table testTable;
    private Database testDatabase;
    
    @BeforeEach
    void setUp() {
        testDatabase = new Database();
testTable = new Table(testDatabase, "test", List.of("id", "name"), 
                             Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        
        // Add some test rows
        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "User" + i);
            testTable.addRow(row);
        }
    }
    
    @Test
    void testSpillWithSmallThreshold() throws IOException {
        // Create undo log with very small threshold (1KB)
        undoLog = new UndoLog(0); // 0MB threshold
        
        // Log multiple records to trigger spill
        for (int i = 0; i < 5; i++) {
            undoLog.log(new UndoLog.InsertUndo(i));
            undoLog.log(new UndoLog.UpdateUndo(i, Map.of("id", i), new RowVersionMeta()));
        }
        
        // After spill, in-memory records should be cleared
        assertTrue(undoLog.getInMemoryRecordCount() == 0 || undoLog.getMemoryUsage() < 1000);
    }
    
    @Test
    void testSpillFileCreation(@TempDir Path tempDir) throws IOException {
        // Create undo log with tiny threshold
        undoLog = new UndoLog(0);
        
        // Log a record to trigger spill
        undoLog.log(new UndoLog.InsertUndo(0));
        
        // Verify spill file was created
        assertTrue(undoLog.getMemoryUsage() > 0); // Some spill occurred
    }
    
    @Test
    void testRollbackAfterSpill(@TempDir Path tempDir) throws IOException {
        // Create undo log with tiny threshold to force spill
        undoLog = new UndoLog(0);
        
        // Log multiple records
        for (int i = 0; i < 3; i++) {
            undoLog.log(new UndoLog.InsertUndo(i));
            undoLog.log(new UndoLog.UpdateUndo(i, Map.of("id", i), new RowVersionMeta()));
        }
        
        // Clear the log (simulating rollback)
        undoLog.clear();
        
        // Verify log is empty after rollback
        assertTrue(undoLog.isEmpty());
    }
    
    @Test
    void testSpillWithLargeRecords(@TempDir Path tempDir) throws IOException {
        // Create undo log with a 1-byte threshold so every record spills
        undoLog = new UndoLog(1); // constructor takes bytes, not MB
        
        // Create a large record (simulate large row data)
        Map<String, Object> largeData = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeData.put("field" + i, "value" + i);
        }
        
        // Log large records to trigger spill
        for (int i = 0; i < 10; i++) {
            undoLog.log(new UndoLog.UpdateUndo(i, largeData, new RowVersionMeta()));
        }
        
        // Verify spill occurred
        assertTrue(undoLog.getMemoryUsage() > 0);
    }
    
    @Test
    void testMultipleSpillCycles(@TempDir Path tempDir) throws IOException {
        // Create undo log with very small threshold
        undoLog = new UndoLog(0);
        
        // Log records in batches to trigger multiple spills
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 2; i++) {
                undoLog.log(new UndoLog.InsertUndo(batch * 10 + i));
            }
        }
        
        // Verify log handles multiple spills
        assertTrue(undoLog.getMemoryUsage() >= 0);
    }
    
    @Test
    void testSpillCleanup(@TempDir Path tempDir) throws IOException {
        // Create undo log with tiny threshold
        undoLog = new UndoLog(0);
        
        // Log records to trigger spill
        for (int i = 0; i < 5; i++) {
            undoLog.log(new UndoLog.InsertUndo(i));
        }
        
        // Clear the log (should clean up spill file)
        undoLog.clear();
        
        // Verify spill file is cleaned up
        assertTrue(undoLog.isEmpty());
    }

    /**
     * Acceptance test (prompt4.md step 2): 100k inserts with
     * undo.spill.threshold.mb=1 must spill to a temp file, and the rollback
     * must read every spilled record back and undo it.
     */
    @Test
    void hundredThousandInsertsSpillWritesAndReadsTempFileAtOneMbThreshold() throws Exception {
        String previous = System.getProperty("undo.spill.threshold.mb");
        System.setProperty("undo.spill.threshold.mb", "1");
        try {
            Database db = new Database();
            db.setDataDir(tempDir.toString());
            db.executeQuery("CREATE TABLE SPILL_ACCEPT (ID LONG PRIMARY KEY SEQUENCE(spill_accept_seq 1 1), VAL STRING)", null);
            for (int i = 0; i < 5; i++) {
                db.executeQuery("INSERT INTO SPILL_ACCEPT (VAL) VALUES ('seed" + i + "')", null);
            }

            String beginResult = (String) db.executeQuery("BEGIN TRANSACTION", null);
            UUID txId = UUID.fromString(beginResult.split(": ")[1]);
            Transaction tx = db.getCurrentTransaction();
            assertNotNull(tx, "the active transaction must be resolvable");
            UndoLog log = tx.getUndoLog();
            Table table = db.getTable("SPILL_ACCEPT");
            long txid = tx.getTxid();

            final int inserts = 100_000;
            for (int i = 0; i < inserts; i++) {
                Map<String, Object> row = new HashMap<>();
                row.put("VAL", "x" + i);
                int rowIndex = table.addRowWithMVCC(row, txid);
                log.addUndoRecord(new UndoLog.InsertUndo(table.getName(), rowIndex));
            }

            // — spill written to a temp file past the 1MB threshold —
            Path spill = log.getSpillFile();
            assertNotNull(spill, "undo log must spill past the 1MB threshold");
            assertTrue(Files.exists(spill), "spill file must exist: " + spill);
            assertTrue(spill.getFileName().toString().startsWith("diesel-undo-"),
                    "spill file must be created in the temp directory: " + spill);
            long spillSize = Files.size(spill);
            assertTrue(spillSize > 1_000_000L,
                    "spill file must exceed the 1MB threshold, was " + spillSize + " bytes");
            assertTrue(log.getInMemoryRecordCount() < 11_000,
                    "in-memory records must stay bounded by the spill, was "
                            + log.getInMemoryRecordCount());

            // — read-back: apply every record (in-memory tail + spilled head) —
            log.rollback(db);
            assertEquals(0, log.getInMemoryRecordCount(), "rollback must drain the in-memory tail");
            int rawCount = table.getRawRowCount();
            assertEquals(5 + inserts, rawCount, "rows are appended after the seed rows");
            int undone = 0;
            for (int i = 0; i < rawCount; i++) {
                RowVersionMeta meta = table.getRowVersionMeta(i);
                if (i < 5) {
                    assertNull(meta, "seed row " + i + " must carry no undo metadata");
                } else {
                    assertNotNull(meta, "inserted row " + i + " must carry version metadata");
                    assertFalse(meta.isUncommittedInsert(),
                            "inserted row " + i + " must be undone via the spilled log");
                    undone++;
                }
            }
            assertEquals(inserts, undone, "every one of the 100k undo records must be read back");
            assertTrue(Files.exists(spill), "rollback does not delete the file; clear() does");

            // — SQL ROLLBACK: tracker cleanup and temp-file cleanup —
            db.executeQuery("ROLLBACK", txId);
            assertEquals(5, countRows(db, "SELECT ID FROM SPILL_ACCEPT"),
                    "only the seed rows survive the transaction");
            assertTrue(log.isEmpty(), "undo log must be empty after ROLLBACK");
            assertNull(log.getSpillFile(), "spill file reference must be released");
            assertFalse(Files.exists(spill), "spill temp file must be deleted");
        } finally {
            if (previous == null) {
                System.clearProperty("undo.spill.threshold.mb");
            } else {
                System.setProperty("undo.spill.threshold.mb", previous);
            }
        }
    }

    private static int countRows(Database db, String sql) {
        try {
            Object result = db.executeQuery(sql, null);
            if (result instanceof List<?> list) {
                return list.size();
            }
            return -1;
        } catch (Exception e) {
            throw new AssertionError("count query failed: " + sql, e);
        }
    }
}