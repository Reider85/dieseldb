package diesel.recovery;

import diesel.wal.DmlPayload;
import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the ARIES undo phase (prompt 4 #19): reverse-LSN delivery of
 * logical DML records of active transactions only.
 */
@Tag("smoke")
@Tag("storage")
public class UndoPhaseTest {

    /** Records every undo callback in delivery order. */
    private static final class RecordingSink implements MvccUndoSink {
        final List<String> calls = new ArrayList<>();

        @Override
        public void onInsertUndo(long txid, String table, int rowIndex, Map<String, Object> inserted) {
            calls.add("INSERT " + txid + " " + table + "[" + rowIndex + "]");
        }

        @Override
        public void onUpdateUndo(long txid, String table, int rowIndex,
                                 Map<String, Object> before, Map<String, Object> after) {
            calls.add("UPDATE " + txid + " " + table + "[" + rowIndex + "]");
        }

        @Override
        public void onDeleteUndo(long txid, String table, int rowIndex, Map<String, Object> before) {
            calls.add("DELETE " + txid + " " + table + "[" + rowIndex + "]");
        }
    }

    private Path walDir;
    private WALManager wal;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Path.of(System.getProperty("java.io.tmpdir"),
                "undo-phase-test-" + UUID.randomUUID());
        wal = new WALManager(WALConfig.of(walDir, 1024 * 1024));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (wal != null) {
            wal.close();
        }
        deleteTree(walDir);
    }

    private static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
    }

    private static Map<String, Object> row(long id, String name) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", id);
        values.put("NAME", name);
        return values;
    }

    private void appendInsert(long txid, String table, int rowIndex, Map<String, Object> values)
            throws IOException {
        wal.append(txid, WALOpcode.INSERT, null, DmlPayload.serialize(table, rowIndex, values));
    }

    private void appendUpdate(long txid, String table, int rowIndex,
                              Map<String, Object> before, Map<String, Object> after) throws IOException {
        wal.append(txid, WALOpcode.UPDATE,
                DmlPayload.serialize(table, rowIndex, before),
                DmlPayload.serialize(table, rowIndex, after));
    }

    private void appendDelete(long txid, String table, int rowIndex, Map<String, Object> before)
            throws IOException {
        wal.append(txid, WALOpcode.DELETE, DmlPayload.serialize(table, rowIndex, before), null);
    }

    @Test
    void emptyActiveSetSkipsWholeLog() throws IOException {
        appendInsert(1, "T", 0, row(1, "a"));
        wal.append(1, WALOpcode.COMMIT, null, new byte[0]);

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(), sink);

        assertTrue(sink.calls.isEmpty(), "no undo work for an empty active set");
        assertEquals(0, result.getTotalUndone());
        assertEquals(0, result.getIgnored());
    }

    @Test
    void activeTxidIsUndoneInReverseLsnOrder() throws IOException {
        // txid 1: insert, update, delete — all active (no COMMIT).
        appendInsert(1, "T", 0, row(1, "a"));
        appendUpdate(1, "T", 0, row(1, "a"), row(1, "b"));
        appendDelete(1, "T", 0, row(1, "b"));

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(1L), sink);

        assertEquals(List.of("DELETE 1 T[0]", "UPDATE 1 T[0]", "INSERT 1 T[0]"), sink.calls,
                "chained changes must unwind newest-first");
        assertEquals(1, result.getUndoneInserts());
        assertEquals(1, result.getUndoneUpdates());
        assertEquals(1, result.getUndoneDeletes());
        assertEquals(0, result.getIgnored());
    }

    @Test
    void committedAndAbortedTxidsAreSkipped() throws IOException {
        appendInsert(1, "T", 0, row(1, "a"));
        wal.append(1, WALOpcode.COMMIT, null, new byte[0]);
        appendInsert(2, "T", 1, row(2, "b"));
        wal.append(2, WALOpcode.ABORT, null, null);
        appendInsert(3, "T", 2, row(3, "c")); // still active

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(3L), sink);

        assertEquals(List.of("INSERT 3 T[2]"), sink.calls);
        assertEquals(1, result.getUndoneInserts());
        assertEquals(4, result.getIgnored(), "COMMIT/ABORT + committed/aborted DML are ignored");
    }

    @Test
    void nonDmlRecordsOfActiveTxidAreIgnored() throws IOException {
        wal.append(1, WALOpcode.BEGIN, null, null);
        appendInsert(1, "T", 0, row(1, "a"));
        wal.append(1, WALOpcode.ABORT, null, null);
        wal.append(1, WALOpcode.CHECKPOINT, null, new byte[] {1, 2, 3});

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(1L), sink);

        assertEquals(List.of("INSERT 1 T[0]"), sink.calls);
        assertEquals(3, result.getIgnored());
        assertEquals(4, result.getTotalRecords());
    }

    @Test
    void insertWithoutAfterImageIsIgnoredNotFatal() throws IOException {
        wal.append(1, WALOpcode.INSERT, null, null); // structurally valid, no image

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(1L), sink);

        assertTrue(sink.calls.isEmpty());
        assertEquals(1, result.getIgnored());
    }

    @Test
    void undecodablePayloadIsIgnoredNotFatal() throws IOException {
        wal.append(1, WALOpcode.INSERT, null, new byte[] {9, 9, 9}); // garbage payload

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(1L), sink);

        assertTrue(sink.calls.isEmpty());
        assertEquals(1, result.getIgnored());
    }

    @Test
    void multiSegmentScanPreservesGlobalReverseOrder() throws IOException {
        // Force several segment rotations so the reverse segment walk matters.
        WALManager smallWal = null;
        Path smallDir = Path.of(System.getProperty("java.io.tmpdir"),
                "undo-multi-seg-" + UUID.randomUUID());
        try {
            smallWal = new WALManager(WALConfig.of(smallDir, WALConfig.MIN_SEGMENT_SIZE_BYTES));
            for (int i = 0; i < 40; i++) {
                smallWal.append(7, WALOpcode.INSERT, null,
                        DmlPayload.serialize("T", i, row(i, "v" + i)));
                if (i % 8 == 7) {
                    smallWal.forceRotate();
                }
            }
            assertTrue(smallWal.getSegments().size() > 1,
                    "test needs multiple segments, got " + smallWal.getSegments().size());

            RecordingSink sink = new RecordingSink();
            UndoResult result = UndoPhase.undo(smallWal, Set.of(7L), sink);

            assertEquals(40, result.getUndoneInserts());
            assertEquals(sink.calls.size(), 40);
            // Strictly descending row indexes: newest entry undone first,
            // across segment boundaries.
            for (int i = 0; i < 40; i++) {
                assertEquals("INSERT 7 T[" + (39 - i) + "]", sink.calls.get(i));
            }
        } finally {
            if (smallWal != null) {
                smallWal.close();
            }
            deleteTree(smallDir);
        }
    }

    @Test
    void emptyWalProducesZeroResult() throws IOException {
        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(5L), sink);

        assertTrue(sink.calls.isEmpty());
        assertEquals(0, result.getTotalRecords());
        assertEquals(0, result.getLastLsn());
    }
}
