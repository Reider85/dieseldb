package diesel.recovery;

import diesel.wal.DmlPayload;
import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALSegment;
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
 * Unit tests for UndoPhase's descending segment iteration optimization
 * (o2_prompts.md WAL-8): verifies direct descendingMap iteration preserves
 * reverse segment order and NavigableMap contract.
 */
@Tag("smoke")
@Tag("storage")
public class UndoPhaseDescendingIterationTest {

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
                "undo-descending-iteration-test-" + UUID.randomUUID());
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

    /**
     * Verifies that WALManager.getSegments() returns a NavigableMap with
     * descending segment order and unmodifiable contract.
     */
    @Test
    void segmentsMapIsNavigableAndUnmodifiable() throws IOException {
        // Create multiple segments, rotating after every append except the last
        for (int i = 0; i < 5; i++) {
            wal.append(1, WALOpcode.INSERT, null, DmlPayload.serialize("T", i, row(i, "v" + i)));
            if (i < 4) { // rotate after every append except the last
                wal.forceRotate();
            }
        }

        // Verify the map is a NavigableMap
        assertTrue(wal.getSegments() instanceof java.util.NavigableMap,
                "getSegments() should return a NavigableMap");

        // Verify descending order: segment numbers should be 5,4,3,2,1
        List<Integer> segmentNumbers = new ArrayList<>();
        for (WALSegment segment : wal.getSegments().descendingMap().values()) {
            segmentNumbers.add(segment.getNumber());
        }
        assertEquals(List.of(5, 4, 3, 2, 1), segmentNumbers,
                "Descending iteration should yield newest segment numbers first");

        // Verify unmodifiable: should throw on modification
        assertThrows(UnsupportedOperationException.class, () -> {
            wal.getSegments().put(99, null);
        }, "getSegments() should return an unmodifiable map");
    }

    /**
     * Verifies that undo with many segments processes newest segment first,
     * proving the descendingMap iteration preserves the reverse-order contract.
     */
    @Test
    void undoAcrossManySegmentsStillNewestFirst() throws IOException {
        // Create 12 segments, each with one INSERT for the same active txid
        for (int seg = 0; seg < 12; seg++) {
            wal.append(7, WALOpcode.INSERT, null,
                    DmlPayload.serialize("T", seg, row(seg, "v" + seg)));
            wal.forceRotate();
        }

        RecordingSink sink = new RecordingSink();
        UndoResult result = UndoPhase.undo(wal, Set.of(7L), sink);

        // All 12 inserts should be undone in newest-first order
        assertEquals(12, result.getUndoneInserts());
        assertEquals(12, sink.calls.size());

        // Check that newest segment (highest segment number) entries are undone first
        for (int i = 0; i < 12; i++) {
            int expectedRow = 11 - i; // newest segment (row 11) undone first
            assertEquals("INSERT 7 T[" + expectedRow + "]", sink.calls.get(i),
                    "Undo should process newest segment first, within segment newest entry first");
        }
    }
}