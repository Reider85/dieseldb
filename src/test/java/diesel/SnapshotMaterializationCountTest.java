package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 MVCC-2 regression: {@link TransactionTableSnapshot#getRows()} must not
 * call the defensive-copy {@link Table#getRows()} inside the per-row loop
 * (that was the O(N^2) hotspot — two full-list materializations per row).
 * The snapshot fetches each row once via the O(1)
 * {@link Table#readPhysicalRow(int)} mirror accessor, so the defensive-copy
 * API is called at most once per snapshot read — in practice zero times.
 */
@Tag("concurrency")
class SnapshotMaterializationCountTest {

    /** Table that counts calls to the defensive-copy getRows() API. */
    static final class CountingTable extends Table {
        final AtomicInteger getRowsCalls = new AtomicInteger();

        CountingTable(Database database, String name) {
            super(database, name, List.of("id", "name"),
                    Map.of("id", Integer.class, "name", String.class), "id", Map.of());
        }

        @Override
        public List<Map<String, Object>> getRows() {
            getRowsCalls.incrementAndGet();
            return super.getRows();
        }
    }

    @Test
    void snapshotReadMaterializesWholeTableAtMostOnce() {
        Database database = new Database();
        CountingTable table = new CountingTable(database, "count_test");
        for (int i = 0; i < 3; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "row" + i);
            table.addRow(row);
        }

        TxStatusTracker tracker = database.getTxStatusTracker();
        long txid = tracker.registerTransaction();
        TransactionTableSnapshot snapshot = new TransactionTableSnapshot(
                "count_test", table, txid, txid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);

        List<Map<String, Object>> rows = snapshot.getRows();
        assertEquals(3, rows.size());
        // Pre-fix the counter was 2 per row (isRowVisible + getVisibleRowValues) = 6.
        assertTrue(table.getRowsCalls.get() <= 1,
                "Table.getRows() called " + table.getRowsCalls.get()
                        + " times per snapshot read; expected at most once (O(N^2) regression)");
        assertEquals(0, table.getRowsCalls.get(),
                "snapshot path must use readPhysicalRow only, never the defensive-copy getRows()");

        // A second read stays bounded too (no hidden re-materialization).
        assertEquals(3, snapshot.getRows().size());
        assertTrue(table.getRowsCalls.get() <= 2,
                "second snapshot read re-materialized the table " + table.getRowsCalls.get() + " times");
    }

    @Test
    void snapshotReadSeesMvccVersionedRowsWithoutMaterialization() {
        Database database = new Database();
        CountingTable table = new CountingTable(database, "count_mvcc");
        for (int i = 0; i < 2; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "row" + i);
            table.addRow(row);
        }

        TxStatusTracker tracker = database.getTxStatusTracker();
        long writerTxid = tracker.registerTransaction();

        // Production invariant: markInsert follows the physical append.
        Map<String, Object> inserted = new HashMap<>();
        inserted.put("id", 2);
        inserted.put("name", "pending");
        table.addRow(inserted);
        table.markInsert(2, writerTxid, inserted);

        // The writer sees its own uncommitted insert.
        TransactionTableSnapshot writer = new TransactionTableSnapshot(
                "count_mvcc", table, writerTxid, writerTxid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        assertEquals(3, writer.getRows().size());

        // Another reader must not see the uncommitted insert (no dirty reads).
        long readerTxid = tracker.registerTransaction();
        TransactionTableSnapshot reader = new TransactionTableSnapshot(
                "count_mvcc", table, readerTxid, readerTxid, tracker.getCurrentCommitCsn(),
                IsolationLevel.READ_COMMITTED, tracker);
        List<Map<String, Object>> rows = reader.getRows();
        assertEquals(2, rows.size());
        assertTrue(rows.stream().noneMatch(r -> "pending".equals(r.get("name"))));

        assertTrue(table.getRowsCalls.get() <= 2,
                "defensive-copy getRows() must not be used per row on the MVCC path"
                        + " (calls=" + table.getRowsCalls.get() + ")");
    }
}
