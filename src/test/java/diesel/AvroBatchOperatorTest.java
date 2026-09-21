package diesel;

import diesel.storage.avro.AvroBatchOperator;
import diesel.storage.avro.AvroBatchOperator.BatchStats;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 81 AVRO batch-operation tests: batch insert (1000+ rows per call),
 * batch read with size prediction, transactional batching (commit / rollback /
 * auto-rollback on close) and the accumulated {@link BatchStats} counters, plus
 * config resolution (sysprop → config.properties → defaults).
 */
@Tag("storage")
@StorageType("avro")
class AvroBatchOperatorTest {

    private static final String[] BATCH_SYS_PROPS = {
            "avro.batch.insert.flush.size",
            "avro.batch.read.estimate.factor",
            "avro.batch.validation.mode",
            "avro.batch.config.file",
    };

    @TempDir
    Path tempDir;

    @AfterEach
    void clearBatchSysProps() {
        for (String key : BATCH_SYS_PROPS) {
            System.clearProperty(key);
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", "User" + id);
        r.put("AGE", (int) (id % 100));
        r.put("ACTIVE", id % 2 == 0);
        return r;
    }

    private static List<Map<String, Object>> rows(int n) {
        List<Map<String, Object>> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(row(i));
        }
        return list;
    }

    private AvroRowStorage storage(String tableName) {
        AvroRowStorage s = new AvroRowStorage(tableName, cols(), types());
        s.setDataDir(tempDir.toString());
        return s;
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void configDefaults() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("cfg_defaults"))) {
            assertEquals(AvroBatchOperator.DEFAULT_INSERT_FLUSH_SIZE, op.getInsertFlushSize());
            assertEquals(AvroBatchOperator.DEFAULT_READ_ESTIMATE_FACTOR, op.getReadEstimateFactor());
            assertEquals(AvroBatchOperator.DEFAULT_VALIDATION_MODE, op.getValidationMode());
        }
    }

    @Test
    void configSyspropOverrides() {
        System.setProperty("avro.batch.insert.flush.size", "250");
        System.setProperty("avro.batch.read.estimate.factor", "2.5");
        System.setProperty("avro.batch.validation.mode", "PERMISSIVE");
        try (AvroBatchOperator op = new AvroBatchOperator(storage("cfg_sysprop"))) {
            assertEquals(250, op.getInsertFlushSize());
            assertEquals(2.5, op.getReadEstimateFactor());
            assertEquals("permissive", op.getValidationMode());
        }
    }

    @Test
    void configInvalidValuesFallBackToDefaults() {
        System.setProperty("avro.batch.insert.flush.size", "not-a-number");
        System.setProperty("avro.batch.read.estimate.factor", "abc");
        try (AvroBatchOperator op = new AvroBatchOperator(storage("cfg_invalid"))) {
            assertEquals(AvroBatchOperator.DEFAULT_INSERT_FLUSH_SIZE, op.getInsertFlushSize());
            assertEquals(AvroBatchOperator.DEFAULT_READ_ESTIMATE_FACTOR, op.getReadEstimateFactor());
        }
    }

    @Test
    void configValuesAreClampedToRange() {
        System.setProperty("avro.batch.insert.flush.size", "0");
        System.setProperty("avro.batch.read.estimate.factor", "0.1");
        try (AvroBatchOperator op = new AvroBatchOperator(storage("cfg_clamp"))) {
            assertEquals(1, op.getInsertFlushSize());
            assertEquals(1.0, op.getReadEstimateFactor());
        }
    }

    @Test
    void configFileOverrideIsHonored() throws IOException {
        Path cfg = tempDir.resolve("batch-config.properties");
        Files.writeString(cfg, "avro.batch.insert.flush.size = 7\n"
                + "avro.batch.read.estimate.factor = 3.0\n"
                + "avro.batch.validation.mode = off\n");
        System.setProperty("avro.batch.config.file", cfg.toString());
        try (AvroBatchOperator op = new AvroBatchOperator(storage("cfg_file"))) {
            assertEquals(7, op.getInsertFlushSize());
            assertEquals(3.0, op.getReadEstimateFactor());
            assertEquals("off", op.getValidationMode());
        }
    }

    // ─── Batch insert ───────────────────────────────────────────────

    @Test
    void insertBatchInsertsAllRows() {
        AvroRowStorage s = storage("batch_small");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            int n = op.insertBatch(rows(50));
            assertEquals(50, n);
            assertEquals(50, s.getInternalRows().size());
        }
    }

    @Test
    void insertBatchHandlesOneThousandPlusRows() {
        AvroRowStorage s = storage("batch_large");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            int n = op.insertBatch(rows(1500));
            assertEquals(1500, n);
            assertEquals(1500, s.getInternalRows().size());
            assertEquals(1500, op.getStats().rowsInserted());
        }
    }

    @Test
    void insertBatchFlushCounterAdvances() {
        System.setProperty("avro.batch.insert.flush.size", "100");
        AvroRowStorage s = storage("batch_flush");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(250));
            assertEquals(2, op.getStats().flushCount());
        }
    }

    @Test
    void insertBatchEmptyReturnsZero() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("batch_empty"))) {
            assertEquals(0, op.insertBatch(List.of()));
            assertEquals(0, op.getStats().rowsInserted());
        }
    }

    @Test
    void insertBatchRejectsNull() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("batch_null"))) {
            assertThrows(IllegalArgumentException.class, () -> op.insertBatch(null));
        }
    }

    @Test
    void insertBatchRawAppendsPositionalRows() {
        AvroRowStorage s = storage("batch_raw");
        List<Object[]> raw = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            raw.add(new Object[]{(long) i, "User" + i, i % 100, i % 2 == 0});
        }
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            assertEquals(1200, op.insertBatchRaw(raw));
            assertEquals(1200, s.getInternalRows().size());
            assertEquals("User7", s.getInternalRows().get(7)[1]);
        }
    }

    @Test
    void insertBatchRawEmptyReturnsZero() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("batch_raw_empty"))) {
            assertEquals(0, op.insertBatchRaw(List.of()));
        }
    }

    @Test
    void insertBatchRawRejectsNull() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("batch_raw_null"))) {
            assertThrows(IllegalArgumentException.class, () -> op.insertBatchRaw(null));
        }
    }

    @Test
    void importFromReaderStreamsRecords() throws IOException {
        AvroRowStorage source = storage("batch_import_src");
        source.insert(row(1));
        source.insert(row(2));
        source.insert(row(3));
        source.saveToFile("batch_import_src");

        File avroFile = new File(tempDir.toFile(), "batch_import_src.avro");
        assertTrue(avroFile.exists());

        AvroRowStorage target = storage("batch_import_dst");
        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile);
             AvroBatchOperator op = new AvroBatchOperator(target)) {
            int imported = op.importFromReader(reader);
            assertEquals(3, imported);
            assertEquals(3, target.getInternalRows().size());
            assertEquals(1L, target.getInternalRows().get(0)[0]);
        }
    }

    @Test
    void importFromReaderRejectsNull() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("batch_import_null"))) {
            assertThrows(IllegalArgumentException.class, () -> op.importFromReader(null));
        }
    }

    // ─── Batch read with size prediction ────────────────────────────

    @Test
    void readBatchWithPredictionReturnsAllRows() {
        AvroRowStorage s = storage("read_pred");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(300));
            List<Map<String, Object>> read = op.readBatchWithPrediction(300);
            assertEquals(300, read.size());
            assertEquals("User42", read.get(42).get("NAME"));
            assertEquals(300, op.getStats().rowsRead());
        }
    }

    @Test
    void readBatchWithPredictionToleratesUnderEstimate() {
        AvroRowStorage s = storage("read_under");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(500));
            List<Map<String, Object>> read = op.readBatchWithPrediction(10);
            assertEquals(500, read.size());
        }
    }

    @Test
    void readBatchWithPredictionNegativeTreatedAsZero() {
        AvroRowStorage s = storage("read_neg");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(5));
            assertEquals(5, op.readBatchWithPrediction(-100).size());
        }
    }

    @Test
    void readBatchWindowReturnsRequestedSlice() {
        AvroRowStorage s = storage("read_window");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(100));
            List<Map<String, Object>> window = op.readBatchWindow(10, 20, 20);
            assertEquals(20, window.size());
            assertEquals(10L, window.get(0).get("ID"));
            assertEquals(29L, window.get(19).get("ID"));
        }
    }

    @Test
    void readBatchWindowClampsPastEnd() {
        AvroRowStorage s = storage("read_window_clamp");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(30));
            List<Map<String, Object>> window = op.readBatchWindow(25, 100, 100);
            assertEquals(5, window.size());
        }
    }

    @Test
    void readBatchWindowOffsetPastEndIsEmpty() {
        AvroRowStorage s = storage("read_window_empty");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(10));
            assertTrue(op.readBatchWindow(100, 10, 10).isEmpty());
        }
    }

    @Test
    void readBatchWindowRejectsNegativeArguments() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("read_window_bad"))) {
            assertThrows(IllegalArgumentException.class, () -> op.readBatchWindow(-1, 10, 10));
            assertThrows(IllegalArgumentException.class, () -> op.readBatchWindow(0, -1, 10));
        }
    }

    @Test
    void estimateRowCountTracksStorage() {
        AvroRowStorage s = storage("estimate");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            assertEquals(0, op.estimateRowCount());
            op.insertBatch(rows(77));
            assertEquals(77, op.estimateRowCount());
        }
    }

    // ─── Transaction batching ───────────────────────────────────────

    @Test
    void transactionCommitKeepsChanges() {
        AvroRowStorage s = storage("tx_commit");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(5));
            op.beginBatch();
            assertTrue(op.isInTransaction());
            op.insertBatch(rows(5));
            assertEquals(10, s.getInternalRows().size());
            op.commitBatch();
            assertFalse(op.isInTransaction());
            assertEquals(10, s.getInternalRows().size());
            assertEquals(1, op.getStats().transactionCount());
        }
    }

    @Test
    void transactionRollbackRestoresSnapshot() {
        AvroRowStorage s = storage("tx_rollback");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(5));
            op.beginBatch();
            op.insertBatch(rows(7));
            assertEquals(12, s.getInternalRows().size());
            op.rollbackBatch();
            assertFalse(op.isInTransaction());
            assertEquals(5, s.getInternalRows().size());
            assertEquals(0L, s.getInternalRows().get(0)[0]);
        }
    }

    @Test
    void transactionRollbackAlsoRevertsRawInsert() {
        AvroRowStorage s = storage("tx_rollback_raw");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.beginBatch();
            List<Object[]> raw = new ArrayList<>();
            raw.add(new Object[]{99L, "Zed", 1, true});
            op.insertBatchRaw(raw);
            assertEquals(1, s.getInternalRows().size());
            op.rollbackBatch();
            assertTrue(s.getInternalRows().isEmpty());
        }
    }

    @Test
    void commitWithoutBeginThrows() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("tx_commit_bad"))) {
            assertThrows(IllegalStateException.class, op::commitBatch);
        }
    }

    @Test
    void rollbackWithoutBeginThrows() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("tx_rollback_bad"))) {
            assertThrows(IllegalStateException.class, op::rollbackBatch);
        }
    }

    @Test
    void doubleBeginThrows() {
        try (AvroBatchOperator op = new AvroBatchOperator(storage("tx_double"))) {
            op.beginBatch();
            assertThrows(IllegalStateException.class, op::beginBatch);
        }
    }

    @Test
    void closeAutoRollsBackOpenTransaction() {
        AvroRowStorage s = storage("tx_auto");
        AvroBatchOperator op = new AvroBatchOperator(s);
        op.insertBatch(rows(3));
        op.beginBatch();
        op.insertBatch(rows(4));
        assertEquals(7, s.getInternalRows().size());
        op.close();
        assertEquals(3, s.getInternalRows().size());
        assertFalse(op.isInTransaction());
    }

    // ─── Statistics ─────────────────────────────────────────────────

    @Test
    void statsAccumulateInsertAndRead() {
        AvroRowStorage s = storage("stats");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(200));
            op.readBatchWithPrediction(200);
            BatchStats st = op.getStats();
            assertEquals(200, st.rowsInserted());
            assertEquals(200, st.rowsRead());
            assertTrue(st.bytesWritten() > 0);
            assertTrue(st.insertNanos() >= 0);
            assertTrue(st.readNanos() >= 0);
        }
    }

    @Test
    void resetStatsZeroesCounters() {
        AvroRowStorage s = storage("stats_reset");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(10));
            op.resetStats();
            assertEquals(BatchStats.empty().rowsInserted(), op.getStats().rowsInserted());
            assertEquals(0, op.mutableStats().rowsInserted());
        }
    }

    @Test
    void mutableStatsReflectProgress() {
        AvroRowStorage s = storage("stats_mutable");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(123));
            assertEquals(123, op.mutableStats().rowsInserted());
            assertEquals(123, op.mutableStats().snapshot().rowsInserted());
        }
    }

    @Test
    void batchStatsToStringMentionsCounters() {
        BatchStats st = new BatchStats(10, 20, 30, 1_000_000, 2_000_000, 1, 2);
        String text = st.toString();
        assertTrue(text.contains("inserted=10"), text);
        assertTrue(text.contains("read=20"), text);
        assertTrue(text.contains("txns=1"), text);
    }

    @Test
    void batchStatsEmptyAndAverage() {
        assertEquals(0, BatchStats.empty().rowsInserted());
        BatchStats st = new BatchStats(10, 0, 0, 0, 0, 0, 0);
        assertEquals(5.0, st.averageInsertBatchSize(2));
        assertEquals(0.0, st.averageInsertBatchSize(0));
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    @Test
    void closedOperatorRejectsOperations() {
        AvroBatchOperator op = new AvroBatchOperator(storage("closed"));
        op.close();
        assertTrue(op.isClosed());
        assertThrows(IllegalStateException.class, () -> op.insertBatch(rows(1)));
        assertThrows(IllegalStateException.class, op::beginBatch);
        assertThrows(IllegalStateException.class, op::commitBatch);
        assertThrows(IllegalStateException.class, op::rollbackBatch);
        assertThrows(IllegalStateException.class, () -> op.readBatchWithPrediction(1));
        assertThrows(IllegalStateException.class, () -> op.readBatchWindow(0, 1, 1));
    }

    @Test
    void closeIsIdempotent() {
        AvroBatchOperator op = new AvroBatchOperator(storage("close_twice"));
        op.close();
        op.close();
        assertTrue(op.isClosed());
    }

    @Test
    void constructorRejectsNullStorage() {
        assertThrows(IllegalArgumentException.class, () -> new AvroBatchOperator(null));
    }

    @Test
    void getStorageReturnsBackingStorage() {
        AvroRowStorage s = storage("backing");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            assertNotNull(op.getStorage());
            assertEquals(s, op.getStorage());
        }
    }

    // ─── Integration: batch insert then persist round-trip ──────────

    @Test
    void batchInsertedRowsSurviveSaveAndLoad() throws IOException {
        AvroRowStorage s = storage("batch_roundtrip");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(1100));
        }
        s.saveToFile("batch_roundtrip");

        AvroRowStorage reloaded = storage("batch_roundtrip");
        reloaded.loadFromFile("batch_roundtrip");
        assertEquals(1100, reloaded.getInternalRows().size());
        assertEquals("User1099", reloaded.getInternalRows().get(1099)[1]);
    }

    @Test
    void writerAndReaderAgreeOnBatchContent() throws IOException {
        AvroRowStorage s = storage("batch_writer_reader");
        try (AvroBatchOperator op = new AvroBatchOperator(s)) {
            op.insertBatch(rows(64));
        }
        File out = new File(tempDir.toFile(), "direct.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), out);
        try {
            for (Object[] r : s.getInternalRows()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ID", r[0]);
                m.put("NAME", r[1]);
                m.put("AGE", r[2]);
                m.put("ACTIVE", r[3]);
                w.writeRow(m);
            }
            w.flush();
        } finally {
            w.close();
        }

        int count = 0;
        try (AvroDataFileReader reader = new AvroDataFileReader(out)) {
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
        }
        assertEquals(64, count);
    }
}
