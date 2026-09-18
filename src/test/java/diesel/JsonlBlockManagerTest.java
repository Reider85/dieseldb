package diesel;

import diesel.storage.json.JsonParserConfig;
import diesel.storage.JsonlBlockManager;
import diesel.storage.JsonlSchemaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 55: {@link JsonlBlockManager} – lazy JSONL block reads from real byte
 * ranges of a plain data file.
 *
 * <p>Blocks are derived from the prompt-54 pre-scan (offset + physical line of
 * every non-blank line); reading a block parses only its span, keeping file:line
 * coordinates absolute. These tests pin the block geometry against the
 * pre-scan, the row/byte boundaries (incl. BOM and blank lines), the bounded
 * LRU cache (a small cache forces real on-disk range reads on eviction), the
 * projection alignment and the optional per-block min/max statistics.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlBlockManagerTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty("jsonl.lazy.blocks");
        System.clearProperty("jsonl.block.rows");
        System.clearProperty("jsonl.block.cache.blocks");
    }

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static JsonParserConfig blockConfig(int blockRows, int cacheBlocks) {
        return JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .blockRows(blockRows)
                .blockCacheBlocks(cacheBlocks)
                .build();
    }

    private JsonlBlockManager manager(String table, JsonParserConfig config) throws Exception {
        File file = tempDir.resolve(table + ".jsonl").toFile();
        return new JsonlBlockManager(file, COLS, types(), config, new JsonlSchemaManager(COLS, types(), config));
    }

    private void write(String table, String content) throws Exception {
        Files.write(tempDir.resolve(table + ".jsonl"), content.getBytes(StandardCharsets.UTF_8));
    }

    private static String row(int id, String name, int age) {
        return "{\"ID\":" + id + ",\"NAME\":\"" + name + "\",\"AGE\":" + age + "}\n";
    }

    private void writeRows(String table, int n) throws Exception {
        StringBuilder sb = new StringBuilder(n * 40);
        for (int i = 1; i <= n; i++) {
            sb.append(row(i, "name-" + i, i % 100));
        }
        Files.write(tempDir.resolve(table + ".jsonl"), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void blocksCoverEveryDataLineAndReadInFileOrder() throws Exception {
        int n = 2503;
        writeRows("BLOCKS", n);
        JsonlBlockManager manager = manager("BLOCKS", blockConfig(1000, 4));
        try {
            assertEquals(n, manager.dataLineCount());
            assertEquals(3, manager.blockCount());
            assertEquals(1000, manager.blockRowCount(0));
            assertEquals(1000, manager.blockRowCount(1));
            assertEquals(503, manager.blockRowCount(2));
            List<Object[]> all = manager.readAll();
            assertEquals(n, all.size());
            assertEquals(1L, all.get(0)[0]);
            assertEquals((long) n, all.get(n - 1)[0]);
            assertEquals("name-1250", all.get(1249)[1]);
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void byteRangesAreContiguousAndNonOverlapping() throws Exception {
        writeRows("RANGES", 25);
        JsonlBlockManager manager = manager("RANGES", blockConfig(10, 8));
        try {
            assertEquals(3, manager.blockCount());
            JsonlBlockManager.Block b0 = manager.getBlock(0);
            JsonlBlockManager.Block b1 = manager.getBlock(1);
            JsonlBlockManager.Block b2 = manager.getBlock(2);
            assertEquals(0L, b0.byteStart(), "block 0 must start at byte 0 (BOM included)");
            assertEquals(b0.byteEnd(), b1.byteStart(), "block 1 must start exactly where block 0 ended");
            assertEquals(b1.byteEnd(), b2.byteStart(), "block 2 must start exactly where block 1 ended");
            assertEquals(1, b0.firstDataLine());
            assertEquals(11, b1.firstDataLine());
            assertEquals(21, b2.firstDataLine());
            assertEquals(10, b0.rowCount());
            assertEquals(10, b1.rowCount());
            assertEquals(5, b2.rowCount());
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void bomAndBlankLinesDoNotShiftBlockGeometry() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("\uFEFF").append(row(1, "One", 1));
        sb.append("\n\n").append(" \t \n");
        for (int i = 2; i <= 12; i++) {
            sb.append(row(i, "name-" + i, i));
        }
        write("BOM", sb.toString());

        JsonlBlockManager manager = manager("BOM", blockConfig(5, 16));
        try {
            assertEquals(12, manager.dataLineCount(), "BOM + blanks must not appear as data lines");
            assertEquals(3, manager.blockCount());
            assertEquals(5, manager.blockRowCount(0));
            assertEquals(5, manager.blockRowCount(1));
            assertEquals(2, manager.blockRowCount(2));
            List<Object[]> all = manager.readAll();
            assertEquals(12, all.size());
            assertEquals("One", all.get(0)[1]);
            assertEquals("name-12", all.get(11)[1]);
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void boundedLruCacheEvictsAndReReadsFromDisk() throws Exception {
        writeRows("LRU", 300);
        JsonlBlockManager manager = manager("LRU", blockConfig(100, 2));
        try {
            assertEquals(3, manager.blockCount());
            manager.getBlock(0);
            manager.getBlock(1);
            manager.getBlock(2);
            assertEquals(2, manager.cachedBlockCount(), "capacity 2 keeps only the two hottest blocks");
            assertEquals(0, manager.getCacheHitCount(), "all three first reads are cold misses");
            assertEquals(3, manager.getCacheMissCount());

            manager.getBlock(1);
            assertEquals(1, manager.getCacheHitCount(), "block 1 served from cache");

            manager.getBlock(0);
            assertEquals(4, manager.getCacheMissCount(), "block 0 was evicted -> a real range read");
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void invalidateDropsCacheAndForcesReScan() throws Exception {
        write("INV", row(1, "A", 1));
        JsonlBlockManager manager = manager("INV", blockConfig(1000, 16));
        try {
            manager.getBlock(0);
            assertEquals(1, manager.getCacheMissCount());
            manager.invalidate();
            assertEquals(0, manager.cachedBlockCount());
            manager.getBlock(0);
            assertEquals(2, manager.getCacheMissCount(), "invalidate must force a fresh read");
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void projectedReadIsAlignedWithInputOrder() throws Exception {
        writeRows("PROJ", 50);
        JsonlBlockManager manager = manager("PROJ", blockConfig(1000, 4));
        try {
            List<Object[]> projected = manager.readAllProjected(List.of("AGE", "ID"));
            assertEquals(50, projected.size());
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(new Object[]{i + 1, (long) (i + 1)}, projected.get(i),
                        "projected row aligned with the projection item order");
            }
            assertEquals(0, manager.cachedBlockCount(), "projected reads bypass the row cache");
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void fullReadCollectsPerColumnStats() throws Exception {
        writeRows("STATS", 10);
        JsonlBlockManager manager = manager("STATS", blockConfig(1000, 4));
        try {
            manager.getBlock(0);
            JsonlBlockManager.BlockStats stats = manager.stats(0);
            assertNotNull(stats);
            assertEquals(10, stats.rowCount());
            assertEquals(1L, stats.min(0));
            assertEquals(10L, stats.max(0));
            assertEquals(1, stats.min(2));
            assertEquals(10, stats.max(2));
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void emptyFileHasNoBlocks() throws Exception {
        write("EMPTY", "");
        JsonlBlockManager manager = manager("EMPTY", blockConfig(1000, 4));
        try {
            assertEquals(0, manager.dataLineCount());
            assertEquals(0, manager.blockCount());
            assertThrows(IndexOutOfBoundsException.class, () -> manager.getBlock(0));
            assertTrue(manager.readAll().isEmpty());
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void blockSizeIsConfigDriven() throws Exception {
        write("CFG", row(1, "A", 1));
        JsonlBlockManager manager = manager("CFG", blockConfig(7, 2));
        try {
            assertEquals(7, manager.blockSize());
            assertEquals(1, manager.blockCount(), "7-row blocks still cover the single line");
        } finally {
            manager.invalidate();
        }
    }

    @Test
    void projectedAndFullReadsAgreeOnValues() throws Exception {
        writeRows("AGREE", 33);
        JsonlBlockManager manager = manager("AGREE", blockConfig(10, 4));
        try {
            List<Object[]> full = manager.readAll();
            List<Object[]> projected = manager.readAllProjected(List.of("ID", "NAME"));
            assertEquals(full.size(), projected.size());
            for (int i = 0; i < full.size(); i++) {
                assertArrayEquals(new Object[]{full.get(i)[0], full.get(i)[1]}, projected.get(i));
            }
        } finally {
            manager.invalidate();
        }
    }
}