package diesel;

import diesel.storage.avro.AvroPrimaryKeyIndex;
import diesel.storage.avro.AvroRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 85 AVRO primary-key index tests: config resolution, build index,
 * lookup hit/miss, insert/update/delete maintenance, bulk rebuild,
 * LRU page cache, range search, sidecar persistence, and
 * AvroRowStorage integration.
 */
@Tag("storage")
@StorageType("avro")
class AvroPrimaryKeyIndexTest {

    private static final String[] INDEX_SYS_PROPS = {
            "avro.index.enabled",
            "avro.index.cache.size",
            "avro.index.page.size",
            "avro.index.config.file",
    };

    @TempDir
    Path tempDir;

    @AfterEach
    void clearIndexSysProps() {
        for (String key : INDEX_SYS_PROPS) {
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

    private static Object[] row(Object id, String name, int age, boolean active) {
        return new Object[]{id, name, age, active};
    }

    private static List<Object[]> rows(int n) {
        List<Object[]> list = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            list.add(row((long) i, "User" + i, 20 + (i % 50), i % 2 == 0));
        }
        return list;
    }

    private static Map<String, Object> mapRow(long id, String name, int age, boolean active) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", name);
        m.put("AGE", age);
        m.put("ACTIVE", active);
        return m;
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void configDefaults() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        assertNotNull(idx);
        assertFalse(idx.isEnabled());
    }

    @Test
    void configSyspropOverride() {
        System.setProperty("avro.index.enabled", "false");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        assertFalse(idx.isEnabled());
    }

    @Test
    void configDisabled() {
        System.setProperty("avro.index.enabled", "false");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertFalse(idx.isEnabled());
    }

    @Test
    void configCacheSizeOverride() {
        System.setProperty("avro.index.cache.size", "4");
        System.setProperty("avro.index.page.size", "3");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(20));
        List<Object[]> allRows = rows(20);
        for (int p = 0; p < idx.pageCount(20); p++) {
            idx.getPage(p, allRows);
        }
        assertTrue(idx.getCachedPageCount() <= 4,
                "Cache should respect max capacity, got " + idx.getCachedPageCount());
    }

    @Test
    void configPageSizeOverride() {
        System.setProperty("avro.index.page.size", "5");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertEquals(2, idx.pageCount(10));
    }

    @Test
    void configInvalidCacheSizeFallback() {
        System.setProperty("avro.index.cache.size", "not_a_number");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        assertNotNull(idx);
    }

    @Test
    void configInvalidPageSizeFallback() {
        System.setProperty("avro.index.page.size", "-5");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertTrue(idx.pageCount(10) > 0);
    }

    @Test
    void configViaFileOverride() throws Exception {
        Path cfg = tempDir.resolve("test-index.properties");
        Files.writeString(cfg, "avro.index.enabled=false\navro.index.page.size=10\n");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types(), cfg.toString());
        assertFalse(idx.isEnabled());
    }

    // ─── Build index ───────────────────────────────────────────────

    @Test
    void buildIndexPopulatesMap() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(100));
        assertEquals(100, idx.size());
    }

    @Test
    void buildIndexEmptyRows() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", List.of());
        assertEquals(0, idx.size());
    }

    // ─── Lookup ────────────────────────────────────────────────────

    @Test
    void lookupHit() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        Integer found = idx.lookup(5L);
        assertNotNull(found);
        assertEquals(4, found);
    }

    @Test
    void lookupMiss() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertNull(idx.lookup(999L));
    }

    @Test
    void lookupNullKey() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertNull(idx.lookup(null));
    }

    @Test
    void lookupStringKey() {
        List<String> nameFirst = List.of("NAME", "ID");
        AvroPrimaryKeyIndex strIdx = AvroPrimaryKeyIndex.create(nameFirst, types());
        List<Object[]> nameRows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            nameRows.add(new Object[]{"User" + i, (long) i, 20 + (i % 50), i % 2 == 0});
        }
        strIdx.setPrimaryKeyColumn("NAME", nameRows);
        Integer found = strIdx.lookup("User3");
        assertNotNull(found);
        assertEquals(2, found);
    }

    // ─── Insert maintenance ────────────────────────────────────────

    @Test
    void insertAddsToIndex() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        assertEquals(10, idx.size());
        idx.insert(row(100L, "New", 30, true), 10);
        assertEquals(11, idx.size());
        assertEquals(10, idx.lookup(100L));
    }

    @Test
    void insertDuplicateKeyThrows() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        assertThrows(IllegalArgumentException.class,
                () -> idx.insert(row(3L, "Dup", 25, false), 5));
    }

    @Test
    void insertNullKeySkipped() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        idx.insert(row(null, "NoKey", 25, false), 5);
        assertEquals(5, idx.size());
    }

    // ─── Update maintenance ────────────────────────────────────────

    @Test
    void updateChangesKey() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        idx.update(row(2L, "User2", 22, false), 1, row(200L, "User200", 22, false));
        assertNull(idx.lookup(2L));
        assertEquals(1, idx.lookup(200L));
    }

    @Test
    void updateSameKey() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("NAME", rows(5));
        idx.update(row(2L, "User2", 22, false), 1, row(2L, "User2", 30, true));
        assertEquals(1, idx.lookup("User2"));
    }

    // ─── Delete maintenance ────────────────────────────────────────

    @Test
    void deleteRemovesFromIndex() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        assertEquals(5, idx.size());
        idx.delete(row(3L, "User3", 23, false), 2);
        assertEquals(4, idx.size());
        assertNull(idx.lookup(3L));
    }

    @Test
    void deleteNullKeyNoOp() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        idx.delete(row(null, "NoKey", 25, false), 5);
        assertEquals(5, idx.size());
    }

    // ─── Bulk rebuild ──────────────────────────────────────────────

    @Test
    void buildIndexReplacesPrevious() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        assertEquals(5, idx.size());
        idx.buildIndex(rows(20));
        assertEquals(20, idx.size());
    }

    // ─── Page cache ────────────────────────────────────────────────

    @Test
    void pageCacheHitAndMiss() {
        System.setProperty("avro.index.page.size", "5");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(20));
        List<Object[]> allRows = rows(20);
        idx.getPage(0, allRows);
        assertEquals(1, idx.getCacheMisses());
        assertEquals(0, idx.getCacheHits());
        idx.getPage(0, allRows);
        assertEquals(1, idx.getCacheHits());
        assertEquals(1, idx.getCacheMisses());
    }

    @Test
    void pageCacheEviction() {
        System.setProperty("avro.index.cache.size", "2");
        System.setProperty("avro.index.page.size", "5");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(20));
        List<Object[]> allRows = rows(20);
        idx.getPage(0, allRows);
        idx.getPage(1, allRows);
        idx.getPage(2, allRows);
        assertTrue(idx.getCachedPageCount() <= 2);
    }

    @Test
    void pageCacheInvalidatedOnMutation() {
        System.setProperty("avro.index.page.size", "5");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        List<Object[]> allRows = rows(10);
        idx.getPage(0, allRows);
        assertTrue(idx.getCachedPageCount() > 0);
        idx.insert(row(100L, "New", 30, true), 10);
        assertEquals(0, idx.getCachedPageCount());
    }

    @Test
    void pageCountCalculation() {
        System.setProperty("avro.index.page.size", "3");
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        assertEquals(4, idx.pageCount(10));
        assertEquals(0, idx.pageCount(0));
        assertEquals(1, idx.pageCount(3));
    }

    // ─── Range search ──────────────────────────────────────────────

    @Test
    void rangeSearchReturnsSubset() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        List<Integer> result = idx.rangeSearch(3L, 7L);
        assertEquals(5, result.size());
        assertTrue(result.contains(2));
        assertTrue(result.contains(6));
    }

    @Test
    void rangeSearchEmptyRange() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        List<Integer> result = idx.rangeSearch(100L, 200L);
        assertTrue(result.isEmpty());
    }

    // ─── Sidecar persistence ───────────────────────────────────────

    @Test
    void sidecarSaveAndLoadRoundTrip() throws Exception {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(10));
        Path sidecar = tempDir.resolve("test.pki");
        idx.saveToSidecar(sidecar, 1024L, 1000L);
        assertTrue(Files.exists(sidecar));
        Map<Object, Integer> loaded = AvroPrimaryKeyIndex.loadFromSidecar(sidecar, 1024L, 1000L);
        assertNotNull(loaded);
        assertEquals(10, loaded.size());
        assertEquals(0, loaded.get(1L));
        assertEquals(9, loaded.get(10L));
    }

    @Test
    void sidecarLoadReturnsNullOnSizeMismatch() throws Exception {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        Path sidecar = tempDir.resolve("test.pki");
        idx.saveToSidecar(sidecar, 1024L, 1000L);
        Map<Object, Integer> loaded = AvroPrimaryKeyIndex.loadFromSidecar(sidecar, 9999L, 1000L);
        assertNull(loaded);
    }

    @Test
    void sidecarLoadReturnsNullOnMissingFile() {
        Map<Object, Integer> loaded = AvroPrimaryKeyIndex.loadFromSidecar(
                tempDir.resolve("nonexistent.pki"), 100L, 100L);
        assertNull(loaded);
    }

    @Test
    void sidecarLoadReturnsNullOnCorruptFile() throws Exception {
        Path sidecar = tempDir.resolve("corrupt.pki");
        Files.writeString(sidecar, "CORRUPT DATA HERE\n");
        Map<Object, Integer> loaded = AvroPrimaryKeyIndex.loadFromSidecar(sidecar, 100L, 100L);
        assertNull(loaded);
    }

    // ─── AvroRowStorage integration ────────────────────────────────

    @Test
    void storageInsertUpdatesIndex() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 25, false));
        assertNotNull(storage.getPrimaryKeyIndex());
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(1L));
        assertEquals(1, storage.getPrimaryKeyIndex().lookup(2L));
    }

    @Test
    void storageUpdateChangesIndex() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 25, false));
        storage.update(0, mapRow(100L, "Alice", 30, true));
        assertNull(storage.getPrimaryKeyIndex().lookup(1L));
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(100L));
    }

    @Test
    void storageDeleteRemovesFromIndex() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 25, false));
        storage.delete(0);
        assertNull(storage.getPrimaryKeyIndex().lookup(1L));
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(2L));
    }

    @Test
    void storageLookupByPrimaryKey() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 25, false));
        Map<String, Object> found = storage.lookupByPrimaryKey(1L);
        assertNotNull(found);
        assertEquals("Alice", found.get("NAME"));
        assertNull(storage.lookupByPrimaryKey(999L));
    }

    @Test
    void storageLookupWithoutPkReturnsNull() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.insert(mapRow(1L, "Alice", 30, true));
        assertNull(storage.lookupByPrimaryKey(1L));
    }

    @Test
    void storageSetRowsRebuildsIndex() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.insert(mapRow(1L, "Alice", 30, true));
        List<Map<String, Object>> newRows = List.of(
                mapRow(10L, "X", 10, false),
                mapRow(20L, "Y", 20, true));
        storage.setRows(newRows);
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(10L));
        assertEquals(1, storage.getPrimaryKeyIndex().lookup(20L));
    }

    // ─── toString ──────────────────────────────────────────────────

    @Test
    void toStringContainsState() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        String s = idx.toString();
        assertTrue(s.contains("entries=5"));
        assertTrue(s.contains("AvroPrimaryKeyIndex"));
    }
}
