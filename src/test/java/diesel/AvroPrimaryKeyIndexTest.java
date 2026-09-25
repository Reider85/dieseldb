package diesel;

import diesel.storage.avro.AvroPrimaryKeyIndex;
import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.AvroSecondaryIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
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

    @Test
    void updateDuplicateKeyThrows() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        assertThrows(IllegalArgumentException.class,
                () -> idx.update(row(2L, "User2", 22, false), 1, row(3L, "User3", 22, false)));
        assertEquals(1, idx.lookup(2L));
        assertEquals(2, idx.lookup(3L));
    }

    @Test
    void updateSameKeyDifferentRowThrows() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(5));
        assertThrows(IllegalArgumentException.class,
                () -> idx.update(row(1L, "User1", 21, true), 0, row(3L, "User3", 21, false)));
        assertEquals(0, idx.lookup(1L));
        assertEquals(2, idx.lookup(3L));
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

    @Test
    void restoreRejectsStalePrimaryKeyMapping() {
        AvroPrimaryKeyIndex idx = AvroPrimaryKeyIndex.create(cols(), types());
        idx.setPrimaryKeyColumn("ID", rows(2));

        assertFalse(idx.restoreFromSidecar(Map.of(1L, 99), rows(2)));
        assertEquals(2, idx.size());
        assertEquals(0, idx.lookup(1L));
    }

    @Test
    void storageLoadsPrimaryKeySidecarIntoIndex() {
        System.setProperty("avro.index.enabled", "true");
        AvroRowStorage first = new AvroRowStorage("PK_SIDECAR", cols(), types());
        first.setDataDir(tempDir.toString());
        first.setPrimaryKeyColumn("ID");
        first.insert(mapRow(1L, "Alice", 30, true));
        first.insert(mapRow(2L, "Bob", 25, false));
        first.saveToFile("PK_SIDECAR");

        AvroRowStorage second = new AvroRowStorage("PK_SIDECAR", cols(), types());
        second.setDataDir(tempDir.toString());
        second.setPrimaryKeyColumn("ID");
        second.loadFromFile("PK_SIDECAR");

        assertEquals(2, second.getPrimaryKeyIndex().size());
        assertEquals(0, second.getPrimaryKeyIndex().lookup(1L));
        assertEquals(1, second.getPrimaryKeyIndex().lookup(2L));
    }

    @Test
    void storageRebuildsPrimaryKeyIndexWhenSidecarIsStale() throws Exception {
        System.setProperty("avro.index.enabled", "true");
        AvroRowStorage first = new AvroRowStorage("PK_STALE", cols(), types());
        first.setDataDir(tempDir.toString());
        first.setPrimaryKeyColumn("ID");
        first.insert(mapRow(1L, "Alice", 30, true));
        first.saveToFile("PK_STALE");

        Path avro = tempDir.resolve("PK_STALE.avro");
        Files.setLastModifiedTime(avro, FileTime.fromMillis(avro.toFile().lastModified() + 10_000));

        AvroRowStorage second = new AvroRowStorage("PK_STALE", cols(), types());
        second.setDataDir(tempDir.toString());
        second.setPrimaryKeyColumn("ID");
        second.loadFromFile("PK_STALE");

        assertEquals(0, second.getPrimaryKeyIndex().lookup(1L));
    }

    @Test
    void storageLoadsStringPrimaryKeySidecarWithoutTypeCoercion() {
        System.setProperty("avro.index.enabled", "true");
        List<String> columns = List.of("ID");
        Map<String, Class<?>> types = Map.of("id", String.class);
        AvroRowStorage first = new AvroRowStorage("STRING_PK", columns, types);
        first.setDataDir(tempDir.toString());
        first.setPrimaryKeyColumn("ID");
        first.insert(Map.of("ID", "1"));
        first.insert(Map.of("ID", "true"));
        first.insert(Map.of("ID", "001"));
        first.saveToFile("STRING_PK");

        AvroRowStorage second = new AvroRowStorage("STRING_PK", columns, types);
        second.setDataDir(tempDir.toString());
        second.setPrimaryKeyColumn("ID");
        second.loadFromFile("STRING_PK");

        assertEquals(0, second.getPrimaryKeyIndex().lookup("1"));
        assertEquals(1, second.getPrimaryKeyIndex().lookup("true"));
        assertEquals(2, second.getPrimaryKeyIndex().lookup("001"));
    }

    @Test
    void secondaryIndexSidecarReloadsAfterPhysicalDelete() {
        AvroRowStorage first = new AvroRowStorage("SECONDARY_DELETE", cols(), types());
        first.setDataDir(tempDir.toString());
        first.createSecondaryIndex("age_idx", "AGE");
        first.insert(mapRow(1L, "Alice", 30, true));
        first.insert(mapRow(2L, "Bob", 20, false));
        first.insert(mapRow(3L, "Charlie", 30, true));
        first.delete(0);
        first.saveToFile("SECONDARY_DELETE");

        AvroRowStorage second = new AvroRowStorage("SECONDARY_DELETE", cols(), types());
        second.setDataDir(tempDir.toString());
        second.loadFromFile("SECONDARY_DELETE");

        AvroSecondaryIndex index = second.getSecondaryIndexManager().getIndex("age_idx");
        assertNotNull(index);
        assertEquals(List.of(1), index.search(30));
        assertEquals(List.of(0), index.search(20));
    }

    @Test
    void savingWithNoSecondaryIndexesRemovesOldSidecar() {
        AvroRowStorage first = new AvroRowStorage("SECONDARY_EMPTY", cols(), types());
        first.setDataDir(tempDir.toString());
        first.createSecondaryIndex("age_idx", "AGE");
        first.insert(mapRow(1L, "Alice", 30, true));
        first.saveToFile("SECONDARY_EMPTY");
        first.insert(mapRow(2L, "Bob", 25, false));
        first.saveToFile("SECONDARY_EMPTY");
        assertTrue(Files.exists(tempDir.resolve("SECONDARY_EMPTY.asi")));

        first.dropSecondaryIndex("age_idx");
        first.saveToFile("SECONDARY_EMPTY");
        assertFalse(Files.exists(tempDir.resolve("SECONDARY_EMPTY.asi")));

        AvroRowStorage second = new AvroRowStorage("SECONDARY_EMPTY", cols(), types());
        second.setDataDir(tempDir.toString());
        second.loadFromFile("SECONDARY_EMPTY");
        assertEquals(0, second.getSecondaryIndexManager().getIndexCount());
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
    void storageUpdateDuplicateKeyLeavesRowAndIndexUnchanged() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.createSecondaryIndex("age_idx", "AGE");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 25, false));

        assertThrows(IllegalArgumentException.class,
                () -> storage.update(0, mapRow(2L, "AliceConflict", 30, false)));
        assertEquals(2, storage.scan().size());
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(1L));
        assertEquals(1, storage.getPrimaryKeyIndex().lookup(2L));
        assertEquals(List.of(0), storage.getSecondaryIndexManager().getIndex("age_idx").search(30));
        assertEquals(List.of(1), storage.getSecondaryIndexManager().getIndex("age_idx").search(25));
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

    @Test
    void secondaryIndexKeepsRowPositionsSorted() {
        AvroSecondaryIndex index = new AvroSecondaryIndex("age_idx", "AGE", Integer.class);
        index.insert(30, 5);
        index.insert(30, 1);
        index.insert(30, 3);

        assertEquals(List.of(1, 3, 5), index.search(30));
        index.remove(30, 3);
        assertEquals(List.of(1, 5), index.search(30));
    }

    @Test
    void compositeIndexSupportsPrefixSearch() {
        AvroSecondaryIndex.CompositeKey prefix = new AvroSecondaryIndex.CompositeKey(new Object[]{"A"});
        AvroSecondaryIndex index = new AvroSecondaryIndex(
                "group_age_idx", List.of("GROUP", "AGE"), AvroSecondaryIndex.CompositeKey.class);
        index.insert(new AvroSecondaryIndex.CompositeKey(new Object[]{"A", 30}), 2);
        index.insert(new AvroSecondaryIndex.CompositeKey(new Object[]{"A", 20}), 0);
        index.insert(new AvroSecondaryIndex.CompositeKey(new Object[]{"B", 20}), 1);

        assertEquals(List.of(0, 2), index.prefixSearch(prefix));
        assertEquals(List.of(0), index.compositeSearch(
                new AvroSecondaryIndex.CompositeKey(new Object[]{"A", 20})));
    }

    @Test
    void storageInsertAtKeepsSecondaryIndexPositionsAligned() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.createSecondaryIndex("age_idx", "AGE");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 20, false));
        storage.insert(mapRow(3L, "Charlie", 30, true));

        storage.insertAt(1, mapRow(4L, "Diana", 30, false));
        AvroSecondaryIndex index = storage.getSecondaryIndexManager().getIndex("age_idx");
        assertEquals(List.of(0, 1, 3), index.search(30));

        storage.delete(0);
        assertEquals(List.of(0, 2), index.search(30));
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(4L));
        assertEquals(1, storage.getPrimaryKeyIndex().lookup(2L));
    }

    @Test
    void storageInsertAtDuplicateKeyLeavesRowsAndIndexesUnchanged() {
        AvroRowStorage storage = new AvroRowStorage("TEST", cols(), types());
        storage.setPrimaryKeyColumn("ID");
        storage.createSecondaryIndex("age_idx", "AGE");
        storage.insert(mapRow(1L, "Alice", 30, true));
        storage.insert(mapRow(2L, "Bob", 20, false));

        assertThrows(IllegalArgumentException.class,
                () -> storage.insertAt(1, mapRow(1L, "Duplicate", 30, false)));
        assertEquals(2, storage.scan().size());
        assertEquals(0, storage.getPrimaryKeyIndex().lookup(1L));
        assertEquals(1, storage.getPrimaryKeyIndex().lookup(2L));
        assertEquals(List.of(0), storage.getSecondaryIndexManager().getIndex("age_idx").search(30));
        assertEquals(List.of(1), storage.getSecondaryIndexManager().getIndex("age_idx").search(20));
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
