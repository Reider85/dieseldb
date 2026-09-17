package diesel;

import diesel.storage.avro.AvroRowStorage;
import diesel.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("storage")
class AvroRowStorageTest {

    @TempDir
    Path tempDir;

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "BIRTHDATE", "LAST_LOGIN", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("BALANCE", BigDecimal.class);
        t.put("BIRTHDATE", LocalDate.class);
        t.put("LAST_LOGIN", LocalDateTime.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        List<String> c = cols();
        for (int i = 0; i < vals.length; i++) {
            r.put(c.get(i), vals[i]);
        }
        return r;
    }

    private AvroRowStorage createStorage(String tableName) {
        AvroRowStorage s = new AvroRowStorage(tableName, cols(), types());
        s.setDataDir(tempDir.toString());
        return s;
    }

    // ─── Basic CRUD ────────────────────────────────────────────────

    @Test
    void testInsertAndScan() {
        AvroRowStorage s = createStorage("crud_test");
        Map<String, Object> r1 = row(1L, "Alice", 30, new BigDecimal("1000.50"),
                LocalDate.of(1994, 5, 15), LocalDateTime.of(2024, 1, 10, 14, 30), true);
        Map<String, Object> r2 = row(2L, "Bob", 25, new BigDecimal("2000.00"),
                LocalDate.of(1999, 8, 20), LocalDateTime.of(2024, 2, 5, 9, 0), false);
        s.insert(r1);
        s.insert(r2);

        List<Map<String, Object>> result = s.scan();
        assertEquals(2, result.size());
        assertEquals("Alice", result.get(0).get("NAME"));
        assertEquals(30, result.get(0).get("AGE"));
        assertEquals("Bob", result.get(1).get("NAME"));
        assertEquals(false, result.get(1).get("ACTIVE"));
    }

    @Test
    void testInsertAt() {
        AvroRowStorage s = createStorage("insertat_test");
        s.insert(row(1L, "Alice", 30, new BigDecimal("100"), LocalDate.now(), LocalDateTime.now(), true));
        s.insert(row(3L, "Charlie", 40, new BigDecimal("300"), LocalDate.now(), LocalDateTime.now(), true));
        s.insertAt(1, row(2L, "Bob", 25, new BigDecimal("200"), LocalDate.now(), LocalDateTime.now(), false));

        List<Map<String, Object>> result = s.scan();
        assertEquals(3, result.size());
        assertEquals("Bob", result.get(1).get("NAME"));
        assertEquals(2L, result.get(1).get("ID"));
    }

    @Test
    void testUpdate() {
        AvroRowStorage s = createStorage("update_test");
        s.insert(row(1L, "Alice", 30, new BigDecimal("100"), LocalDate.now(), LocalDateTime.now(), true));
        s.update(0, row(1L, "Alice Updated", 31, new BigDecimal("150"), LocalDate.now(), LocalDateTime.now(), true));

        List<Map<String, Object>> result = s.scan();
        assertEquals(1, result.size());
        assertEquals("Alice Updated", result.get(0).get("NAME"));
        assertEquals(31, result.get(0).get("AGE"));
    }

    @Test
    void testDelete() {
        AvroRowStorage s = createStorage("delete_test");
        s.insert(row(1L, "Alice", 30, new BigDecimal("100"), LocalDate.now(), LocalDateTime.now(), true));
        s.insert(row(2L, "Bob", 25, new BigDecimal("200"), LocalDate.now(), LocalDateTime.now(), false));
        s.delete(0);

        List<Map<String, Object>> result = s.scan();
        assertEquals(1, result.size());
        assertEquals("Bob", result.get(0).get("NAME"));
    }

    @Test
    void testSetRows() {
        AvroRowStorage s = createStorage("setrows_test");
        s.insert(row(1L, "Alice", 30, new BigDecimal("100"), LocalDate.now(), LocalDateTime.now(), true));

        List<Map<String, Object>> newRows = new ArrayList<>();
        newRows.add(row(10L, "X", 1, new BigDecimal("1"), LocalDate.now(), LocalDateTime.now(), false));
        newRows.add(row(20L, "Y", 2, new BigDecimal("2"), LocalDate.now(), LocalDateTime.now(), true));
        s.setRows(newRows);

        List<Map<String, Object>> result = s.scan();
        assertEquals(2, result.size());
        assertEquals("X", result.get(0).get("NAME"));
        assertEquals("Y", result.get(1).get("NAME"));
    }

    // ─── Persistence round-trip ─────────────────────────────────────

    @Test
    void testSaveAndLoadRoundTrip() {
        String tableName = "roundtrip_test";
        AvroRowStorage s1 = createStorage(tableName);
        Map<String, Object> r1 = row(1L, "Alice", 30, new BigDecimal("1000.50"),
                LocalDate.of(1994, 5, 15), LocalDateTime.of(2024, 1, 10, 14, 30), true);
        Map<String, Object> r2 = row(2L, "Bob", 25, new BigDecimal("2000.00"),
                LocalDate.of(1999, 8, 20), LocalDateTime.of(2024, 2, 5, 9, 0), false);
        s1.insert(r1);
        s1.insert(r2);
        s1.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);

        List<Map<String, Object>> result = s2.scan();
        assertEquals(2, result.size());
        assertEquals(1L, result.get(0).get("ID"));
        assertEquals("Alice", result.get(0).get("NAME"));
        assertEquals(30, result.get(0).get("AGE"));
        assertEquals(new BigDecimal("1000.50").compareTo((BigDecimal) result.get(0).get("BALANCE")), 0);
        assertEquals(LocalDate.of(1994, 5, 15), result.get(0).get("BIRTHDATE"));
        assertEquals(LocalDateTime.of(2024, 1, 10, 14, 30), result.get(0).get("LAST_LOGIN"));
        assertEquals(true, result.get(0).get("ACTIVE"));

        assertEquals(2L, result.get(1).get("ID"));
        assertEquals("Bob", result.get(1).get("NAME"));
    }

    @Test
    void testSaveAndLoadEmptyTable() {
        String tableName = "empty_test";
        AvroRowStorage s1 = createStorage(tableName);
        s1.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);

        assertTrue(s2.scan().isEmpty());
    }

    @Test
    void testLoadFromFileNotFound() {
        AvroRowStorage s = createStorage("nonexistent_table");
        s.loadFromFile("nonexistent_table");
        assertTrue(s.scan().isEmpty());
    }

    // ─── All scalar types ──────────────────────────────────────────

    @Test
    void testAllScalarTypes() {
        String tableName = "scalars_test";
        AvroRowStorage s = createStorage(tableName);
        Map<String, Object> r = row(42L, "Test", 99, new BigDecimal("9999.99"),
                LocalDate.of(2000, 1, 1), LocalDateTime.of(2024, 6, 15, 12, 0), false);
        s.insert(r);
        s.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);

        Map<String, Object> loaded = s2.scan().get(0);
        assertEquals(42L, loaded.get("ID"));
        assertEquals("Test", loaded.get("NAME"));
        assertEquals(99, loaded.get("AGE"));
        assertEquals(0, new BigDecimal("9999.99").compareTo((BigDecimal) loaded.get("BALANCE")));
        assertEquals(LocalDate.of(2000, 1, 1), loaded.get("BIRTHDATE"));
        assertEquals(LocalDateTime.of(2024, 6, 15, 12, 0), loaded.get("LAST_LOGIN"));
        assertEquals(false, loaded.get("ACTIVE"));
    }

    // ─── Null values ────────────────────────────────────────────────

    @Test
    void testNullValues() {
        String tableName = "nulls_test";
        AvroRowStorage s = createStorage(tableName);
        Map<String, Object> r = row(null, "NullTest", null, null, null, null, null);
        s.insert(r);
        s.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);

        Map<String, Object> loaded = s2.scan().get(0);
        assertNull(loaded.get("ID"));
        assertEquals("NullTest", loaded.get("NAME"));
        assertNull(loaded.get("AGE"));
        assertNull(loaded.get("BALANCE"));
        assertNull(loaded.get("BIRTHDATE"));
        assertNull(loaded.get("LAST_LOGIN"));
        assertNull(loaded.get("ACTIVE"));
    }

    // ─── StorageFactory integration ─────────────────────────────────

    @Test
    void testStorageFactoryCreatesAvro() {
        List<String> cols = List.of("X");
        Map<String, Class<?>> types = Map.of("X", String.class);
        var storage = StorageFactory.create("avro", "factory_test", cols, types);
        assertInstanceOf(AvroRowStorage.class, storage);
    }

    // ─── Multiple save/load cycles ──────────────────────────────────

    @Test
    void testMultipleSaveLoadCycles() {
        String tableName = "multi_cycle_test";
        AvroRowStorage s = createStorage(tableName);
        s.insert(row(1L, "First", 10, new BigDecimal("10"), LocalDate.now(), LocalDateTime.now(), true));
        s.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);
        assertEquals(1, s2.scan().size());
        assertEquals("First", s2.scan().get(0).get("NAME"));

        s2.insert(row(2L, "Second", 20, new BigDecimal("20"), LocalDate.now(), LocalDateTime.now(), false));
        s2.saveToFile(tableName);

        AvroRowStorage s3 = createStorage(tableName);
        s3.loadFromFile(tableName);
        assertEquals(2, s3.scan().size());
        assertEquals("First", s3.scan().get(0).get("NAME"));
        assertEquals("Second", s3.scan().get(1).get("NAME"));
    }

    // ─── File existence ─────────────────────────────────────────────

    @Test
    void testAvroFileCreated() {
        String tableName = "file_exists_test";
        AvroRowStorage s = createStorage(tableName);
        s.insert(row(1L, "Test", 1, new BigDecimal("1"), LocalDate.now(), LocalDateTime.now(), true));
        s.saveToFile(tableName);

        File avroFile = new File(tempDir.toString(), tableName + ".avro");
        assertTrue(avroFile.exists());
        assertTrue(avroFile.length() > 0);
    }

    // ─── Large row set ──────────────────────────────────────────────

    @Test
    void testLargeRowSet() {
        String tableName = "large_test";
        AvroRowStorage s = createStorage(tableName);
        for (int i = 0; i < 1000; i++) {
            s.insert(row((long) i, "User" + i, i % 100, new BigDecimal(i + ".50"),
                    LocalDate.of(2000 + (i % 24), 1, 1), LocalDateTime.now(), i % 2 == 0));
        }
        s.saveToFile(tableName);

        AvroRowStorage s2 = createStorage(tableName);
        s2.loadFromFile(tableName);

        assertEquals(1000, s2.scan().size());
        assertEquals("User0", s2.scan().get(0).get("NAME"));
        assertEquals("User999", s2.scan().get(999).get("NAME"));
    }
}
