package diesel;

import diesel.storage.avro.AvroArrayHandler;
import diesel.storage.avro.AvroEnumHandler;
import diesel.storage.avro.AvroMapHandler;
import diesel.storage.avro.AvroRecordHandler;
import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.AvroTypeMapper;
import diesel.storage.avro.AvroUnionHandler;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Prompt 75 complex-type handlers (ARRAY / MAP / RECORD / ENUM)
 * and their wiring into {@link AvroTypeMapper}, {@link AvroUnionHandler} and
 * {@link AvroRowStorage}.
 */
@Tag("storage")
@StorageType("avro")
class AvroComplexTypeTest {

    @TempDir
    Path tempDir;

    private static final AvroUnionHandler.ValueConverter PASSTHROUGH = (v, s) -> v;

    private Object nestedWrite(Object v, Schema s) {
        if (v == null || s == null) {
            return v;
        }
        Schema base = s.getType() == Schema.Type.UNION ? AvroUnionHandler.unwrapUnion(s) : s;
        return base != null && base.getType() == Schema.Type.RECORD
                ? AvroRecordHandler.toAvroRecord(v, base, this::nestedWrite) : v;
    }

    private Object nestedRead(Object v, Schema s) {
        if (v == null || s == null) {
            return v;
        }
        Schema base = s.getType() == Schema.Type.UNION ? AvroUnionHandler.unwrapUnion(s) : s;
        return base != null && base.getType() == Schema.Type.RECORD
                ? AvroRecordHandler.fromAvroRecord(v, base, this::nestedRead) : v;
    }

    private final AvroUnionHandler.ValueConverter NESTED_WRITE = this::nestedWrite;
    private final AvroUnionHandler.ValueConverter NESTED_READ = this::nestedRead;

    // ─── ARRAY handler ─────────────────────────────────────────────

    @Test
    void arraySchemaBuilders() {
        Schema arr = AvroArrayHandler.createArraySchema(Schema.create(Schema.Type.STRING));
        assertEquals(Schema.Type.ARRAY, arr.getType());
        assertEquals(Schema.Type.STRING, arr.getElementType().getType());
        Schema nullable = AvroArrayHandler.createNullableArraySchema(Schema.create(Schema.Type.INT));
        assertTrue(AvroUnionHandler.isNullableUnion(nullable));
        assertEquals(Schema.Type.ARRAY, nullable.getTypes().get(1).getType());
        assertThrows(IllegalArgumentException.class,
                () -> AvroArrayHandler.createArraySchema(null));
    }

    @Test
    void arrayRoundTrip() {
        Schema arr = AvroArrayHandler.createArraySchema(Schema.create(Schema.Type.STRING));
        List<Object> value = List.of("a", "b", "c");
        Object out = AvroArrayHandler.toAvroArray(value, arr, PASSTHROUGH);
        assertInstanceOf(GenericData.Array.class, out);
        assertEquals(value, AvroArrayHandler.fromAvroArray(out, arr, PASSTHROUGH));
        assertNull(AvroArrayHandler.toAvroArray(null, arr, PASSTHROUGH));
        assertNull(AvroArrayHandler.fromAvroArray(null, arr, PASSTHROUGH));
    }

    @Test
    void arrayNullableElementsAndObjectArray() {
        Schema arr = AvroArrayHandler.createArraySchema(
                AvroUnionHandler.createNullableUnion(Schema.create(Schema.Type.STRING)));
        Object[] value = new Object[]{"x", null, "z"};
        Object out = AvroArrayHandler.toAvroArray(value, arr, PASSTHROUGH);
        assertEquals(Arrays.asList("x", null, "z"), AvroArrayHandler.fromAvroArray(out, arr, PASSTHROUGH));
    }

    @Test
    void arrayOverMaxSizeRejected() {
        Schema arr = AvroArrayHandler.createArraySchema(Schema.create(Schema.Type.INT));
        String key = AvroArrayHandler.MAX_SIZE_KEY;
        System.setProperty(key, "2");
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> AvroArrayHandler.toAvroArray(List.of(1, 2, 3), arr, PASSTHROUGH));
            assertEquals(List.of(1, 2), AvroArrayHandler.fromAvroArray(
                    AvroArrayHandler.toAvroArray(List.of(1, 2), arr, PASSTHROUGH), arr, PASSTHROUGH));
        } finally {
            System.clearProperty(key);
        }
    }

    // ─── MAP handler ────────────────────────────────────────────────

    @Test
    void mapSchemaBuilders() {
        Schema m = AvroMapHandler.createMapSchema(Schema.create(Schema.Type.INT));
        assertEquals(Schema.Type.MAP, m.getType());
        assertEquals(Schema.Type.INT, m.getValueType().getType());
        Schema nullable = AvroMapHandler.createNullableMapSchema(Schema.create(Schema.Type.INT));
        assertTrue(AvroUnionHandler.isNullableUnion(nullable));
        assertEquals(Schema.Type.MAP, nullable.getTypes().get(1).getType());
        assertThrows(IllegalArgumentException.class,
                () -> AvroMapHandler.createMapSchema(null));
    }

    @Test
    void mapRoundTrip() {
        Schema m = AvroMapHandler.createMapSchema(
                AvroUnionHandler.createNullableUnion(Schema.create(Schema.Type.STRING)));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("k1", "v1");
        value.put("k2", null);
        Object out = AvroMapHandler.toAvroMap(value, m, PASSTHROUGH);
        assertEquals(value, AvroMapHandler.fromAvroMap(out, m, PASSTHROUGH));
        assertNull(AvroMapHandler.toAvroMap(null, m, PASSTHROUGH));
        assertNull(AvroMapHandler.fromAvroMap(null, m, PASSTHROUGH));
    }

    @Test
    void mapOverMaxSizeRejected() {
        Schema m = AvroMapHandler.createMapSchema(Schema.create(Schema.Type.STRING));
        String key = AvroMapHandler.MAX_SIZE_KEY;
        System.setProperty(key, "1");
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> AvroMapHandler.toAvroMap(Map.of("a", "1", "b", "2"), m, PASSTHROUGH));
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void mapNullKeyRejected() {
        Schema m = AvroMapHandler.createMapSchema(Schema.create(Schema.Type.STRING));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(null, "x");
        assertThrows(IllegalArgumentException.class,
                () -> AvroMapHandler.toAvroMap(value, m, PASSTHROUGH));
    }

    // ─── ENUM handler ───────────────────────────────────────────────

    @Test
    void enumCanonicalizesSymbolCaseInsensitiveByDefault() {
        Schema e = AvroEnumHandler.createEnumSchema("Status", List.of("ACTIVE", "PENDING", "CLOSED"), "diesel.avro");
        assertTrue(AvroEnumHandler.isValidSymbol(e, "active"));
        Object symbol = AvroEnumHandler.toAvroEnum("active", e);
        assertInstanceOf(GenericData.EnumSymbol.class, symbol);
        assertEquals("ACTIVE", AvroEnumHandler.fromAvroEnum(symbol));
        assertEquals(List.of("ACTIVE", "PENDING", "CLOSED"), AvroEnumHandler.symbols(e));
    }

    @Test
    void enumCaseSensitiveConfig() {
        Schema e = AvroEnumHandler.createEnumSchema("Status", List.of("ACTIVE", "CLOSED"), "diesel.avro");
        String key = AvroEnumHandler.CASE_SENSITIVE_KEY;
        System.setProperty(key, "true");
        try {
            assertFalse(AvroEnumHandler.isValidSymbol(e, "active"));
            assertThrows(IllegalArgumentException.class, () -> AvroEnumHandler.toAvroEnum("active", e));
            assertEquals("ACTIVE", AvroEnumHandler.fromAvroEnum(AvroEnumHandler.toAvroEnum("ACTIVE", e)));
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void enumInvalidSymbolThrows() {
        Schema e = AvroEnumHandler.createEnumSchema("Status", List.of("ACTIVE", "CLOSED"), "diesel.avro");
        assertThrows(IllegalArgumentException.class, () -> AvroEnumHandler.toAvroEnum("BANNED", e));
    }

    @Test
    void enumSchemaValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroEnumHandler.createEnumSchema("", List.of("A"), null));
        assertThrows(IllegalArgumentException.class,
                () -> AvroEnumHandler.createEnumSchema("S", List.of(), null));
        assertThrows(IllegalArgumentException.class,
                () -> AvroEnumHandler.createEnumSchema("S", List.of("A", "A"), null));
        assertThrows(IllegalArgumentException.class,
                () -> AvroEnumHandler.createEnumSchema("S", List.of(" "), null));
    }

    // ─── RECORD handler ─────────────────────────────────────────────

    @Test
    void recordRoundTrip() {
        Schema inner = AvroRecordHandler.createRecordSchema("Inner",
                List.of(new Schema.Field("x", Schema.create(Schema.Type.INT), null, null)), "ns");
        Schema outer = AvroRecordHandler.createRecordSchema("Outer",
                List.of(new Schema.Field("code", Schema.create(Schema.Type.STRING), null, null),
                        new Schema.Field("inner", AvroUnionHandler.createNullableUnion(inner), null, null)), "ns");
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("CODE", "hello");
        value.put("inner", Map.of("x", 5));
        Object out = AvroRecordHandler.toAvroRecord(value, outer, NESTED_WRITE);
        assertInstanceOf(GenericData.Record.class, out);
        Map<String, Object> back = AvroRecordHandler.fromAvroRecord(out, outer, NESTED_READ);
        assertEquals("hello", back.get("code"));
        assertEquals(Map.of("x", 5), back.get("inner"));
        assertNull(AvroRecordHandler.toAvroRecord(null, outer, NESTED_WRITE));
        assertNull(AvroRecordHandler.fromAvroRecord(null, outer, NESTED_READ));
    }

    @Test
    void recordNestingDepthLimit() {
        Schema inner = AvroRecordHandler.createRecordSchema("Inner",
                List.of(new Schema.Field("x", Schema.create(Schema.Type.INT), null, null)), "ns");
        Schema mid = AvroRecordHandler.createRecordSchema("Mid",
                List.of(new Schema.Field("i", inner, null, null)), "ns");
        Schema outer = AvroRecordHandler.createRecordSchema("Outer",
                List.of(new Schema.Field("m", mid, null, null)), "ns");
        Map<String, Object> value = Map.of("m", Map.of("i", Map.of("x", 1)));
        String key = AvroRecordHandler.MAX_NESTING_DEPTH_KEY;
        System.setProperty(key, "2");
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> AvroRecordHandler.toAvroRecord(value, outer, NESTED_WRITE));
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void recursiveRecordSchemaRejected() {
        Schema a = Schema.createRecord("A", null, "ns", false);
        Schema b = Schema.createRecord("B", null, "ns", false);
        a.setFields(List.of(new Schema.Field("b", b, null, null)));
        b.setFields(List.of(new Schema.Field("a", a, null, null)));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("b", Map.of("a", Map.of("b", Map.of("a", Map.of()))));
        assertThrows(IllegalArgumentException.class,
                () -> AvroRecordHandler.toAvroRecord(value, a, NESTED_WRITE));
    }

    // ─── Union integration ──────────────────────────────────────────

    @Test
    void nullableEnumUnionResolvesStringValue() {
        Schema e = AvroEnumHandler.createEnumSchema("Status", List.of("ACTIVE", "CLOSED"), "diesel.avro");
        Schema union = AvroUnionHandler.createNullableUnion(e);
        Object out = AvroUnionHandler.wrapForWrite("active", union,
                (v, s) -> s.getType() == Schema.Type.ENUM ? AvroEnumHandler.toAvroEnum(v, s) : v);
        assertInstanceOf(GenericData.EnumSymbol.class, out);
        assertEquals("ACTIVE", AvroEnumHandler.fromAvroEnum(out));
        assertNull(AvroUnionHandler.wrapForWrite(null, union, (v, s) -> {
            throw new AssertionError("converter must not run for null values");
        }));
    }

    @Test
    void objectArrayResolvesArrayUnionBranch() {
        Schema union = AvroUnionHandler.createNullableUnion(
                AvroArrayHandler.createArraySchema(Schema.create(Schema.Type.STRING)));
        assertEquals(1, AvroUnionHandler.resolveBranchIndex(union, new Object[]{"a", "b"}));
        assertEquals(Schema.Type.ARRAY,
                AvroUnionHandler.unwrapForRead(new Object[]{"a"}, union).branchSchema().getType());
    }

    // ─── Type mapper mappings ───────────────────────────────────────

    @Test
    void typeMapperComplexMappings() {
        Schema arr = AvroTypeMapper.toAvroSchema(List.class, "tags");
        assertEquals(Schema.Type.ARRAY, arr.getType());
        assertEquals(Schema.Type.UNION, arr.getElementType().getType());
        assertEquals(Schema.Type.STRING, arr.getElementType().getTypes().get(1).getType());
        Schema map = AvroTypeMapper.toAvroSchema(Map.class, "attrs");
        assertEquals(Schema.Type.MAP, map.getType());
        assertEquals(List.class, AvroTypeMapper.toJavaType(AvroUnionHandler.createNullableUnion(arr)));
        assertEquals(Map.class, AvroTypeMapper.toJavaType(AvroUnionHandler.createNullableUnion(map)));
        assertEquals("ARRAY", AvroTypeMapper.typeName(List.class));
        assertEquals("MAP", AvroTypeMapper.typeName(Map.class));
        assertEquals(List.class, AvroTypeMapper.typeClass("ARRAY"));
        assertEquals(Map.class, AvroTypeMapper.typeClass("MAP"));
    }

    // ─── RowStorage integration (full save/load round trip) ─────────

    @Test
    void rowStorageComplexRoundTrip() {
        List<String> cols = List.of("ID", "TAGS", "ATTRS");
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("ID", Long.class);
        types.put("TAGS", List.class);
        types.put("ATTRS", Map.class);

        AvroRowStorage s = new AvroRowStorage("complex_roundtrip", cols, types);
        s.setDataDir(tempDir.toString());
        Map<String, Object> r1 = new LinkedHashMap<>();
        r1.put("ID", 1L);
        r1.put("TAGS", List.of("a", "b", "c"));
        r1.put("ATTRS", Map.of("k", "v", "n", "42"));
        Map<String, Object> r2 = new LinkedHashMap<>();
        r2.put("ID", 2L);
        r2.put("TAGS", null);
        r2.put("ATTRS", null);
        s.insert(r1);
        s.insert(r2);
        s.saveToFile("complex_roundtrip");

        AvroRowStorage t = new AvroRowStorage("complex_roundtrip", cols, types);
        t.setDataDir(tempDir.toString());
        t.loadFromFile("complex_roundtrip");
        List<Object[]> rows = t.getInternalRows();
        assertEquals(2, rows.size());
        assertEquals(List.of("a", "b", "c"), rows.get(0)[1]);
        assertEquals(Map.of("k", "v", "n", "42"), rows.get(0)[2]);
        assertNull(rows.get(1)[1]);
        assertNull(rows.get(1)[2]);
    }
}