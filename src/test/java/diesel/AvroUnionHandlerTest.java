package diesel;

import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.AvroSchemaManager;
import diesel.storage.avro.AvroTypeMapper;
import diesel.storage.avro.AvroUnionHandler;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AvroUnionHandler} — Prompt 74.
 * Covers null-first union construction, ordering validation, branch resolution,
 * the fast-path write/read helpers, and the AvroRowStorage delegation.
 */
@Tag("storage")
@StorageType("avro")
class AvroUnionHandlerTest {

    @TempDir
    Path tempDir;

    private static Schema str() {
        return Schema.create(Schema.Type.STRING);
    }

    private static Schema intSchema() {
        return Schema.create(Schema.Type.INT);
    }

    private static Schema longSchema() {
        return Schema.create(Schema.Type.LONG);
    }

    private static Schema nullSchema() {
        return Schema.create(Schema.Type.NULL);
    }

    // ─── createNullableUnion ────────────────────────────────────────

    @Test
    void createNullableUnionPutsNullFirst() {
        Schema union = AvroUnionHandler.createNullableUnion(str());
        assertEquals(Schema.Type.UNION, union.getType());
        List<Schema> types = union.getTypes();
        assertEquals(2, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertEquals(Schema.Type.STRING, types.get(1).getType());
    }

    @Test
    void createNullableUnionPreservesLogicalTypes() {
        Schema date = org.apache.avro.LogicalTypes.date()
                .addToSchema(Schema.create(Schema.Type.INT));
        Schema union = AvroUnionHandler.createNullableUnion(date);
        assertEquals(Schema.Type.INT, union.getTypes().get(1).getType());
        assertEquals("date", union.getTypes().get(1).getLogicalType().getName());
    }

    @Test
    void createNullableUnionRejectsUnionInput() {
        Schema inner = AvroUnionHandler.createNullableUnion(str());
        assertThrows(IllegalArgumentException.class, () -> AvroUnionHandler.createNullableUnion(inner));
    }

    @Test
    void createNullableUnionRejectsNullTypeInput() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createNullableUnion(nullSchema()));
    }

    @Test
    void createNullableUnionRejectsNullSchema() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createNullableUnion(null));
    }

    // ─── createUnion ────────────────────────────────────────────────

    @Test
    void createUnionSortsNullFirst() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        List<Schema> types = union.getTypes();
        assertEquals(3, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertEquals(Schema.Type.STRING, types.get(1).getType());
        assertEquals(Schema.Type.INT, types.get(2).getType());
    }

    @Test
    void createUnionListOverload() {
        List<Schema> branches = new ArrayList<>();
        branches.add(intSchema());
        branches.add(nullSchema());
        Schema union = AvroUnionHandler.createUnion(branches);
        assertEquals(Schema.Type.NULL, union.getTypes().get(0).getType());
        assertEquals(Schema.Type.INT, union.getTypes().get(1).getType());
    }

    @Test
    void createUnionRejectsDuplicateBranches() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createUnion(str(), str(), nullSchema()));
    }

    @Test
    void createUnionRejectsDuplicateNamedBranches() {
        Schema r1 = recordSchema("Dupe", "ns");
        Schema r2 = recordSchema("Dupe", "ns");
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createUnion(r1, r2, nullSchema()));
    }

    @Test
    void createUnionAllowsDistinctNamedBranches() {
        Schema r1 = recordSchema("A", "ns");
        Schema r2 = recordSchema("B", "ns");
        Schema union = AvroUnionHandler.createUnion(r1, r2, nullSchema());
        assertEquals(3, union.getTypes().size());
    }

    @Test
    void createUnionRejectsNullBranchElement() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createUnion(str(), null));
    }

    @Test
    void createUnionRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, AvroUnionHandler::createUnion);
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createUnion(List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.createUnion((List<Schema>) null));
    }

    @Test
    void createUnionNullFirstConfigFalsePreservesOrder() {
        String key = AvroUnionHandler.UNION_NULL_FIRST_KEY;
        System.setProperty(key, "false");
        try {
            Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
            assertEquals(Schema.Type.STRING, union.getTypes().get(0).getType());
            assertEquals(Schema.Type.NULL, union.getTypes().get(1).getType());
            assertEquals(Schema.Type.INT, union.getTypes().get(2).getType());
        } finally {
            System.clearProperty(key);
        }
    }

    // ─── isNullableUnion / getNullBranchIndex ───────────────────────

    @Test
    void isNullableUnionDetectsNullBranch() {
        assertTrue(AvroUnionHandler.isNullableUnion(AvroUnionHandler.createNullableUnion(intSchema())));
        assertFalse(AvroUnionHandler.isNullableUnion(str()));
        assertFalse(AvroUnionHandler.isNullableUnion(null));
        assertFalse(AvroUnionHandler.isNullableUnion(AvroUnionHandler.createUnion(str(), intSchema())));
    }

    @Test
    void getNullBranchIndexLocatesNull() {
        assertEquals(0, AvroUnionHandler.getNullBranchIndex(AvroUnionHandler.createNullableUnion(intSchema())));
        assertEquals(-1, AvroUnionHandler.getNullBranchIndex(str()));
        assertEquals(1, AvroUnionHandler.getNullBranchIndex(
                Schema.createUnion(List.of(str(), nullSchema(), intSchema()))));
    }

    // ─── getNonNullableBranches / isSingleTypeUnion ─────────────────

    @Test
    void getNonNullableBranchesFiltersNull() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        List<Schema> branches = AvroUnionHandler.getNonNullableBranches(union);
        assertEquals(2, branches.size());
        assertTrue(branches.stream().noneMatch(s -> s.getType() == Schema.Type.NULL));
    }

    @Test
    void isSingleTypeUnion() {
        assertTrue(AvroUnionHandler.isSingleTypeUnion(AvroUnionHandler.createNullableUnion(str())));
        assertFalse(AvroUnionHandler.isSingleTypeUnion(
                AvroUnionHandler.createUnion(str(), nullSchema(), intSchema())));
        assertFalse(AvroUnionHandler.isSingleTypeUnion(str()));
        assertFalse(AvroUnionHandler.isSingleTypeUnion(null));
    }

    // ─── unwrapUnion / unwrapNonNullType ────────────────────────────

    @Test
    void unwrapUnionReturnsFirstNonNullBranch() {
        Schema union = AvroUnionHandler.createNullableUnion(longSchema());
        Schema unwrapped = AvroUnionHandler.unwrapUnion(union);
        assertEquals(Schema.Type.LONG, unwrapped.getType());
    }

    @Test
    void unwrapUnionReturnsMultiBranchNonNullFirst() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        assertEquals(Schema.Type.STRING, AvroUnionHandler.unwrapUnion(union).getType());
    }

    @Test
    void unwrapUnionPassesThroughNonUnion() {
        assertEquals(Schema.Type.STRING, AvroUnionHandler.unwrapUnion(str()).getType());
        assertNull(AvroUnionHandler.unwrapUnion(null));
    }

    @Test
    void unwrapNonNullTypeMapsJavaClass() {
        Schema union = AvroUnionHandler.createNullableUnion(longSchema());
        assertEquals(Long.class, AvroUnionHandler.unwrapNonNullType(union));
        assertEquals(String.class, AvroUnionHandler.unwrapNonNullType(str()));
    }

    @Test
    void toJavaTypeDelegatesUnionToHandler() {
        Schema date = org.apache.avro.LogicalTypes.date()
                .addToSchema(Schema.create(Schema.Type.INT));
        Schema dateUnion = AvroUnionHandler.createNullableUnion(date);
        assertEquals(LocalDate.class, AvroTypeMapper.toJavaType(dateUnion));
        Schema longUnion = AvroUnionHandler.createNullableUnion(longSchema());
        assertEquals(Long.class, AvroTypeMapper.toJavaType(longUnion));
    }

    // ─── resolveBranchIndex ─────────────────────────────────────────

    @Test
    void resolveBranchIndexNullValue() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        assertEquals(0, AvroUnionHandler.resolveBranchIndex(union, null));
    }

    @Test
    void resolveBranchIndexMatchesString() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        assertEquals(1, AvroUnionHandler.resolveBranchIndex(union, "hello"));
    }

    @Test
    void resolveBranchIndexMatchesInt() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        assertEquals(2, AvroUnionHandler.resolveBranchIndex(union, 42));
    }

    @Test
    void resolveBranchIndexNoMatch() {
        Schema union = AvroUnionHandler.createUnion(str(), nullSchema(), intSchema());
        assertEquals(-1, AvroUnionHandler.resolveBranchIndex(union, 42L));
    }

    @Test
    void resolveBranchIndexNullValueWithoutNullBranch() {
        Schema union = AvroUnionHandler.createUnion(str(), intSchema());
        assertEquals(-1, AvroUnionHandler.resolveBranchIndex(union, null));
    }

    @Test
    void resolveBranchIndexNonUnionThrows() {
        assertThrows(IllegalArgumentException.class, () -> AvroUnionHandler.resolveBranchIndex(str(), "x"));
    }

    // ─── wrapForWrite ───────────────────────────────────────────────

    private static final AvroUnionHandler.ValueConverter PASSTHROUGH = (v, s) -> v;

    @Test
    void wrapForWriteNonUnionDelegatesDirectly() {
        Object result = AvroUnionHandler.wrapForWrite("abc", str(), PASSTHROUGH);
        assertEquals("abc", result);
    }

    @Test
    void wrapForWriteNullFastPathSkipsConverter() {
        Schema union = AvroUnionHandler.createNullableUnion(str());
        AvroUnionHandler.ValueConverter throwing = (v, s) -> {
            throw new AssertionError("converter must not run for null values");
        };
        assertNull(AvroUnionHandler.wrapForWrite(null, union, throwing));
    }

    @Test
    void wrapForWriteSingleNullableConvertsOnBranch() {
        Schema union = AvroUnionHandler.createNullableUnion(longSchema());
        AvroUnionHandler.ValueConverter capture = (v, s) -> {
            assertEquals(Schema.Type.LONG, s.getType());
            return v;
        };
        assertEquals(7L, AvroUnionHandler.wrapForWrite(7L, union, capture));
    }

    @Test
    void wrapForWriteMultiBranchSelectsCorrectBranch() {
        Schema union = AvroUnionHandler.createUnion(str(), intSchema(), nullSchema());
        List<Schema.Type> seen = new ArrayList<>();
        AvroUnionHandler.ValueConverter capture = (v, s) -> {
            seen.add(s.getType());
            return v;
        };
        AvroUnionHandler.wrapForWrite("x", union, capture);
        AvroUnionHandler.wrapForWrite(5, union, capture);
        assertEquals(List.of(Schema.Type.STRING, Schema.Type.INT), seen);
    }

    @Test
    void wrapForWriteNoMatchThrows() {
        Schema union = AvroUnionHandler.createNullableUnion(str());
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.wrapForWrite(true, union, PASSTHROUGH));
    }

    @Test
    void wrapForWriteNullConverterThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroUnionHandler.wrapForWrite("x", str(), null));
    }

    // ─── unwrapForRead ──────────────────────────────────────────────

    @Test
    void unwrapForReadNullValueResolvesNullBranch() {
        Schema union = AvroUnionHandler.createNullableUnion(str());
        AvroUnionHandler.UnionReadValue uv = AvroUnionHandler.unwrapForRead(null, union);
        assertNull(uv.value());
        assertEquals(Schema.Type.NULL, uv.branchSchema().getType());
    }

    @Test
    void unwrapForReadSelectsBranchByValueType() {
        Schema union = AvroUnionHandler.createUnion(str(), intSchema(), nullSchema());
        AvroUnionHandler.UnionReadValue uv = AvroUnionHandler.unwrapForRead(42, union);
        assertEquals(Schema.Type.INT, uv.branchSchema().getType());
        assertEquals(42, uv.value());
        AvroUnionHandler.UnionReadValue sv = AvroUnionHandler.unwrapForRead("xx", union);
        assertEquals(Schema.Type.STRING, sv.branchSchema().getType());
    }

    @Test
    void unwrapForReadNonUnionPassthrough() {
        AvroUnionHandler.UnionReadValue uv = AvroUnionHandler.unwrapForRead("x", str());
        assertEquals(Schema.Type.STRING, uv.branchSchema().getType());
        assertEquals("x", uv.value());
    }

    // ─── validateUnionOrdering ──────────────────────────────────────

    @Test
    void validateUnionOrderingNullFirstOk() {
        assertTrue(AvroUnionHandler.validateUnionOrdering(
                AvroUnionHandler.createNullableUnion(intSchema())).ok());
        assertTrue(AvroUnionHandler.validateUnionOrdering(intSchema()).ok());
        assertTrue(AvroUnionHandler.validateUnionOrdering(null).ok());
        assertTrue(AvroUnionHandler.validateUnionOrdering(
                AvroUnionHandler.createUnion(str(), intSchema())).ok());
    }

    @Test
    void validateUnionOrderingNullNotFirstFails() {
        Schema union = Schema.createUnion(List.of(str(), nullSchema(), intSchema()));
        AvroUnionHandler.UnionOrdering ordering = AvroUnionHandler.validateUnionOrdering(union);
        assertFalse(ordering.ok());
        assertTrue(ordering.message().contains("index 1"));
    }

    @Test
    void validateUnionOrderingConfigCanDisable() {
        String key = AvroUnionHandler.UNION_VALIDATE_ORDERING_KEY;
        System.setProperty(key, "false");
        try {
            Schema union = Schema.createUnion(List.of(str(), nullSchema(), intSchema()));
            assertTrue(AvroUnionHandler.validateUnionOrdering(union).ok());
        } finally {
            System.clearProperty(key);
        }
    }

    // ─── Integration: AvroRowStorage delegation ────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "BALANCE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("BALANCE", BigDecimal.class);
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

    @Test
    void nullValuesRoundTripThroughStorage() {
        AvroRowStorage s = new AvroRowStorage("union_roundtrip", cols(), types());
        s.setDataDir(tempDir.toString());
        s.insert(row(1L, null, null, new BigDecimal("100.50")));
        s.insert(row(2L, "Bob", 30, null));
        s.insert(row(3L, "Carol", null, new BigDecimal("0.00")));
        s.saveToFile("union_roundtrip");

        AvroRowStorage t = new AvroRowStorage("union_roundtrip", cols(), types());
        t.setDataDir(tempDir.toString());
        t.loadFromFile("union_roundtrip");
        List<Object[]> rows = t.getInternalRows();
        assertEquals(3, rows.size());
        assertNull(rows.get(0)[1]);
        assertNull(rows.get(0)[2]);
        assertEquals(0, new BigDecimal("100.50").compareTo((BigDecimal) rows.get(0)[3]));
        assertEquals("Bob", rows.get(1)[1]);
        assertEquals(30, rows.get(1)[2]);
        assertNull(rows.get(1)[3]);
        assertNull(rows.get(2)[2]);
    }

    @Test
    void nullableColumnSchemaFromStoragePutsNullFirst() throws Exception {
        AvroRowStorage s = new AvroRowStorage("union_schema", cols(), types());
        s.setDataDir(tempDir.toString());
        s.insert(row(1L, "A", 1, new BigDecimal("1.00")));
        s.saveToFile("union_schema");
        Schema schema = AvroSchemaManager.readSchemaFile(
                tempDir.resolve("union_schema.avsc"));
        for (Schema.Field field : schema.getFields()) {
            Schema fieldSchema = field.schema();
            assertEquals(Schema.Type.UNION, fieldSchema.getType());
            assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
            assertTrue(AvroUnionHandler.isNullableUnion(fieldSchema));
        }
    }

    @Test
    void toAvroTypeAndBackThroughNullableUnion() {
        Schema union = AvroUnionHandler.createNullableUnion(
                org.apache.avro.LogicalTypes.timestampMillis()
                        .addToSchema(Schema.create(Schema.Type.LONG)));
        assertEquals(LocalDateTime.class, AvroTypeMapper.toJavaType(union));
        AvroUnionHandler.UnionReadValue uv = AvroUnionHandler.unwrapForRead(1700000000000L, union);
        assertEquals(Schema.Type.LONG, uv.branchSchema().getType());
    }

    private static Schema recordSchema(String name, String namespace) {
        Schema record = Schema.createRecord(name, null, namespace, false);
        List<Schema.Field> fields = List.of(
                new Schema.Field("v", Schema.create(Schema.Type.STRING), null, null));
        record.setFields(fields);
        return record;
    }
}