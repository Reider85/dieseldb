package diesel;

import diesel.storage.avro.AvroSchemaManager;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AvroTypeMapper} and {@link AvroSchemaManager} — Prompt 58.
 * Covers all DieselDB SQL type → Avro type mappings, round-trip conversions,
 * schema building, .avsc file I/O, and compatibility validation.
 */
@Tag("storage")
class AvroSchemaTest {

    @TempDir
    Path tempDir;

    // ─── AvroTypeMapper: SQL → Avro scalar mappings ─────────────────

    @Test
    void stringTypeMapsToAvroString() {
        Schema s = AvroTypeMapper.toAvroSchema(String.class, "NAME");
        assertEquals(Schema.Type.STRING, s.getType());
    }

    @Test
    void integerTypeMapsToAvroInt() {
        Schema s = AvroTypeMapper.toAvroSchema(Integer.class, "AGE");
        assertEquals(Schema.Type.INT, s.getType());
    }

    @Test
    void longTypeMapsToAvroLong() {
        Schema s = AvroTypeMapper.toAvroSchema(Long.class, "ID");
        assertEquals(Schema.Type.LONG, s.getType());
    }

    @Test
    void floatTypeMapsToAvroFloat() {
        Schema s = AvroTypeMapper.toAvroSchema(Float.class, "TEMP");
        assertEquals(Schema.Type.FLOAT, s.getType());
    }

    @Test
    void doubleTypeMapsToAvroDouble() {
        Schema s = AvroTypeMapper.toAvroSchema(Double.class, "AMOUNT");
        assertEquals(Schema.Type.DOUBLE, s.getType());
    }

    @Test
    void booleanTypeMapsToAvroBoolean() {
        Schema s = AvroTypeMapper.toAvroSchema(Boolean.class, "ACTIVE");
        assertEquals(Schema.Type.BOOLEAN, s.getType());
    }

    @Test
    void bigDecimalMapsToAvroBytesDecimal() {
        Schema s = AvroTypeMapper.toAvroSchema(BigDecimal.class, "BALANCE");
        assertEquals(Schema.Type.BYTES, s.getType());
        assertNotNull(s.getLogicalType());
        assertEquals("decimal", s.getLogicalType().getName());
    }

    @Test
    void dateMapsToAvroIntDate() {
        Schema s = AvroTypeMapper.toAvroSchema(LocalDate.class, "BIRTHDATE");
        assertEquals(Schema.Type.INT, s.getType());
        assertNotNull(s.getLogicalType());
        assertEquals("date", s.getLogicalType().getName());
    }

    @Test
    void dateTimeMapsToAvroLongTimestamp() {
        Schema s = AvroTypeMapper.toAvroSchema(LocalDateTime.class, "LAST_LOGIN");
        assertEquals(Schema.Type.LONG, s.getType());
        assertNotNull(s.getLogicalType());
        assertEquals("timestamp-millis", s.getLogicalType().getName());
    }

    @Test
    void shortAndByteMapToAvroInt() {
        Schema shortSchema = AvroTypeMapper.toAvroSchema(Short.class, "RANK");
        assertEquals(Schema.Type.INT, shortSchema.getType());
        assertNull(shortSchema.getLogicalType()); // no logical type for SHORT

        Schema byteSchema = AvroTypeMapper.toAvroSchema(Byte.class, "FLAG");
        assertEquals(Schema.Type.INT, byteSchema.getType());
        assertNull(byteSchema.getLogicalType());
    }

    @Test
    void charMapsToAvroString() {
        Schema s = AvroTypeMapper.toAvroSchema(Character.class, "INITIAL");
        assertEquals(Schema.Type.STRING, s.getType());
    }

    @Test
    void uuidMapsToAvroStringUuid() {
        Schema s = AvroTypeMapper.toAvroSchema(UUID.class, "SESSION_ID");
        assertEquals(Schema.Type.STRING, s.getType());
        assertNotNull(s.getLogicalType());
        assertEquals("uuid", s.getLogicalType().getName());
    }

    @Test
    void unsupportedTypeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroTypeMapper.toAvroSchema(Object.class, "BAD"));
    }

    // ─── AvroTypeMapper: nullable ───────────────────────────────────

    @Test
    void nullableColumnCreatesUnion() {
        Schema s = AvroTypeMapper.nullableOf(String.class, "NAME");
        assertEquals(Schema.Type.UNION, s.getType());
        assertEquals(2, s.getTypes().size());
        assertEquals(Schema.Type.NULL, s.getTypes().get(0).getType());
        assertEquals(Schema.Type.STRING, s.getTypes().get(1).getType());
    }

    @Test
    void nullableOfSchemaWrapsExisting() {
        Schema base = Schema.create(Schema.Type.INT);
        Schema nullable = AvroTypeMapper.nullableOf(base);
        assertEquals(Schema.Type.UNION, nullable.getType());
        assertEquals(2, nullable.getTypes().size());
        assertEquals(Schema.Type.NULL, nullable.getTypes().get(0).getType());
        assertEquals(Schema.Type.INT, nullable.getTypes().get(1).getType());
    }

    @Test
    void isNullableDetectsUnionWithNull() {
        Schema nullable = AvroTypeMapper.nullableOf(String.class, "X");
        assertTrue(AvroTypeMapper.isNullable(nullable));

        Schema nonNullable = Schema.create(Schema.Type.STRING);
        assertFalse(AvroTypeMapper.isNullable(nonNullable));

        assertFalse(AvroTypeMapper.isNullable(null));
    }

    // ─── AvroTypeMapper: round-trip Java → Avro → Java ─────────────

    @Test
    void roundTripStringType() {
        Schema avro = AvroTypeMapper.toAvroSchema(String.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(String.class, back);
    }

    @Test
    void roundTripIntegerType() {
        Schema avro = AvroTypeMapper.toAvroSchema(Integer.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(Integer.class, back);
    }

    @Test
    void roundTripLongType() {
        Schema avro = AvroTypeMapper.toAvroSchema(Long.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(Long.class, back);
    }

    @Test
    void roundTripFloatType() {
        Schema avro = AvroTypeMapper.toAvroSchema(Float.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(Float.class, back);
    }

    @Test
    void roundTripDoubleType() {
        Schema avro = AvroTypeMapper.toAvroSchema(Double.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(Double.class, back);
    }

    @Test
    void roundTripBooleanType() {
        Schema avro = AvroTypeMapper.toAvroSchema(Boolean.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(Boolean.class, back);
    }

    @Test
    void roundTripBigDecimalType() {
        Schema avro = AvroTypeMapper.toAvroSchema(BigDecimal.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(BigDecimal.class, back);
    }

    @Test
    void roundTripDateType() {
        Schema avro = AvroTypeMapper.toAvroSchema(LocalDate.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(LocalDate.class, back);
    }

    @Test
    void roundTripDateTimeType() {
        Schema avro = AvroTypeMapper.toAvroSchema(LocalDateTime.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(LocalDateTime.class, back);
    }

    @Test
    void roundTripUuidType() {
        Schema avro = AvroTypeMapper.toAvroSchema(UUID.class, "X");
        Class<?> back = AvroTypeMapper.toJavaType(avro);
        assertEquals(UUID.class, back);
    }

    @Test
    void roundTripNullableType() {
        Schema nullable = AvroTypeMapper.nullableOf(Long.class, "ID");
        Class<?> back = AvroTypeMapper.toJavaType(nullable);
        assertEquals(Long.class, back);
    }

    // ─── AvroTypeMapper: type name helpers ──────────────────────────

    @Test
    void typeNameReturnsDieselDbName() {
        assertEquals("LONG", AvroTypeMapper.typeName(Long.class));
        assertEquals("INTEGER", AvroTypeMapper.typeName(Integer.class));
        assertEquals("STRING", AvroTypeMapper.typeName(String.class));
        assertEquals("BOOLEAN", AvroTypeMapper.typeName(Boolean.class));
        assertEquals("DATE", AvroTypeMapper.typeName(LocalDate.class));
        assertEquals("DATETIME", AvroTypeMapper.typeName(LocalDateTime.class));
        assertEquals("UUID", AvroTypeMapper.typeName(UUID.class));
        assertEquals("BIGDECIMAL", AvroTypeMapper.typeName(BigDecimal.class));
        assertEquals("SHORT", AvroTypeMapper.typeName(Short.class));
        assertEquals("BYTE", AvroTypeMapper.typeName(Byte.class));
        assertEquals("CHAR", AvroTypeMapper.typeName(Character.class));
        assertNull(AvroTypeMapper.typeName(null));
    }

    @Test
    void typeClassResolvesDieselDbName() {
        assertEquals(Long.class, AvroTypeMapper.typeClass("LONG"));
        assertEquals(Integer.class, AvroTypeMapper.typeClass("INTEGER"));
        assertEquals(String.class, AvroTypeMapper.typeClass("STRING"));
        assertEquals(Boolean.class, AvroTypeMapper.typeClass("BOOLEAN"));
        assertEquals(LocalDate.class, AvroTypeMapper.typeClass("DATE"));
        assertEquals(LocalDateTime.class, AvroTypeMapper.typeClass("DATETIME"));
        assertEquals(LocalDateTime.class, AvroTypeMapper.typeClass("DATETIME_MS"));
        assertEquals(UUID.class, AvroTypeMapper.typeClass("UUID"));
        assertEquals(BigDecimal.class, AvroTypeMapper.typeClass("BIGDECIMAL"));
        assertEquals(Short.class, AvroTypeMapper.typeClass("SHORT"));
        assertEquals(Byte.class, AvroTypeMapper.typeClass("BYTE"));
        assertEquals(Character.class, AvroTypeMapper.typeClass("CHAR"));
        assertNull(AvroTypeMapper.typeClass("NONEXISTENT"));
        assertNull(AvroTypeMapper.typeClass(null));
    }

    @Test
    void typeClassIsCaseInsensitive() {
        assertEquals(String.class, AvroTypeMapper.typeClass("string"));
        assertEquals(Integer.class, AvroTypeMapper.typeClass("Integer"));
        assertEquals(Long.class, AvroTypeMapper.typeClass("long"));
    }

    // ─── AvroSchemaManager: build table schema ──────────────────────

    @Test
    void buildTableSchemaCreatesRecord() {
        List<String> cols = List.of("ID", "NAME", "AGE");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("AGE", Integer.class);

        Schema schema = AvroSchemaManager.buildTableSchema("USERS", cols, types);

        assertEquals(Schema.Type.RECORD, schema.getType());
        assertEquals("USERS", schema.getName());
        assertEquals("diesel.avro", schema.getNamespace());
        assertEquals(3, schema.getFields().size());
    }

    @Test
    void buildTableSchemaFieldNamesMatch() {
        List<String> cols = List.of("ID", "NAME", "BALANCE");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("BALANCE", BigDecimal.class);

        Schema schema = AvroSchemaManager.buildTableSchema("ACCOUNTS", cols, types);

        assertEquals("ID", schema.getField("ID").name());
        assertEquals("NAME", schema.getField("NAME").name());
        assertEquals("BALANCE", schema.getField("BALANCE").name());
    }

    @Test
    void buildTableSchemaFieldTypesMatch() {
        List<String> cols = List.of("ID", "NAME", "ACTIVE", "SCORE");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("ACTIVE", Boolean.class);
        types.put("SCORE", Double.class);

        Schema schema = AvroSchemaManager.buildTableSchema("TEST", cols, types);

        assertEquals(Schema.Type.LONG, schema.getField("ID").schema().getType());
        assertEquals(Schema.Type.STRING, schema.getField("NAME").schema().getType());
        assertEquals(Schema.Type.BOOLEAN, schema.getField("ACTIVE").schema().getType());
        assertEquals(Schema.Type.DOUBLE, schema.getField("SCORE").schema().getType());
    }

    @Test
    void buildTableSchemaWithAllScalarTypes() {
        List<String> cols = List.of("C_STR", "C_INT", "C_LONG", "C_FLOAT", "C_DBL",
                "C_BOOL", "C_DEC", "C_DATE", "C_DT", "C_UUID", "C_SHORT", "C_BYTE", "C_CHAR");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("C_STR", String.class);
        types.put("C_INT", Integer.class);
        types.put("C_LONG", Long.class);
        types.put("C_FLOAT", Float.class);
        types.put("C_DBL", Double.class);
        types.put("C_BOOL", Boolean.class);
        types.put("C_DEC", BigDecimal.class);
        types.put("C_DATE", LocalDate.class);
        types.put("C_DT", LocalDateTime.class);
        types.put("C_UUID", UUID.class);
        types.put("C_SHORT", Short.class);
        types.put("C_BYTE", Byte.class);
        types.put("C_CHAR", Character.class);

        Schema schema = AvroSchemaManager.buildTableSchema("ALL_TYPES", cols, types);

        assertEquals(13, schema.getFields().size());
        assertEquals(Schema.Type.STRING, schema.getField("C_STR").schema().getType());
        assertEquals(Schema.Type.INT, schema.getField("C_INT").schema().getType());
        assertEquals(Schema.Type.LONG, schema.getField("C_LONG").schema().getType());
        assertEquals(Schema.Type.FLOAT, schema.getField("C_FLOAT").schema().getType());
        assertEquals(Schema.Type.DOUBLE, schema.getField("C_DBL").schema().getType());
        assertEquals(Schema.Type.BOOLEAN, schema.getField("C_BOOL").schema().getType());
        assertEquals(Schema.Type.BYTES, schema.getField("C_DEC").schema().getType());
        assertEquals(Schema.Type.INT, schema.getField("C_DATE").schema().getType());
        assertEquals(Schema.Type.LONG, schema.getField("C_DT").schema().getType());
        assertEquals(Schema.Type.STRING, schema.getField("C_UUID").schema().getType());
        assertEquals(Schema.Type.INT, schema.getField("C_SHORT").schema().getType());
        assertEquals(Schema.Type.INT, schema.getField("C_BYTE").schema().getType());
        assertEquals(Schema.Type.STRING, schema.getField("C_CHAR").schema().getType());
    }

    @Test
    void buildTableSchemaBlankNameThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroSchemaManager.buildTableSchema("",
                        List.of("X"), Map.of("X", String.class)));
    }

    @Test
    void buildTableSchemaEmptyColumnsThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroSchemaManager.buildTableSchema("T",
                        List.of(), Map.of()));
    }

    @Test
    void buildTableSchemaFromTypeNamesWorks() {
        List<String> cols = List.of("ID", "NAME");
        Map<String, String> typeNames = Map.of("ID", "LONG", "NAME", "STRING");

        Schema schema = AvroSchemaManager.buildTableSchemaFromTypeNames("USERS", cols, typeNames);

        assertEquals(Schema.Type.RECORD, schema.getType());
        assertEquals(2, schema.getFields().size());
        assertEquals(Schema.Type.LONG, schema.getField("ID").schema().getType());
        assertEquals(Schema.Type.STRING, schema.getField("NAME").schema().getType());
    }

    @Test
    void buildTableSchemaUnknownTypeNameThrows() {
        Map<String, String> typeNames = Map.of("ID", "NONEXISTENT");
        assertThrows(IllegalArgumentException.class,
                () -> AvroSchemaManager.buildTableSchemaFromTypeNames("T",
                        List.of("ID"), typeNames));
    }

    // ─── AvroSchemaManager: .avsc file write/read ───────────────────

    @Test
    void writeAndReadSchemaFile() throws IOException {
        List<String> cols = List.of("ID", "NAME", "ACTIVE");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("ACTIVE", Boolean.class);

        Schema original = AvroSchemaManager.buildTableSchema("USERS", cols, types);
        Path avscPath = tempDir.resolve("USERS.avsc");

        AvroSchemaManager.writeSchemaFile(original, avscPath);
        assertTrue(Files.exists(avscPath));

        Schema parsed = AvroSchemaManager.readSchemaFile(avscPath);

        assertEquals(original.getName(), parsed.getName());
        assertEquals(original.getFields().size(), parsed.getFields().size());
        for (Schema.Field f : original.getFields()) {
            assertEquals(f.schema().getType(), parsed.getField(f.name()).schema().getType());
        }
    }

    @Test
    void writeSchemaFileCreatesParentDirs() throws IOException {
        Path nested = tempDir.resolve("sub").resolve("dir").resolve("T.avsc");
        Schema schema = AvroTypeMapper.nullableOf(String.class, "X");
        // wrap in a record since writeSchemaFile takes any Schema
        Schema record = Schema.createRecord("T", null, "test", false);
        record.setFields(List.of(new Schema.Field("col", schema)));

        AvroSchemaManager.writeSchemaFile(record, nested);
        assertTrue(Files.exists(nested));
    }

    @Test
    void readSchemaFileNonexistentThrows() {
        Path fake = tempDir.resolve("nope.avsc");
        assertThrows(IOException.class,
                () -> AvroSchemaManager.readSchemaFile(fake));
    }

    @Test
    void resolveSchemaPathUsesConfigOrDefault() {
        Path p = AvroSchemaManager.resolveSchemaPath("MY_TABLE");
        assertTrue(p.toString().endsWith("MY_TABLE.avsc"));
        // Should contain the sanitized name
        assertTrue(p.getFileName().toString().equals("MY_TABLE.avsc"));
    }

    // ─── AvroSchemaManager: compatibility validation ────────────────

    @Test
    void validateCompatibilityIdenticalSchemaReturnsEmpty() {
        List<String> cols = List.of("ID", "NAME");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("NAME", String.class);

        Schema schema = AvroSchemaManager.buildTableSchema("T", cols, types);
        List<String> issues = AvroSchemaManager.validateCompatibility(schema, cols, types);

        assertTrue(issues.isEmpty(), "Identical schemas should be compatible, got: " + issues);
    }

    @Test
    void validateCompatibilityDetectsMissingColumn() {
        List<String> cols = List.of("ID");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);

        Schema schema = AvroSchemaManager.buildTableSchema("T", cols, types);

        // Now add a column that the schema doesn't have
        List<String> newCols = List.of("ID", "NAME");
        Map<String, Class<?>> newTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        newTypes.put("ID", Long.class);
        newTypes.put("NAME", String.class);

        List<String> issues = AvroSchemaManager.validateCompatibility(schema, newCols, newTypes);

        assertFalse(issues.isEmpty());
        assertTrue(issues.stream().anyMatch(i -> i.contains("NAME")),
                "Should report NAME as missing: " + issues);
    }

    @Test
    void validateCompatibilityDetectsTypeMismatch() {
        List<String> cols = List.of("ID", "AMOUNT");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);
        types.put("AMOUNT", Double.class);

        Schema schema = AvroSchemaManager.buildTableSchema("T", cols, types);

        // Change AMOUNT type to FLOAT
        Map<String, Class<?>> changedTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        changedTypes.put("ID", Long.class);
        changedTypes.put("AMOUNT", Float.class);

        List<String> issues = AvroSchemaManager.validateCompatibility(schema, cols, changedTypes);

        assertFalse(issues.isEmpty());
        assertTrue(issues.stream().anyMatch(i -> i.contains("AMOUNT") && i.contains("mismatch")),
                "Should report AMOUNT type mismatch: " + issues);
    }

    @Test
    void validateCompatibilityDetectsExtraAvroField() {
        List<String> cols = List.of("ID");
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.put("ID", Long.class);

        Schema schema = AvroSchemaManager.buildTableSchema("T", cols, types);

        // DieselDB has fewer columns than Avro schema
        List<String> fewerCols = List.of("ID");
        Map<String, Class<?>> fewerTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        fewerTypes.put("ID", Long.class);

        // Actually, same — let's do a schema with more fields than the DieselDB list
        List<String> moreCols = List.of("ID", "EXTRA_AVRO_ONLY");
        Map<String, Class<?>> moreTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        moreTypes.put("ID", Long.class);
        moreTypes.put("EXTRA_AVRO_ONLY", String.class);
        Schema widerSchema = AvroSchemaManager.buildTableSchema("T", moreCols, moreTypes);

        // Validate with fewer DieselDB columns
        List<String> issues = AvroSchemaManager.validateCompatibility(widerSchema, fewerCols, fewerTypes);

        assertFalse(issues.isEmpty());
        assertTrue(issues.stream().anyMatch(i -> i.contains("EXTRA_AVRO_ONLY")),
                "Should report EXTRA_AVRO_ONLY as extra: " + issues);
    }

    @Test
    void validateCompatibilityNullSchemaReturnsIssue() {
        List<String> issues = AvroSchemaManager.validateCompatibility(null,
                List.of("X"), Map.of("X", String.class));
        assertFalse(issues.isEmpty());
    }

    // ─── AvroSchemaManager: sanitize name ───────────────────────────

    @Test
    void sanitizeNameHandlesSpecialChars() {
        assertEquals("MY_TABLE", AvroSchemaManager.sanitizeName("MY.TABLE"));
        assertEquals("MY_TABLE", AvroSchemaManager.sanitizeName("MY-TABLE"));
        assertEquals("_123", AvroSchemaManager.sanitizeName("123"));
        assertEquals("unknown", AvroSchemaManager.sanitizeName(null));
        assertEquals("unknown", AvroSchemaManager.sanitizeName(""));
    }

    // ─── Complex type support ───────────────────────────────────────

    @Test
    void createArraySchema() {
        Schema elem = Schema.create(Schema.Type.STRING);
        Schema array = AvroTypeMapper.createArray(elem);
        assertEquals(Schema.Type.ARRAY, array.getType());
        assertEquals(Schema.Type.STRING, array.getElementType().getType());
    }

    @Test
    void createMapSchema() {
        Schema val = Schema.create(Schema.Type.LONG);
        Schema map = AvroTypeMapper.createMap(val);
        assertEquals(Schema.Type.MAP, map.getType());
        assertEquals(Schema.Type.LONG, map.getValueType().getType());
    }

    @Test
    void createEnumSchema() {
        Schema enumSchema = AvroTypeMapper.createEnum("STATUS",
                List.of("ACTIVE", "INACTIVE", "PENDING"), "diesel.avro");
        assertEquals(Schema.Type.ENUM, enumSchema.getType());
        assertEquals("STATUS", enumSchema.getName());
        assertEquals(3, enumSchema.getEnumSymbols().size());
        assertTrue(enumSchema.getEnumSymbols().contains("ACTIVE"));
    }

    @Test
    void buildRecordSchema() {
        List<Schema.Field> fields = List.of(
                new Schema.Field("X", Schema.create(Schema.Type.STRING)),
                new Schema.Field("Y", Schema.create(Schema.Type.INT))
        );
        Schema record = AvroTypeMapper.buildRecord("MyRec", fields, "diesel.avro", false);
        assertEquals(Schema.Type.RECORD, record.getType());
        assertEquals("MyRec", record.getName());
        assertEquals(2, record.getFields().size());
    }
}
