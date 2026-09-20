package diesel;

import ch.qos.logback.classic.Level;
import diesel.storage.avro.AvroDataValidator;
import diesel.storage.avro.AvroDataValidator.AvroValidationException;
import diesel.storage.avro.AvroDataValidator.DatasetValidationResult;
import diesel.storage.avro.AvroDataValidator.ErrorType;
import diesel.storage.avro.AvroDataValidator.FieldError;
import diesel.storage.avro.AvroDataValidator.ValidationMode;
import diesel.storage.avro.AvroDataValidator.ValidationResult;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AvroDataValidator} — Prompt 73.
 * Covers row validation against an Avro schema, strict vs permissive modes,
 * invalid-record logging and validation statistics.
 */
@Tag("storage")
@StorageType("avro")
class AvroDataValidatorTest {

    private static final String NS = "diesel.avro";

    // ─── Schema helpers ─────────────────────────────────────────────

    private static Schema record(String name, Schema.Field... fields) {
        Schema r = Schema.createRecord(name, null, NS, false);
        r.setFields(Arrays.asList(fields));
        return r;
    }

    private static Schema.Field f(String name, Schema type) {
        return new Schema.Field(name, type, null, (Object) null);
    }

    private static Schema str() {
        return Schema.create(Schema.Type.STRING);
    }

    private static Schema intT() {
        return Schema.create(Schema.Type.INT);
    }

    private static Schema boolT() {
        return Schema.create(Schema.Type.BOOLEAN);
    }

    private static Schema enumT(String... symbols) {
        return Schema.createEnum("status", null, NS, Arrays.asList(symbols));
    }

    private static Schema decimal() {
        return LogicalTypes.decimal(38, 18)
                .addToSchema(Schema.create(Schema.Type.BYTES));
    }

    private static Schema date() {
        return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    }

    private static Schema timestamp() {
        return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }

    private static Schema uuid() {
        return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    }

    private static Schema person() {
        return record("person", f("id", str()), f("name", str()), f("age", intT()));
    }

    private static FieldError findError(ValidationResult result, String fieldName) {
        return result.errors().stream()
                .filter(e -> e.fieldName().equals(fieldName))
                .findFirst()
                .orElse(null);
    }

    // ─── Valid rows ─────────────────────────────────────────────────

    @Test
    void validObjectArrayRowPasses() {
        ValidationResult result = AvroDataValidator.validateRow(
                new Object[]{"u1", "Anna", 30}, person());
        assertTrue(result.isValid());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    void validMapRowMatchesCaseInsensitively() {
        Map<String, Object> row = new HashMap<>();
        row.put("ID", "u1");
        row.put("NAME", "Anna");
        row.put("Age", 30);
        ValidationResult result = AvroDataValidator.validateRow(row, person());
        assertTrue(result.isValid());
    }

    @Test
    void validGenericRecordPasses() {
        GenericRecord record = new GenericData.Record(person());
        record.put("id", "u1");
        record.put("name", "Anna");
        record.put("age", 30);
        assertTrue(AvroDataValidator.validateRow(record, person()).isValid());
    }

    @Test
    void nullableUnionAcceptsNull() {
        Schema s = record("t", f("id", AvroTypeMapper.nullableOf(str())));
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{null}, s);
        assertTrue(result.isValid());
        assertEquals(0, result.errors().size());
    }

    @Test
    void bareNullTypeAcceptsNull() {
        Schema s = record("t", f("id", Schema.create(Schema.Type.NULL)));
        assertTrue(AvroDataValidator.validateRow(new Object[]{null}, s).isValid());
    }

    // ─── Null handling ──────────────────────────────────────────────

    @Test
    void nullOnNonNullableColumnFails() {
        Schema s = person();
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{"u1", null, 30}, s);
        assertFalse(result.isValid());
        FieldError error = findError(result, "name");
        assertNotNull(error);
        assertEquals(ErrorType.NULL_NOT_ALLOWED, error.type());
    }

    @Test
    void nullOnNullTypeWithValueFails() {
        Schema s = record("t", f("id", Schema.create(Schema.Type.NULL)));
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{"x"}, s);
        assertFalse(result.isValid());
        assertEquals(ErrorType.TYPE_MISMATCH, result.errors().get(0).type());
    }

    // ─── Type mismatches ────────────────────────────────────────────

    @Test
    void stringValueInIntColumnFails() {
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{"u1", "Anna", "thirty"}, person());
        assertFalse(result.isValid());
        FieldError error = findError(result, "age");
        assertNotNull(error);
        assertEquals(ErrorType.TYPE_MISMATCH, error.type());
    }

    @Test
    void integerValueInStringColumnFails() {
        Schema s = record("t", f("name", str()));
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{42}, s);
        assertFalse(result.isValid());
        assertEquals(ErrorType.TYPE_MISMATCH, result.errors().get(0).type());
    }

    @Test
    void booleanColumnRejectsNonBoolean() {
        Schema s = record("t", f("active", boolT()));
        assertFalse(AvroDataValidator.validateRow(new Object[]{42}, s).isValid());
        assertFalse(AvroDataValidator.validateRow(new Object[]{"true"}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{Boolean.TRUE}, s).isValid());
    }

    @Test
    void intRangeRespectedForWiderNumericTypes() {
        Schema s = record("t", f("n", intT()));
        assertTrue(AvroDataValidator.validateRow(new Object[]{5L}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{(short) 3}, s).isValid());
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{Integer.MAX_VALUE + 1L}, s);
        assertFalse(result.isValid());
        FieldError error = result.errors().get(0);
        assertEquals(ErrorType.VALUE_OUT_OF_RANGE, error.type());
    }

    @Test
    void longColumnAcceptsAnyIntegralNumber() {
        Schema s = record("t", f("n", Schema.create(Schema.Type.LONG)));
        assertTrue(AvroDataValidator.validateRow(new Object[]{5}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{5L}, s).isValid());
        assertFalse(AvroDataValidator.validateRow(new Object[]{"5"}, s).isValid());
    }

    // ─── ENUM ───────────────────────────────────────────────────────

    @Test
    void enumAcceptsDeclaredSymbols() {
        Schema s = record("t", f("status", enumT("ACTIVE", "INACTIVE")));
        assertTrue(AvroDataValidator.validateRow(new Object[]{"ACTIVE"}, s).isValid());
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{"UNKNOWN"}, s);
        assertFalse(result.isValid());
        FieldError error = result.errors().get(0);
        assertEquals(ErrorType.NOT_IN_ENUM, error.type());
    }

    @Test
    void enumRejectsNonString() {
        Schema s = record("t", f("status", enumT("ACTIVE")));
        assertFalse(AvroDataValidator.validateRow(new Object[]{1}, s).isValid());
        assertEquals(ErrorType.TYPE_MISMATCH, AvroDataValidator.validateRow(new Object[]{1}, s).errors().get(0).type());
    }

    // ─── Logical types ──────────────────────────────────────────────

    @Test
    void decimalAcceptsBigDecimalOnly() {
        Schema s = record("t", f("amount", decimal()));
        assertTrue(AvroDataValidator.validateRow(new Object[]{new BigDecimal("10.50")}, s).isValid());
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{10}, s);
        assertFalse(result.isValid());
        assertEquals(ErrorType.TYPE_MISMATCH, result.errors().get(0).type());
    }

    @Test
    void dateAcceptsLocalDateAndEpochDay() {
        Schema s = record("t", f("d", date()));
        assertTrue(AvroDataValidator.validateRow(new Object[]{LocalDate.of(2024, 1, 1)}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{19723}, s).isValid());
        assertFalse(AvroDataValidator.validateRow(new Object[]{"2024-01-01"}, s).isValid());
    }

    @Test
    void timestampAcceptsLocalDateTimeAndMillis() {
        Schema s = record("t", f("ts", timestamp()));
        assertTrue(AvroDataValidator.validateRow(new Object[]{LocalDateTime.of(2024, 1, 1, 0, 0)}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{1704067200000L}, s).isValid());
        assertFalse(AvroDataValidator.validateRow(new Object[]{"now"}, s).isValid());
    }

    @Test
    void uuidAcceptsUuidAndParseableString() {
        String hex = UUID.randomUUID().toString();
        Schema s = record("t", f("u", uuid()));
        assertTrue(AvroDataValidator.validateRow(new Object[]{UUID.fromString(hex)}, s).isValid());
        assertTrue(AvroDataValidator.validateRow(new Object[]{hex}, s).isValid());
        assertFalse(AvroDataValidator.validateRow(new Object[]{"not-a-uuid"}, s).isValid());
    }

    @Test
    void byteValueNotAllowedInDecimalField() {
        Schema s = record("t", f("amount", decimal()));
        assertFalse(AvroDataValidator.validateRow(new Object[]{new byte[]{1, 2}}, s).isValid());
    }

    // ─── Extra / missing columns ────────────────────────────────────

    @Test
    void extraObjectArrayValuesReported() {
        ValidationResult result = AvroDataValidator.validateRow(
                new Object[]{"u1", "Anna", 30, "EXTRA"}, person());
        assertFalse(result.isValid());
        FieldError error = findError(result, "row[3]");
        assertNotNull(error);
        assertEquals(ErrorType.EXTRA_FIELD, error.type());
    }

    @Test
    void extraMapKeyReported() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", "u1");
        row.put("name", "Anna");
        row.put("age", 30);
        row.put("bogus", 1);
        ValidationResult result = AvroDataValidator.validateRow(row, person());
        assertFalse(result.isValid());
        FieldError error = findError(result, "bogus");
        assertNotNull(error);
        assertEquals(ErrorType.EXTRA_FIELD, error.type());
    }

    @Test
    void extraMapKeyMatchingFieldCaseInsensitivelyIsNotExtra() {
        Map<String, Object> row = new HashMap<>();
        row.put("ID", "u1");
        row.put("NAME", "Anna");
        row.put("AGE", 30);
        assertTrue(AvroDataValidator.validateRow(row, person()).isValid());
    }

    @Test
    void missingMapKeyTreatedAsNull() {
        Schema s = record("t", f("id", AvroTypeMapper.nullableOf(str())));
        Map<String, Object> row = new HashMap<>();
        assertTrue(AvroDataValidator.validateRow(row, s).isValid());
        Schema strict = record("t", f("id", str()));
        assertFalse(AvroDataValidator.validateRow(row, strict).isValid());
    }

    @Test
    void shortArrayMissingTrailingTreatedAsNull() {
        Schema s = record("t", f("id", AvroTypeMapper.nullableOf(str())), f("v", intT()));
        ValidationResult result = AvroDataValidator.validateRow(new Object[]{"u1"}, s);
        assertFalse(result.isValid());
        assertEquals(ErrorType.NULL_NOT_ALLOWED, findError(result, "v").type());
    }

    // ─── Dataset / statistics (permissive) ──────────────────────────

    @Test
    void permissiveDatasetAllValid() {
        Schema s = person();
        List<Object[]> rows = List.of(
                new Object[]{"u1", "Anna", 30},
                new Object[]{"u2", "Bob", 40});
        DatasetValidationResult report =
                AvroDataValidator.validateDataset(rows, s, ValidationMode.PERMISSIVE, false);
        assertTrue(report.isAllValid());
        assertEquals(2, report.totalRows());
        assertEquals(2, report.validRows());
        assertEquals(0, report.invalidRows());
        assertTrue(report.errorCounts().isEmpty());
        assertTrue(report.failedRowIndexes().isEmpty());
    }

    @Test
    void permissiveDatasetAggregatesStatistics() {
        Schema s = record("t", f("id", str()), f("age", intT()));
        List<Object[]> rows = List.of(
                new Object[]{"u1", 30},
                new Object[]{"u2", null},
                new Object[]{"u3", "bad"},
                new Object[]{"u4", 45});
        DatasetValidationResult report =
                AvroDataValidator.validateDataset(rows, s, ValidationMode.PERMISSIVE, false);
        assertFalse(report.isAllValid());
        assertEquals(4, report.totalRows());
        assertEquals(2, report.validRows());
        assertEquals(2, report.invalidRows());
        assertEquals(List.of(1, 2), report.failedRowIndexes());
        assertEquals(1L, report.errorCounts().get(ErrorType.NULL_NOT_ALLOWED));
        assertEquals(1L, report.errorCounts().get(ErrorType.TYPE_MISMATCH));
        assertEquals(2L, report.perFieldErrorCounts().get("age"));
        assertNotNull(report.summary());
        assertTrue(report.summary().contains("2 valid, 2 invalid"));
    }

    @Test
    void datasetSupportsMixedRowForms() {
        Schema s = record("t", f("id", str()));
        Map<String, Object> mapRow = Map.of("id", "u1");
        GenericRecord rec = new GenericData.Record(s);
        rec.put("id", "u2");
        DatasetValidationResult report = AvroDataValidator.validateDataset(
                List.of(new Object[]{"u0"}, mapRow, rec), s, ValidationMode.PERMISSIVE, false);
        assertEquals(3, report.validRows());
        assertEquals(0, report.invalidRows());
    }

    @Test
    void validateDatasetNullRowsReturnsNull() {
        assertEquals(null, AvroDataValidator.validateDataset(null, person()));
    }

    // ─── Strict mode ────────────────────────────────────────────────

    @Test
    void strictModeThrowsOnFirstInvalidRow() {
        Schema s = person();
        List<Object[]> rows = List.of(
                new Object[]{"u1", "Anna", 30},
                new Object[]{"u2", null, 40});
        AvroValidationException ex = assertThrows(AvroValidationException.class,
                () -> AvroDataValidator.validateDataset(rows, s, ValidationMode.STRICT, false));
        assertEquals(1, ex.rowIndex());
        assertEquals(ValidationMode.STRICT, ex.mode());
        assertFalse(ex.fieldErrors().isEmpty());
        assertEquals(ErrorType.NULL_NOT_ALLOWED, ex.fieldErrors().get(0).type());
        assertTrue(ex.getMessage().contains("row 1"));
    }

    @Test
    void strictModePassesOnAllValid() {
        Schema s = person();
        List<Object[]> rows = List.of(
                new Object[]{"u1", "Anna", 30},
                new Object[]{"u2", "Bob", 40});
        DatasetValidationResult report = AvroDataValidator.validateDataset(rows, s, ValidationMode.STRICT, false);
        assertTrue(report.isAllValid());
        assertEquals(2, report.validRows());
    }

    @Test
    void requireValidThrowsOnInvalidReport() {
        Schema s = record("t", f("id", str()));
        DatasetValidationResult report = AvroDataValidator.validateDataset(
                List.<Object[]>of(new Object[]{1}), s, ValidationMode.PERMISSIVE, false);
        assertThrows(AvroValidationException.class, () -> AvroDataValidator.requireValid(report));
        DatasetValidationResult clean = AvroDataValidator.validateDataset(
                List.<Object[]>of(new Object[]{"u1"}), s, ValidationMode.PERMISSIVE, false);
        assertEquals(clean, AvroDataValidator.requireValid(clean));
    }

    // ─── Logging ────────────────────────────────────────────────────

    @Test
    void invalidRowsLoggedAtWarnWhenEnabled() {
        Schema s = record("t", f("id", str()));
        try (Slf4jLogCapture capture = new Slf4jLogCapture(AvroDataValidator.class)) {
            AvroDataValidator.validateDataset(
                    List.of(new Object[]{"ok"}, new Object[]{1}), s, ValidationMode.PERMISSIVE, true);
            assertEquals(1, capture.eventsMatching(Level.WARN, "AVRO validation: invalid row").size());
            assertEquals(1, capture.eventsMatching(Level.WARN, "validated 2 rows").size());
        }
    }

    @Test
    void invalidRowsNotLoggedWhenDisabled() {
        Schema s = record("t", f("id", str()));
        try (Slf4jLogCapture capture = new Slf4jLogCapture(AvroDataValidator.class)) {
            AvroDataValidator.validateDataset(
                    List.of(new Object[]{"ok"}, new Object[]{1}), s, ValidationMode.PERMISSIVE, false);
            assertTrue(capture.eventsMatching(Level.WARN, "AVRO validation: invalid row").isEmpty());
        }
    }

    // ─── Guards ─────────────────────────────────────────────────────

    @Test
    void nullSchemaRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroDataValidator.validateRow(new Object[]{"x"}, null));
    }

    @Test
    void nonRecordSchemaRejected() {
        Schema plain = Schema.create(Schema.Type.STRING);
        assertThrows(IllegalArgumentException.class,
                () -> AvroDataValidator.validateRow(new Object[]{"x"}, plain));
        assertThrows(IllegalArgumentException.class,
                () -> AvroDataValidator.validateDataset(List.<Object[]>of(new Object[]{"x"}), plain));
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void defaultModeIsPermissive() {
        assertEquals(ValidationMode.PERMISSIVE, AvroDataValidator.resolveMode());
        assertTrue(AvroDataValidator.resolveLogInvalid());
    }

    @Test
    void modeResolutionSystemPropertyOverride() {
        try {
            System.setProperty(AvroDataValidator.MODE_KEY, "strict");
            assertEquals(ValidationMode.STRICT, AvroDataValidator.resolveMode());
            System.setProperty(AvroDataValidator.LOG_KEY, "off");
            assertFalse(AvroDataValidator.resolveLogInvalid());
        } finally {
            System.clearProperty(AvroDataValidator.MODE_KEY);
            System.clearProperty(AvroDataValidator.LOG_KEY);
        }
    }

    @Test
    void invalidModeValueFallsBackToPermissive() {
        try {
            System.setProperty(AvroDataValidator.MODE_KEY, "bogus");
            assertEquals(ValidationMode.PERMISSIVE, AvroDataValidator.resolveMode());
        } finally {
            System.clearProperty(AvroDataValidator.MODE_KEY);
        }
    }

    @Test
    void datasetUsesResolvedModeFromSysprop() {
        try {
            System.setProperty(AvroDataValidator.MODE_KEY, "strict");
            Schema s = record("t", f("id", str()));
            assertThrows(AvroValidationException.class,
                    () -> AvroDataValidator.validateDataset(List.<Object[]>of(new Object[]{1}), s));
        } finally {
            System.clearProperty(AvroDataValidator.MODE_KEY);
        }
    }
}