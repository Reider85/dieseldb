package diesel;

import diesel.storage.avro.AvroTypeMapper;
import diesel.storage.avro.SchemaCompatibilityChecker;
import diesel.storage.avro.SchemaConflictResolver;
import diesel.storage.avro.SchemaConflictResolver.ConflictType;
import diesel.storage.avro.SchemaConflictResolver.FieldConflict;
import diesel.storage.avro.SchemaConflictResolver.ResolutionConfig;
import diesel.storage.avro.SchemaConflictResolver.ResolutionResult;
import diesel.storage.avro.SchemaConflictResolver.ResolutionStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SchemaConflictResolver} — Prompt 72.
 * Covers evolution conflict resolution rules, default values for added fields,
 * removal handling, and alias-based field renaming.
 */
@Tag("storage")
@StorageType("avro")
class SchemaConflictResolverTest {

    // ─── Schema helpers ─────────────────────────────────────────────

    private static final String NS = "diesel.avro";

    private static Schema record(String name, Schema.Field... fields) {
        Schema r = Schema.createRecord(name, null, NS, false);
        r.setFields(Arrays.asList(fields));
        return r;
    }

    private static Schema.Field f(String name, Schema type) {
        return new Schema.Field(name, type, null, (Object) null);
    }

    private static Schema.Field fDef(String name, Schema type, Object defaultValue) {
        return new Schema.Field(name, type, null, defaultValue);
    }

    private static Schema.Field fAlias(String name, Schema type, String... aliases) {
        Schema.Field field = new Schema.Field(name, type, null, (Object) null);
        for (String alias : aliases) {
            field.addAlias(alias);
        }
        return field;
    }

    private static Schema.Field withAliases(Schema.Field field, String... aliases) {
        for (String alias : aliases) {
            field.addAlias(alias);
        }
        return field;
    }

    private static Schema str() {
        return Schema.create(Schema.Type.STRING);
    }

    private static Schema nStr() {
        return AvroTypeMapper.nullableOf(str());
    }

    private static Schema intT() {
        return Schema.create(Schema.Type.INT);
    }

    private static Schema nInt() {
        return AvroTypeMapper.nullableOf(intT());
    }

    private static Schema longT() {
        return Schema.create(Schema.Type.LONG);
    }

    private static Schema nLong() {
        return AvroTypeMapper.nullableOf(longT());
    }

    private static Schema doubleT() {
        return Schema.create(Schema.Type.DOUBLE);
    }

    private static Schema v1() {
        return record("t1", f("id", str()), f("name", str()));
    }

    // ─── Conflict classification ────────────────────────────────────

    @Test
    void identicalSchemasResolveCleanly() {
        Schema w = v1();
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(w, v1(), new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals(2, result.conflicts().size());
        assertTrue(result.conflicts().stream().allMatch(c -> c.strategy() == ResolutionStrategy.PROMOTE));
        assertTrue(result.conflicts().stream().allMatch(c -> c.type() == ConflictType.COMPATIBLE));
        assertTrue(result.warnings().isEmpty());
    }

    @Test
    void caseInsensitiveNameMatchIsCompatible() {
        Schema writer = record("t1", f("ID", str()));
        Schema reader = record("t1", f("id", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals(ConflictType.COMPATIBLE, result.conflicts().get(0).type());
    }

    @Test
    void fieldAddedWithExplicitDefaultUsesIt() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), f("region", str()));
        ResolutionConfig cfg = new ResolutionConfig().withFieldDefault("region", "NA");
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, cfg);
        assertTrue(result.isResolved());
        FieldConflict conflict = findByField(result, "region");
        assertNotNull(conflict);
        assertEquals(ConflictType.FIELD_ADDED, conflict.type());
        assertEquals(ResolutionStrategy.USE_DEFAULT, conflict.strategy());
        assertEquals("NA", conflict.defaultValue());
        assertEquals("NA", result.defaultValues().get("region"));
    }

    @Test
    void fieldAddedNullableGetsImplicitNull() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), f("nickname", nStr()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        FieldConflict conflict = findByField(result, "nickname");
        assertEquals(ResolutionStrategy.USE_DEFAULT, conflict.strategy());
        assertEquals(null, result.defaultValues().get("nickname"));
    }

    @Test
    void fieldAddedWithSchemaDefaultUsesIt() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals("NA", result.defaultValues().get("region"));
    }

    @Test
    void addedFieldWithoutDefaultSkipsLenient() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), f("req", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        FieldConflict conflict = findByField(result, "req");
        assertEquals(ConflictType.FIELD_ADDED, conflict.type());
        assertEquals(ResolutionStrategy.SKIP, conflict.strategy());
        assertTrue(result.warnings().stream().anyMatch(w -> w.contains("req")));
    }

    @Test
    void addedFieldWithoutDefaultFailsStrict() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), f("req", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(
                writer, reader, new ResolutionConfig().strict(true));
        assertFalse(result.isResolved());
        assertEquals(ResolutionStrategy.FAIL, findByField(result, "req").strategy());
    }

    @Test
    void useDefaultsOffSkipsAddedField() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(
                writer, reader, new ResolutionConfig().useDefaultValues(false));
        assertTrue(result.isResolved());
        assertEquals(ResolutionStrategy.SKIP, findByField(result, "region").strategy());
        assertTrue(result.defaultValues().isEmpty());
    }

    // ─── Removed fields ─────────────────────────────────────────────

    @Test
    void removedFieldIgnoredByDefault() {
        Schema writer = record("t1", f("id", str()), f("name", str()));
        Schema reader = record("t1", f("id", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertTrue(result.conflicts().stream().noneMatch(c -> c.type() == ConflictType.FIELD_REMOVED));
    }

    @Test
    void removedFieldReportedWhenNotIgnored() {
        Schema writer = record("t1", f("id", str()), f("name", str()));
        Schema reader = record("t1", f("id", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(
                writer, reader, new ResolutionConfig().ignoreRemovedFields(false));
        assertTrue(result.isResolved());
        FieldConflict conflict = findByType(result, ConflictType.FIELD_REMOVED);
        assertEquals("name", conflict.fieldName());
        assertEquals(ResolutionStrategy.SKIP, conflict.strategy());
    }

    // ─── Type evolution ─────────────────────────────────────────────

    @Test
    void promotedTypeResolvesAsPromote() {
        Schema writer = record("t1", f("id", intT()));
        Schema reader = record("t1", f("id", longT()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals(ConflictType.TYPE_PROMOTED, result.conflicts().get(0).type());
        assertEquals(ResolutionStrategy.PROMOTE, result.conflicts().get(0).strategy());
    }

    @Test
    void promotedNullableUnionWidens() {
        Schema writer = record("t1", f("id", nInt()));
        Schema reader = record("t1", f("id", nLong()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals(ConflictType.TYPE_PROMOTED, result.conflicts().get(0).type());
    }

    @Test
    void typeMismatchFails() {
        Schema writer = record("t1", f("id", str()));
        Schema reader = record("t1", f("id", intT()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertFalse(result.isResolved());
        FieldConflict conflict = result.conflicts().get(0);
        assertEquals(ConflictType.TYPE_MISMATCH, conflict.type());
        assertEquals(ResolutionStrategy.FAIL, conflict.strategy());
    }

    // ─── Renaming ───────────────────────────────────────────────────

    @Test
    void renamedViaConfigMapping() {
        Schema writer = record("t1", f("id", str()), f("email", str()));
        Schema reader = record("t1", f("id", str()), f("contact", str()));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(
                writer, reader, new ResolutionConfig().withAlias("email", "contact"));
        assertTrue(result.isResolved());
        FieldConflict conflict = findByField(result, "contact");
        assertNotNull(conflict);
        assertEquals(ConflictType.FIELD_RENAMED, conflict.type());
        assertEquals(ResolutionStrategy.RENAME, conflict.strategy());
    }

    @Test
    void renamedViaAvroAlias() {
        Schema writer = record("t1", f("id", str()), f("email", str()));
        Schema reader = record("t1", f("id", str()), fAlias("contact", str(), "email"));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertEquals(ConflictType.FIELD_RENAMED, findByField(result, "contact").type());
    }

    @Test
    void renameNotDetectedWhenAliasesDisabled() {
        Schema writer = record("t1", f("id", str()), f("email", str()));
        Schema reader = record("t1", f("id", str()), fAlias("contact", str(), "email"));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(
                writer, reader, new ResolutionConfig().allowAliases(false));
        FieldConflict contact = findByField(result, "contact");
        assertNotNull(contact);
        assertEquals(ConflictType.FIELD_ADDED, contact.type());
        assertEquals(ResolutionStrategy.SKIP, contact.strategy());
        assertTrue(result.conflicts().stream()
                .noneMatch(c -> c.type() == ConflictType.FIELD_RENAMED));
    }

    // ─── Mixed scenario ─────────────────────────────────────────────

    @Test
    void mixedConflictsApplyPerFieldStrategies() {
        Schema writer = record("t1",
                f("a", str()),
                f("b", intT()),
                f("gone", str()),
                f("email", str()));
        Schema reader = record("t1",
                f("a", str()),
                f("b", longT()),
                fDef("c", str(), "x"),
                f("contact", str()));
        ResolutionConfig cfg = new ResolutionConfig().withAlias("email", "contact");
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(writer, reader, cfg);
        assertTrue(result.isResolved());
        assertEquals(ResolutionStrategy.PROMOTE, findByField(result, "b").strategy());
        assertEquals(ConflictType.TYPE_PROMOTED, findByField(result, "b").type());
        assertEquals(ResolutionStrategy.USE_DEFAULT, findByField(result, "c").strategy());
        assertEquals(ResolutionStrategy.RENAME, findByField(result, "contact").strategy());
        assertEquals("x", result.defaultValues().get("c"));
        // gone is ignored by default — not reported as a conflict
        assertTrue(result.conflicts().stream().noneMatch(c -> c.type() == ConflictType.FIELD_REMOVED));
    }

    @Test
    void resolutionMatchesCheckerBackwardVerdict() {
        Schema v1 = v1();
        Schema v2 = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(v1, v2));
        ResolutionResult result = SchemaConflictResolver.resolveConflicts(v1, v2, new ResolutionConfig());
        assertTrue(result.isResolved());
        assertTrue(result.conflicts().stream().noneMatch(c -> c.strategy() == ResolutionStrategy.FAIL));
    }

    // ─── Guards ─────────────────────────────────────────────────────

    @Test
    void nullWriterRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SchemaConflictResolver.resolveConflicts(null, v1(), new ResolutionConfig()));
    }

    @Test
    void nonRecordSchemasRejected() {
        Schema writer = Schema.create(Schema.Type.STRING);
        assertThrows(IllegalArgumentException.class,
                () -> SchemaConflictResolver.resolveConflicts(writer, v1(), new ResolutionConfig()));
    }

    // ─── resolveDefaults ────────────────────────────────────────────

    @Test
    void resolveDefaultsCollectsAddedFieldDefaults() {
        Schema writer = v1();
        Schema reader = record("t1",
                f("id", str()), f("name", str()),
                fDef("region", str(), "NA"),
                f("nickname", nStr()),
                f("req", str()));
        Map<String, Object> defaults = SchemaConflictResolver.resolveDefaults(writer, reader);
        assertEquals("NA", defaults.get("region"));
        assertEquals(null, defaults.get("nickname"));
        assertFalse(defaults.containsKey("req"));
    }

    @Test
    void resolveDefaultsExplicitOverrideWins() {
        Schema writer = v1();
        Schema reader = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        Map<String, Object> defaults = SchemaConflictResolver.resolveDefaults(
                writer, reader, Map.of("region", "EU"));
        assertEquals("EU", defaults.get("region"));
    }

    @Test
    void resolveDefaultsIgnoresExistingFields() {
        Schema writer = v1();
        Map<String, Object> defaults = SchemaConflictResolver.resolveDefaults(v1(), writer);
        assertTrue(defaults.isEmpty());
    }

    @Test
    void resolveDefaultsNullRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SchemaConflictResolver.resolveDefaults(null, v1()));
    }

    // ─── applyDefaults ──────────────────────────────────────────────

    @Test
    void applyDefaultsFillsNullSlots() {
        Schema reader = record("t1", f("id", str()), f("region", str()));
        GenericRecord record = new GenericData.Record(reader);
        record.put("id", "x");
        SchemaConflictResolver.applyDefaults(record, Map.of("region", "NA"));
        assertEquals("NA", record.get("region"));
    }

    @Test
    void applyDefaultsKeepsExistingValues() {
        Schema reader = record("t1", f("id", str()), f("region", str()));
        GenericRecord record = new GenericData.Record(reader);
        record.put("id", "x");
        record.put("region", "EU");
        SchemaConflictResolver.applyDefaults(record, Map.of("region", "NA"));
        assertEquals("EU", record.get("region"));
    }

    // ─── buildAliasedSchema ─────────────────────────────────────────

    @Test
    void buildAliasedSchemaAddsRenameAlias() {
        Schema reader = record("t1", f("id", str()), f("contact", str()));
        Schema aliased = SchemaConflictResolver.buildAliasedSchema(reader, Map.of("contact", "email"));
        Schema.Field contact = aliased.getField("contact");
        assertTrue(contains(contact.aliases(), "email"));
        assertEquals(reader.getName(), aliased.getName());
    }

    @Test
    void buildAliasedSchemaPreservesExistingAliasesAndDefault() {
        Schema reader = record("t1", f("id", str()), withAliases(fDef("contact", str(), "N/A"), "legacy"));
        Schema aliased = SchemaConflictResolver.buildAliasedSchema(reader, Map.of("contact", "email"));
        Schema.Field contact = aliased.getField("contact");
        assertTrue(contact.hasDefaultValue());
        assertEquals("N/A", contact.defaultVal());
        assertTrue(contains(contact.aliases(), "legacy"));
        assertTrue(contains(contact.aliases(), "email"));
    }

    @Test
    void buildAliasedSchemaNullOrNonRecordRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SchemaConflictResolver.buildAliasedSchema(null, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> SchemaConflictResolver.buildAliasedSchema(Schema.create(Schema.Type.STRING), Map.of()));
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void resolvedConfigHasCodeLevelDefaults() {
        ResolutionConfig cfg = SchemaConflictResolver.resolveConfig();
        assertTrue(cfg.ignoreRemovedFields());
        assertTrue(cfg.useDefaultValues());
        assertTrue(cfg.allowAliases());
        assertFalse(cfg.strict());
    }

    @Test
    void configResolutionSystemPropertyOverride() {
        try {
            System.setProperty(SchemaConflictResolver.STRICT_KEY, "true");
            assertTrue(SchemaConflictResolver.resolveStrict());
            assertTrue(SchemaConflictResolver.resolveConfig().strict());
        } finally {
            System.clearProperty(SchemaConflictResolver.STRICT_KEY);
        }
    }

    @Test
    void invalidConfigValueFallsBackToDefault() {
        try {
            System.setProperty(SchemaConflictResolver.IGNORE_REMOVED_KEY, "bogus");
            assertTrue(SchemaConflictResolver.resolveIgnoreRemoved());
        } finally {
            System.clearProperty(SchemaConflictResolver.IGNORE_REMOVED_KEY);
        }
    }

    @Test
    void configCopyPreservesSettings() {
        ResolutionConfig cfg = new ResolutionConfig()
                .strict(true)
                .withFieldDefault("region", "NA")
                .withAlias("old", "new");
        ResolutionConfig copy = new ResolutionConfig(cfg);
        assertTrue(copy.strict());
        assertEquals("NA", copy.fieldDefaults().get("region"));
        assertEquals("new", copy.aliasMapping().get("old"));
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static FieldConflict findByField(ResolutionResult result, String fieldName) {
        return result.conflicts().stream()
                .filter(c -> c.fieldName().equals(fieldName))
                .findFirst()
                .orElse(null);
    }

    private static FieldConflict findByType(ResolutionResult result, ConflictType type) {
        return result.conflicts().stream()
                .filter(c -> c.type() == type)
                .findFirst()
                .orElse(null);
    }

    private static boolean contains(Iterable<String> values, String target) {
        for (String value : values) {
            if (value.equals(target)) {
                return true;
            }
        }
        return false;
    }
}