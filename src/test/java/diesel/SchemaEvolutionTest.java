package diesel;

import diesel.storage.avro.AvroSchemaManager;
import diesel.storage.avro.AvroTypeMapper;
import diesel.storage.avro.SchemaCompatibilityChecker;
import diesel.storage.avro.SchemaCompatibilityChecker.CompatibilityMode;
import diesel.storage.avro.SchemaCompatibilityChecker.CompatibilityReport;
import diesel.storage.avro.SchemaCompatibilityChecker.CompatibilityResult;
import diesel.storage.avro.SchemaCompatibilityChecker.SchemaDiff;
import diesel.storage.avro.SchemaEvolutionManager;
import diesel.storage.avro.SchemaEvolutionManager.SchemaVersion;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SchemaCompatibilityChecker} and {@link SchemaEvolutionManager} — Prompt 71.
 * Covers backward/forward/full Avro schema evolution rules, type promotion,
 * field diffs, config mode resolution, and versioned schema history persistence.
 */
@Tag("storage")
@StorageType("avro")
class SchemaEvolutionTest {

    @TempDir
    Path tempDir;

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

    private static Schema floatT() {
        return Schema.create(Schema.Type.FLOAT);
    }

    private static Schema fromSql(String table, String... cols) {
        TreeMap<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String c : cols) {
            types.put(c, String.class);
        }
        return AvroSchemaManager.buildTableSchema(table, List.of(cols), types);
    }

    private static Schema fromSqlWithTypes(String table, Map<String, Class<?>> types, String... cols) {
        return AvroSchemaManager.buildTableSchema(table, List.of(cols), types);
    }

    // ─── Backward compatibility ─────────────────────────────────────

    // v1 {id STRING, name STRING}
    private static Schema oldSchema() {
        return record("t1", f("id", str()), f("name", str()));
    }

    @Test
    void identicalSchemasAreBackwardCompatible() {
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldSchema(), oldSchema()));
    }

    @Test
    void addingFieldWithDefaultIsBackwardCompatible() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldSchema(), v2));
    }

    @Test
    void addingNullableFieldIsBackwardCompatibleWithoutExplicitDefault() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("nickname", nStr()));
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldSchema(), v2));
    }

    @Test
    void addingNonNullableFieldWithoutDefaultBreaksBackward() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        assertFalse(SchemaCompatibilityChecker.isBackwardCompatible(oldSchema(), v2));
    }

    @Test
    void removedFieldIsBackwardCompatible() {
        Schema v2 = record("t1", f("id", str()));
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldSchema(), v2));
    }

    @Test
    void widenedTypeIsBackwardCompatible() {
        Schema oldT = fromSqlWithTypes("t2", types(Integer.class), "qty");
        Schema newT = fromSqlWithTypes("t2", types(Long.class), "qty");
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldT, newT));
    }

    @Test
    void widenedTypeBreaksForwardDirection() {
        Schema oldT = fromSqlWithTypes("t2b", types(Integer.class), "qty");
        Schema newT = fromSqlWithTypes("t2b", types(Long.class), "qty");
        // an old int reader cannot decode long writer data
        assertFalse(SchemaCompatibilityChecker.isForwardCompatible(newT, oldT));
    }

    @Test
    void narrowedTypeBreaksBackward() {
        Schema oldT = fromSqlWithTypes("t3", types(Long.class), "qty");
        Schema newT = fromSqlWithTypes("t3", types(Integer.class), "qty");
        assertFalse(SchemaCompatibilityChecker.isBackwardCompatible(oldT, newT));
    }

    // ─── Forward compatibility ──────────────────────────────────────

    @Test
    void addingFieldsIsForwardCompatible() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("region", str()));
        assertTrue(SchemaCompatibilityChecker.isForwardCompatible(v2, oldSchema()));
    }

    @Test
    void removingReaderFieldBreaksForward() {
        Schema v2 = record("t1", f("id", str()));
        assertFalse(SchemaCompatibilityChecker.isForwardCompatible(v2, oldSchema()));
    }

    @Test
    void widenedTypeIsForwardCompatible() {
        Schema oldT = fromSqlWithTypes("t4", types(Integer.class), "qty");
        Schema newT = fromSqlWithTypes("t4", types(Long.class), "qty");
        assertFalse(SchemaCompatibilityChecker.isForwardCompatible(newT, oldT));
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldT, newT));
    }

    @Test
    void narrowedTypeKeepsForwardDirection() {
        Schema oldT = fromSqlWithTypes("t5", types(Long.class), "qty");
        Schema newT = fromSqlWithTypes("t5", types(Integer.class), "qty");
        // an old long reader can decode new int writer data
        assertTrue(SchemaCompatibilityChecker.isForwardCompatible(newT, oldT));
        assertFalse(SchemaCompatibilityChecker.isBackwardCompatible(oldT, newT));
    }

    // ─── Full compatibility ─────────────────────────────────────────

    @Test
    void addingNullableFieldIsFullyCompatible() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("nickname", nStr()));
        assertTrue(SchemaCompatibilityChecker.isFullyCompatible(oldSchema(), v2));
    }

    @Test
    void addingDefaultedFieldIsFullyCompatible() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), fDef("region", str(), "NA"));
        assertTrue(SchemaCompatibilityChecker.isFullyCompatible(oldSchema(), v2));
    }

    @Test
    void removingFieldIsNotFullyCompatible() {
        Schema v2 = record("t1", f("id", str()));
        assertFalse(SchemaCompatibilityChecker.isFullyCompatible(oldSchema(), v2));
    }

    @Test
    void addingFieldWithoutDefaultIsNotFullyCompatible() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        assertFalse(SchemaCompatibilityChecker.isFullyCompatible(oldSchema(), v2));
    }

    // ─── Mode / report ──────────────────────────────────────────────

    @Test
    void checkCompatibilityBackwardModeSucceeds() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("nickname", nStr()));
        CompatibilityReport report = SchemaCompatibilityChecker.checkCompatibility(oldSchema(), v2, CompatibilityMode.BACKWARD);
        assertTrue(report.compatible());
        assertEquals(CompatibilityMode.BACKWARD, report.mode());
        assertNotNull(report.summary());
    }

    @Test
    void checkCompatibilityBackwardModeFailsOnMissingDefault() {
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        CompatibilityReport report = SchemaCompatibilityChecker.checkCompatibility(oldSchema(), v2, CompatibilityMode.BACKWARD);
        assertFalse(report.compatible());
        assertTrue(report.diffs().stream().anyMatch(d -> d.severity() == CompatibilityResult.INCOMPATIBLE));
    }

    @Test
    void checkCompatibilityFullRequiresBothDirections() {
        // removing a field is backward-ok but forward-broken
        Schema v2 = record("t1", f("id", str()));
        assertFalse(SchemaCompatibilityChecker.checkCompatibility(oldSchema(), v2, CompatibilityMode.FULL).compatible());
    }

    @Test
    void noneModeIsAlwaysCompatible() {
        Schema v2 = record("t1", f("id", str()));
        CompatibilityReport report = SchemaCompatibilityChecker.checkCompatibility(oldSchema(), v2, CompatibilityMode.NONE);
        assertTrue(report.compatible());
        assertTrue(report.diffs().isEmpty());
    }

    // ─── Union handling & promotion ─────────────────────────────────

    @Test
    void nullableUnionPromotesToNullableUnion() {
        // writer: ["null", "int"] → reader: ["null", "long"]
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(nLong(), nInt()));
        assertFalse(SchemaCompatibilityChecker.isTypePromotable(nInt(), nLong()));
    }

    @Test
    void promotionChainIntToLongToDouble() {
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(doubleT(), longT()));
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(doubleT(), intT()));
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(longT(), intT()));
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(floatT(), intT()));
    }

    @Test
    void stringBytesPromoteBothWays() {
        Schema bytes = Schema.create(Schema.Type.BYTES);
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(bytes, str()));
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(str(), bytes));
        assertFalse(SchemaCompatibilityChecker.isTypePromotable(str(), intT()));
    }

    @Test
    void nullWriterTypeReadsAsNullableReaderOnly() {
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(nStr(), nullSchema));
        assertFalse(SchemaCompatibilityChecker.isTypePromotable(intT(), nullSchema));
        assertTrue(SchemaCompatibilityChecker.isTypePromotable(nullSchema, nullSchema));
    }

    // ─── Guards ─────────────────────────────────────────────────────

    @Test
    void nullOrNonRecordSchemasAreGuarded() {
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(null, oldSchema()));
        assertFalse(SchemaCompatibilityChecker.isBackwardCompatible(Schema.create(Schema.Type.INT), oldSchema()));
        CompatibilityReport report = SchemaCompatibilityChecker.checkCompatibility(null, oldSchema(), CompatibilityMode.BACKWARD);
        assertFalse(report.compatible());
    }

    // ─── Aliases ────────────────────────────────────────────────────

    @Test
    void renamedFieldViaAliasStaysCompatible() {
        // old schema writes "full_name"; new reader expects "name" aliased to "full_name"
        Schema oldR = record("u", f("full_name", str()));
        Schema.Field renamed = new Schema.Field("name", str(), null, (Object) null);
        renamed.addAlias("full_name");
        Schema reader = record("u", renamed);
        // backward: new reader reads old writer (has full_name, matched via alias) — no default required
        assertTrue(SchemaCompatibilityChecker.isBackwardCompatible(oldR, reader));
        // without the alias, the new "name" reader field would demand a default
        Schema noAlias = record("u", f("name", str()));
        assertFalse(SchemaCompatibilityChecker.isBackwardCompatible(oldR, noAlias));
    }

    // ─── collectDiffs ───────────────────────────────────────────────

    @Test
    void collectDiffsReportsAddedRemovedAndTypeChanges() {
        Schema oldR = record("t", f("id", str()), f("qty", intT()), f("gone", str()));
        Schema newR = record("t", f("id", str()), f("qty", longT()), f("nickname", nStr()));
        List<SchemaDiff> diffs = SchemaCompatibilityChecker.collectDiffs(oldR, newR);
        assertTrue(diffs.stream().anyMatch(d -> d.fieldName().equalsIgnoreCase("gone")
                && d.severity() == CompatibilityResult.WARNING));
        assertFalse(diffs.stream().anyMatch(d -> d.fieldName().equalsIgnoreCase("gone")
                && d.severity() == CompatibilityResult.INCOMPATIBLE));
        assertTrue(diffs.stream().anyMatch(d -> d.fieldName().equalsIgnoreCase("nickname")
                && d.severity() == CompatibilityResult.COMPATIBLE));
    }

    @Test
    void collectDiffsCleanPairIsEmpty() {
        assertTrue(SchemaCompatibilityChecker.collectDiffs(oldSchema(), oldSchema()).isEmpty());
    }

    // ─── Config mode resolution ─────────────────────────────────────

    @Test
    void resolveCompatibilityModeDefaultsToBackward() {
        assertNull(System.getProperty(SchemaCompatibilityChecker.MODE_CONFIG_KEY));
        assertEquals(CompatibilityMode.BACKWARD, SchemaCompatibilityChecker.resolveCompatibilityMode());
    }

    @Test
    void resolveCompatibilityModeHonoursSystemProperty() {
        System.setProperty(SchemaCompatibilityChecker.MODE_CONFIG_KEY, "FULL");
        try {
            assertEquals(CompatibilityMode.FULL, SchemaCompatibilityChecker.resolveCompatibilityMode());
        } finally {
            System.clearProperty(SchemaCompatibilityChecker.MODE_CONFIG_KEY);
        }
    }

    @Test
    void resolveCompatibilityModeInvalidValueFallsBack() {
        System.setProperty(SchemaCompatibilityChecker.MODE_CONFIG_KEY, "BOGUS");
        try {
            assertEquals(CompatibilityMode.BACKWARD, SchemaCompatibilityChecker.resolveCompatibilityMode());
        } finally {
            System.clearProperty(SchemaCompatibilityChecker.MODE_CONFIG_KEY);
        }
    }

    // ─── SchemaEvolutionManager: registration ───────────────────────

    @Test
    void firstVersionIsNumberOne() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        SchemaVersion v = mgr.registerSchema(oldSchema(), "create");
        assertEquals(1, v.version());
        assertEquals(1, mgr.getVersionCount());
        assertTrue(v.timestampMs() > 0);
        assertEquals("create", v.description());
    }

    @Test
    void versionsIncrementAndFullyCompatibleEvolutionRegisters() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("nickname", nStr()));
        SchemaVersion two = mgr.registerSchema(v2, "add nullable nickname");
        assertEquals(2, two.version());
        assertEquals(2, mgr.getVersionCount());
        assertEquals(two, mgr.getLatestVersion());
        assertEquals(1, mgr.getVersion(1).version());
        assertNull(mgr.getVersion(99));
    }

    @Test
    void incompatibleEvolutionThrows() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        assertThrows(IllegalArgumentException.class, () -> mgr.registerSchema(v2, "add non-null age"));
        assertEquals(1, mgr.getVersionCount());
    }

    @Test
    void forceRegistersIncompatibleEvolution() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        SchemaVersion two = mgr.registerSchema(v2, "forced break", true);
        assertEquals(2, mgr.getVersionCount());
        assertEquals(2, two.version());
    }

    @Test
    void evolveSchemaIsAliasOfRegister() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        SchemaVersion v = mgr.evolveSchema(oldSchema(), "evolve");
        assertEquals(1, v.version());
    }

    @Test
    void fullModeGateRejectsBackwardOnlyEvolution() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager("t", CompatibilityMode.FULL);
        mgr.registerSchema(oldSchema(), "v1");
        // removing field is backward-ok but forward-broken → fails FULL
        Schema v2 = record("t1", f("id", str()));
        assertThrows(IllegalArgumentException.class, () -> mgr.registerSchema(v2, "remove name"));
    }

    @Test
    void noneModeAcceptsAnyEvolution() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager(null, CompatibilityMode.NONE);
        mgr.registerSchema(oldSchema(), "v1");
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", intT()));
        assertDoesNotThrow(() -> mgr.registerSchema(v2, "any change"));
        assertEquals(2, mgr.getVersionCount());
    }

    @Test
    void nonRecordSchemaRejected() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        assertThrows(IllegalArgumentException.class,
                () -> mgr.registerSchema(Schema.create(Schema.Type.STRING), "nope"));
    }

    @Test
    void modeCanBeSetAndRead() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        assertEquals(CompatibilityMode.BACKWARD, mgr.getCompatibilityMode());
        mgr.setCompatibilityMode(CompatibilityMode.FULL);
        assertEquals(CompatibilityMode.FULL, mgr.getCompatibilityMode());
        mgr.setCompatibilityMode(null);
        assertEquals(CompatibilityMode.BACKWARD, mgr.getCompatibilityMode());
    }

    @Test
    void validateEvolutionDelegatesToChecker() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        Schema v2 = record("t1", f("id", str()), f("nickname", nStr()));
        assertTrue(mgr.validateEvolution(oldSchema(), v2, CompatibilityMode.BACKWARD).compatible());
        assertFalse(mgr.validateEvolution(oldSchema(), v2, CompatibilityMode.FORWARD).compatible());
    }

    // ─── SchemaEvolutionManager: evolution path ─────────────────────

    @Test
    void evolutionPathReturnsIntermediateSteps() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        mgr.registerSchema(record("t1", f("id", str()), f("nickname", nStr())), "v2");
        mgr.registerSchema(record("t1", f("id", str()), f("nickname", nStr()), fDef("region", str(), "NA")), "v3");
        List<SchemaVersion> path = mgr.getEvolutionPath(1, 3);
        assertEquals(2, path.size());
        assertEquals(2, path.get(0).version());
        assertEquals(3, path.get(1).version());
        assertTrue(mgr.getEvolutionPath(3, 3).isEmpty());
    }

    @Test
    void evolutionPathValidatesBounds() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        assertThrows(IllegalArgumentException.class, () -> mgr.getEvolutionPath(2, 1));
        assertThrows(IllegalArgumentException.class, () -> mgr.getEvolutionPath(1, 5));
        assertThrows(IllegalArgumentException.class, () -> mgr.getEvolutionPath(-1, 1));
    }

    // ─── SchemaEvolutionManager: persistence ────────────────────────

    @Test
    void versionHistoryRoundTrip() throws IOException {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        mgr.registerSchema(record("t1", f("id", str()), f("nickname", nStr())), "v2 add nickname");

        Path file = tempDir.resolve("history.json");
        mgr.writeVersionHistory(file);

        SchemaEvolutionManager restored = SchemaEvolutionManager.readVersionHistory(file);
        assertEquals(2, restored.getVersionCount());
        assertEquals(mgr.getAllVersions().size(), restored.getAllVersions().size());
        assertEquals("v1", restored.getVersion(1).description());
        assertEquals("v2 add nickname", restored.getVersion(2).description());
        Schema restoredOld = restored.getVersion(1).schema();
        Schema restoredNew = restored.getVersion(2).schema();
        assertEquals("id", restoredOld.getField("id").name());
        assertEquals(Schema.Type.STRING, restoredOld.getField("id").schema().getType());
        assertNotNull(restoredNew.getField("nickname"));
    }

    @Test
    void readVersionHistoryMissingFileThrows() {
        assertThrows(IOException.class,
                () -> SchemaEvolutionManager.readVersionHistory(tempDir.resolve("nope.json")));
    }

    @Test
    void writeVersionHistoryCreatesParentDirectories() throws IOException {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        mgr.registerSchema(oldSchema(), "v1");
        Path deep = tempDir.resolve("a").resolve("b").resolve("h.json");
        mgr.writeVersionHistory(deep);
        assertTrue(java.nio.file.Files.exists(deep));
    }

    @Test
    void defaultHistoryFileExpandsTableName() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager("Orders");
        Path p = mgr.defaultHistoryFile();
        assertEquals("Orders.schema-history.json", p.getFileName().toString());
    }

    @Test
    void defaultHistoryFileWithoutTable() {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager();
        assertEquals("schema-history.json", mgr.defaultHistoryFile().getFileName().toString());
    }

    @Test
    void historyFileConfigOverrideExpandsTablePlaceholder() {
        System.setProperty(SchemaEvolutionManager.HISTORY_FILE_KEY, "meta/{table}.hist.json");
        try {
            SchemaEvolutionManager mgr = new SchemaEvolutionManager("A-b");
            // the table name is sanitized (dash → underscore) before expansion
            String expected = "meta" + java.io.File.separator + "A_b.hist.json";
            assertEquals(expected, mgr.defaultHistoryFile().toString());
        } finally {
            System.clearProperty(SchemaEvolutionManager.HISTORY_FILE_KEY);
        }
    }

    // ─── Integration: full lifecycle ────────────────────────────────

    @Test
    void fullSchemaEvolutionLifecycle() throws IOException {
        SchemaEvolutionManager mgr = new SchemaEvolutionManager("users", CompatibilityMode.FULL);
        mgr.registerSchema(oldSchema(), "initial");
        Schema v2 = record("t1", f("id", str()), f("name", str()), f("age", nInt()));
        mgr.registerSchema(v2, "add nullable age");
        Schema v3 = record("t1", f("id", str()), f("name", str()));
        assertThrows(IllegalArgumentException.class, () -> mgr.registerSchema(v3, "drop age"));

        Path file = tempDir.resolve("users.history.json");
        mgr.writeVersionHistory(file);
        SchemaEvolutionManager restored = SchemaEvolutionManager.readVersionHistory(file);

        assertTrue(SchemaCompatibilityChecker.isFullyCompatible(
                restored.getVersion(1).schema(), restored.getVersion(2).schema()));
        assertEquals(2, restored.getVersionCount());
    }

    private static Map<String, Class<?>> types(Class<?> cls) {
        TreeMap<String, Class<?>> t = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        t.put("qty", cls);
        return t;
    }
}