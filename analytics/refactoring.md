# Refactoring Plan: Extensible Storage Format Framework

Цель: чтобы поддерживать несколько форматов хранения (CSV, Parquet, и далее еще много — и строчных и колоночных) было удобно вносить правки в каждую версию и добавлять новые форматы.

## Current Architecture Problems

| Issue | Location | Impact |
|-------|----------|--------|
| Format selection hardcoded in `StorageFormat` enum | `StorageFormat.java:78-83` | Adding formats requires modifying core class |
| `Table` directly implements CSV/serialized/Parquet persistence | `Table.java:2474-2616` | Violates SRP; 200+ lines of format logic in Table |
| `TableStorage` interface tied to ROW_BASED/COLUMNAR dichotomy | `TableStorage.java:14-19` | Can't express hybrid or new format types |
| Parquet logic split across `ParquetWriter`, `ParquetReader`, `ColumnarTableStorage` | 3 files | Fragmented; hard to add similar formats |
| Global config only; no per-table format selection | `StorageFormat.java:34-35` | Inflexible for mixed workloads |

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Table                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Row Storage │  │  Indexes    │  │ FormatHandler (delegate)│  │
│  │ (in-memory) │  │ (BTree etc) │  │  - write(rows, schema)  │  │
│  └─────────────┘  └─────────────┘  │  - read(projection, pred)│  │
│                                    │  - schema inference     │  │
│                                    │  - capabilities         │  │
│                                    └───────────┬─────────────┘  │
└────────────────────────────────────────────────┼────────────────┘
                                                 │
                    ┌────────────────────────────┼────────────────────────────┐
                    ▼                            ▼                            ▼
           ┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
           │  CsvFormat      │          │ ParquetFormat   │          │  JsonFormat     │
           │  (implements    │          │  (implements    │          │  (future)       │
           │   TableFormat)  │          │   TableFormat)  │          │                 │
           └─────────────────┘          └─────────────────┘          └─────────────────┘
                    │                            │                            │
                    ▼                            ▼                            ▼
           ┌─────────────────────────────────────────────────────────────────┐
           │                    FormatRegistry                               │
           │  - register(formatName, TableFormat)                            │
           │  - get(formatName) → TableFormat                                │
           │  - resolve(tableName, config) → TableFormat                     │
           └─────────────────────────────────────────────────────────────────┘
```

## Core Interfaces (New Files)

### 1. `TableFormat.java` — Core abstraction for read/write/schema

```java
interface TableFormat {
    /** Unique format identifier (e.g., "CSV", "PARQUET", "JSON", "ORC") */
    String getName();

    /** Human-readable description */
    String getDescription();

    /** File extension including dot (e.g., ".csv", ".parquet") */
    String getFileExtension();

    /** Capabilities this format supports */
    FormatCapabilities getCapabilities();

    /** Write table data to file */
    void write(TableData data, Path filePath, WriteOptions options) throws IOException;

    /** Read table data from file with projection/predicate pushdown */
    TableData read(Path filePath, ReadOptions options) throws IOException;

    /** Infer schema from existing file (for CREATE TABLE FROM FILE) */
    TableSchema inferSchema(Path filePath) throws IOException;

    /** Validate file can be read by this format */
    boolean canRead(Path filePath);
}
```

### 2. `FormatCapabilities.java` — Feature matrix per format

```java
final class FormatCapabilities {
    // Read capabilities
    final boolean supportsProjection;
    final boolean supportsPredicatePushdown;
    final boolean supportsRowGroupStats;
    final boolean supportsPartitionPruning;

    // Write capabilities
    final boolean supportsCompression;
    final boolean supportsAppend;
    final boolean supportsSchemaEvolution;
    final CompressionCodec[] supportedCodecs;

    // Storage model
    final boolean isColumnar;
    final boolean isSplittable;

    // Factory for default capabilities by format type
    static FormatCapabilities rowBased() { ... }
    static FormatCapabilities columnar() { ... }
}
```

### 3. `TableData.java` — Unified data carrier

```java
final class TableData {
    final List<String> columns;
    final Map<String, Class<?>> columnTypes;
    final List<Map<String, Object>> rows;  // Lazy for large datasets
    final Map<String, Object> metadata;    // Sequences, indexes, etc.
}
```

### 4. `ReadOptions` / `WriteOptions` — Configuration objects

```java
final class ReadOptions {
    final List<String> projection;           // Columns to read (null = all)
    final List<Condition> pushdownPredicates; // WHERE conditions for pushdown
    final long limit;                         // Max rows
    final Map<String, String> formatOptions;  // Format-specific hints
}

final class WriteOptions {
    final CompressionCodec compression;
    final int rowGroupSize;                   // For columnar formats
    final Map<String, String> formatOptions;  // Format-specific hints
}
```

### 5. `FormatRegistry.java` — Plugin registry

```java
final class FormatRegistry {
    private static final Map<String, TableFormat> FORMATS = new ConcurrentHashMap<>();

    static void register(TableFormat format) { ... }
    static TableFormat get(String name) { ... }
    static TableFormat resolve(String tableName, Properties config) { ... }
    static Set<String> getRegisteredFormats() { ... }

    // Built-in registration happens in static initializer
    static { register(new CsvFormat()); register(new ParquetFormat()); }
}
```

## Migration Strategy

### Phase 1: Core Abstractions (Week 1)
1. Create `TableFormat`, `FormatCapabilities`, `TableData`, `ReadOptions`, `WriteOptions`, `FormatRegistry`
2. Add `FormatRegistry` static registration in `Database` constructor
3. **No behavioral changes** — just new interfaces/classes

### Phase 2: CSV Format Handler (Week 1-2)
1. Create `CsvFormat implements TableFormat`
2. Move CSV logic from `Table.saveToFile()` / `Table.loadFromFile()` to `CsvFormat`
3. Handle CSV-specific: quoting, escaping, null representation, type parsing
4. Register in `FormatRegistry`

### Phase 3: Parquet Format Handler (Week 2)
1. Create `ParquetFormat implements TableFormat`
2. Consolidate `ParquetWriter`, `ParquetReader`, `ColumnarTableStorage` logic
3. Handle Parquet-specific: row groups, compression, predicate pushdown, schema evolution
4. Register in `FormatRegistry`

### Phase 4: Table Refactor (Week 2-3)
1. Add `TableFormat formatHandler` field to `Table`
2. Delegate `saveToFile()` / `loadFromFile()` to `formatHandler`
3. Remove CSV/Parquet/serialized logic from `Table` (~200 lines removed)
4. Keep in-memory row storage, indexes, transactions — core Table logic unchanged

### Phase 5: StorageFormat & Database Integration (Week 3)
1. Refactor `StorageFormat` to use `FormatRegistry.resolve()`
2. Add per-table format config: `storage.format.TABLE_NAME=PARQUET`
3. Update `Database.persistModifiedTables()` to use table's format handler
4. Update `ColumnarConversionJob` to use `ParquetFormat` directly

### Phase 6: Configuration & Testing (Week 3-4)
1. Add config properties: `storage.format.default=CSV`, `storage.format.TABLE=PARQUET`
2. Update tests to verify format delegation
3. Add integration tests for format switching
4. Verify `make timing` passes

## File Map

### Files to Create (New)
| File | Purpose |
|------|---------|
| `diesel/format/TableFormat.java` | Core format interface |
| `diesel/format/FormatCapabilities.java` | Capability matrix |
| `diesel/format/TableData.java` | Unified data carrier |
| `diesel/format/ReadOptions.java` | Read configuration |
| `diesel/format/WriteOptions.java` | Write configuration |
| `diesel/format/FormatRegistry.java` | Plugin registry |
| `diesel/format/CsvFormat.java` | CSV implementation |
| `diesel/format/ParquetFormat.java` | Parquet implementation |

### Files to Modify
| File | Changes |
|------|---------|
| `Table.java` | Add `formatHandler` field; delegate persistence; remove 200+ lines of format logic |
| `StorageFormat.java` | Use `FormatRegistry.resolve()`; add per-table config support |
| `Database.java` | Initialize `FormatRegistry`; update `persistModifiedTables()` |
| `ColumnarConversionJob.java` | Use `ParquetFormat` directly |
| `ErrorMessages.java` | Add new format config keys |
| `TableStorage.java` | Extend interface if needed (or deprecate in favor of `TableFormat`) |

### Files to Potentially Remove/Simplify
| File | Reason |
|------|--------|
| `ColumnarTableStorage.java` | Logic moves to `ParquetFormat` + `TableFormat.read()` with pushdown |
| `ParquetWriter.java` / `ParquetReader.java` | Consolidated into `ParquetFormat` |

## Backward Compatibility

| Aspect | Strategy |
|--------|----------|
| Existing `.table` files | `CsvFormat`/`ParquetFormat` can read legacy formats via detection |
| `config.properties` | Global `storage.format` still works; per-table overrides are additive |
| Serialized tables | Keep Java serialization as a format option (`SERIALIZED`) |
| Columnar threshold | Moved to `ParquetFormat` config; default preserved |

## Adding a New Format (Future)

```java
// 1. Implement TableFormat
public class OrcFormat implements TableFormat {
    public String getName() { return "ORC"; }
    public String getFileExtension() { return ".orc"; }
    public FormatCapabilities getCapabilities() {
        return FormatCapabilities.columnar()
            .withCompression(CompressionCodec.SNAPPY, CompressionCodec.ZSTD);
    }
    public void write(TableData data, Path path, WriteOptions opts) { ... }
    public TableData read(Path path, ReadOptions opts) { ... }
    public TableSchema inferSchema(Path path) { ... }
    public boolean canRead(Path path) { return path.toString().endsWith(".orc"); }
}

// 2. Register (auto via SPI or manual)
FormatRegistry.register(new OrcFormat());

// 3. Use in config
# config.properties
storage.format.large_table=ORC
```

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Regression in CSV/Parquet | Phase 2-3: implement handlers first, verify with existing tests before Table refactor |
| Performance regression | Keep `TableData.rows` as `List<Map>` for now; lazy loading can be added later |
| Config migration | `StorageFormat.resolve()` falls back to global config; existing configs work unchanged |
| Columnar conversion job | Phase 5: update to use `ParquetFormat` directly; same logic, cleaner abstraction |

## Test Strategy

1. **Unit tests per format**: `CsvFormatTest`, `ParquetFormatTest` — test read/write/roundtrip
2. **Integration tests**: `FormatRegistryTest` — verify resolution, per-table config
3. **Regression tests**: Existing `PersistenceTest`, `ColumnarStorageTest` must pass
4. **Performance baseline**: `make timing` before/after to ensure no regression

## Estimated Effort

| Phase | Days | Risk |
|-------|------|------|
| Core abstractions | 1 | Low |
| CSV handler | 2 | Low |
| Parquet handler | 3 | Medium (complex Parquet logic) |
| Table refactor | 2 | Medium (touching core Table) |
| StorageFormat/Database integration | 2 | Low |
| Testing & config | 2 | Low |
| **Total** | **~12 days** | |