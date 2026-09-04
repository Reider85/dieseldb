# Plan for Prompt 89: Schema Evolution для Parquet

## Overview
Implement schema evolution support for Parquet files to handle:
1. **Adding new columns** (nullable) - when reading old files missing new columns, return null
2. **Removing columns** - when reading old files with extra columns, ignore them
3. **Changing types** (safe casts only) - when file type differs from expected, attempt safe cast

## Current Architecture Analysis

### Key Components
| Component | Role |
|-----------|------|
| `ParquetReader.readAll()` | Reads all rows from Parquet file |
| `ParquetReader.readProjected()` | Reads specific columns (projection pushdown) |
| `ParquetReader.readWhere()` | Reads with predicate pushdown |
| `Table.loadFromParquetFile()` | Loads table from Parquet, reconstructs schema |
| `ColumnarTableStorage.getRows()` | Columnar read path for OLAP queries |
| `ParquetWriter.writeTableToParquet()` | Writes table to Parquet with metadata |

## Implementation Plan

### 1. Create `ParquetSchemaManager.java` (New Class)
**Location:** `diesel/ParquetSchemaManager.java`

**Core Classes:**
```java
// Schema comparison result
class SchemaEvolutionPlan {
    Set<String> addedColumns;       // New columns in expected schema (not in file)
    Set<String> removedColumns;     // Columns in file but not in expected schema
    Map<String, TypeChange> typeChanges;
    
    static class TypeChange {
        Class<?> fileType;
        Class<?> expectedType;
        boolean isSafeCast;
    }
}

// Main API
public class ParquetSchemaManager {
    static SchemaEvolutionPlan computePlan(
        List<String> expectedColumns,
        Map<String, Class<?>> expectedTypes,
        MessageType fileSchema,
        Map<String, Class<?>> fileTypes
    );
    
    static Map<String, Object> applyEvolution(
        Map<String, Object> fileRow,
        SchemaEvolutionPlan plan,
        Map<String, Class<?>> expectedTypes
    );
    
    static Object safeCast(Object value, Class<?> fromType, Class<?> toType);
}
```

### 2. Modify `ParquetReader.java`
- Add `readFileSchemaInfo(Path file)` returning columns, types, and MessageType
- Modify `read()`, `readAll()`, `readProjected()`, `readWhere()` to accept optional `SchemaEvolutionPlan`
- Apply evolution to each row after reading when plan provided

### 3. Modify `Table.loadFromParquetFile()`
- Read expected schema from table definition (or use file schema as baseline)
- Compute evolution plan using `ParquetSchemaManager`
- Merge schemas (add missing columns as nullable)
- Pass plan to `ParquetReader.readAll()`

### 4. Modify `ColumnarTableStorage.getRows()`
- Add `setExpectedSchema(columns, types)` method
- Compute evolution plan if schemas differ
- Pass plan to ParquetReader methods

### 5. Safe Cast Logic
| From → To | Rule |
|-----------|------|
| Integer → Long/Double/Float | Widening |
| Long → Double/Float | Widening |
| Float → Double | Widening |
| String → Any | Parse (numbers, dates) |
| Any → String | toString() |
| Boolean → String | "true"/"false" |

**Unsafe (reject/return null):** Narrowing, Boolean↔Number, Date↔Number

### 6. Metadata Enhancement
Add `dieseldb.schemaVersion` to Parquet footer metadata for tracking.

## Test Cases (New: `ParquetSchemaEvolutionTest.java`)
- `testAddColumn()` - Old file missing new column → null
- `testRemoveColumn()` - Old file has extra column → ignored  
- `testTypeChangeSafeCast()` - INTEGER → LONG
- `testTypeChangeUnsafeCast()` - STRING "abc" → INTEGER → null
- `testColumnarReadWithEvolution()` - Columnar path with evolution

## Files to Change
| File | Change Type |
|------|-------------|
| `diesel/ParquetSchemaManager.java` | NEW |
| `diesel/ParquetReader.java` | MODIFY |
| `diesel/Table.java` | MODIFY (`loadFromParquetFile`) |
| `diesel/ColumnarTableStorage.java` | MODIFY |
| `diesel/ErrorMessages.java` | ADD constant |
| `src/test/java/diesel/ParquetSchemaEvolutionTest.java` | NEW TEST |

## Implementation Order
1. Create `ParquetSchemaManager` - Core logic
2. Modify `ParquetReader` - Add evolution plan parameter
3. Modify `Table.loadFromParquetFile` - Use schema manager
4. Modify `ColumnarTableStorage` - Support evolution
5. Add comprehensive tests
6. Run `make quick-test` then `make timing`

## Risk Mitigation
- **Backward compatibility**: No plan = current behavior
- **Performance**: Schema comparison cached; row transform minimal
- **Type safety**: Only safe casts; unsafe = null + warning
- **Testing**: All existing Parquet tests must pass