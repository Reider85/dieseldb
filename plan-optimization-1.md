# Plan for addressing the 3× slowdown after switching to Parquet

## 1. Likely O(n²) / repeated‑work sources in the Parquet path

| # | Where it happens (file:line) | Why it can become O(n²) (or repeatedly O(n)) | Evidence from code |
|---|------------------------------|--------------------------------------------|--------------------|
| 1 | `SelectQuery.java:getIndexedRows` (≈2455) → returns `null` when no index exists | For columnar (`Parquet`) storage there are **no indexes** (`table.getIndex()` always returns `null`). Consequently `getIndexedRows` returns `null` and the caller falls back to `Table.getLiveRows()`. In `ColumnarTableStorage.getLiveRows()` (which delegates to `ParquetReader.readAll`) the **entire Parquet file is read and converted to a List<Map>** each time it is called. If a query references the same table multiple times (e.g., self‑join, multiple join stages, or repeated scans in sub‑queries) the file is read that many times → O(k·n) where *k* is the number of scans. |
| 2 | `SelectQuery.java:getIndexedRows` → `lookupCompositeIndex` / `tryCoveringIndex` | These helpers also eventually call `table.getRows()` (i.e. the same full scan) when no usable index/composite/covering index exists, adding another potential full scan per condition set. |
| 3 | `ParquetReader.readWhere` (lines ≈ 120‑150) | Although predicate push‑down is used to skip row‑groups, the method **re‑evaluates every condition with `matchesAll`** after reading the rows. For very wide tables this adds a constant factor but not O(n²). Still worth noting for constant‑factor tuning. |
| 4 | `ParquetReader.groupToRow` (≈455‑480) | For each row it iterates over **all column names** (even if only a subset is needed) and does a map lookup per column. If the query only needs a few columns, the loop still pays O(total_columns) per row. When total_columns is large this is a constant‑factor overhead, not quadratic, but it contributes to the 3× gap. |

*No actual nested‑loop over rows was found; the dominant issue is the **repeated full scans** caused by missing indexes on columnar storage.*

## 2. Quick‑win / 20 % effort → ≈80 % gain proposals

| # | Action | Expected impact | How to implement (read‑only hints) |
|---|--------|----------------|-----------------------------------|
| **A** | **Cache columnar table rows per‑query** – after the first `getLiveRows()` (or `getRows(columns,conditions)`) call, store the resulting `List<Map>` inside the `Table` object (or a query‑scoped cache) and reuse it for subsequent scans of the same table in the same query. | Avoids re-reading the same Parquet file multiple times in joins/sub‑queries → turns O(k·n) into O(n) + O(k·cached). | In `Table.java` add a transient `Map<QueryKey, List<Map<String,Object>>> rowCache;` (key = columns+conditions hash). In `ColumnarTableStorage.getRows(...)` check the cache before delegating to `ParquetReader`. Clear cache at end of query or on mutation. |
| **B** | **Refine OLAP auto‑switch** – only switch to columnar storage when the query exhibits OLAP traits (aggregation, GROUP BY, ORDER BY, LIMIT, or large result set). Currently the switch only happens for single‑table, index‑less queries (`mainRows == null && joins.isEmpty()`). Extend the condition to also require `queryType.isOlap()` or presence of `groupBy`, `orderBy`, `limit`, `having`, or aggregate functions. | Prevents columnar overhead for simple OLTP‑style queries (point lookups, small inserts/updates) while keeping it for true analytical workloads. | In `SelectQuery.java` around lines 957‑965 replace the `if (mainRows == null && joins.isEmpty())` block with a broader check that calls `QueryOptimizer.isOlapQuery(this, table)` (you can reuse existing classifier logic). |
| **C** | **Push projection down earlier** – ensure that when a `SELECT` lists only a subset of columns, the `columns` argument is passed to `ParquetReader.readProjected`/`readWhere` for *all* code paths (including the fallback path in `getIndexedRows` when `mainRows == null`). | Reduces per‑row work in `groupToRow` from O(total_columns) to O(requested_columns). For wide tables this can cut CPU time significantly. | Scan `SelectQuery.java` for calls to `getIndexedRows` that discard the `combinedColumnTypes` argument; modify the fallback (`mainRows = table.getLiveRows();`) to instead call `table.getRows(projectedColumns, conditions)` when a projection is known (the projection plan is already built at line 949). |
| **D** | **Optimize `COUNT(*)` and similar aggregations** – when the query only needs the row count (e.g., `SELECT COUNT(*) FROM t`), use Parquet row‑group statistics to obtain the count without materializing any rows. | Eliminates O(n) work for common aggregate queries. | In `SelectQuery.java` detect a projection that is solely an aggregate with no grouping; if the table is columnar and `conditions` is empty/null, call `ParquetReader.getRowCount(file)` (you can add a small helper that sums `pageStore.getRowCount()`). |
| **E** | **Enable dictionary & byte‑batch optimizations** – verify that `ParquetWriter` and `ParquetReader` are using the default Parquet dictionary encoding (they already appear to use `GroupReadSupport`/`GroupWriteSupport`). Ensure that `writeTableToParquet` does not disable dictionary or use unnecessary validation steps that add overhead. | Lowers I/O and CPU cost per column, especially for low‑cardinality string columns. | No code change needed if defaults are already used; just confirm in `ParquetWriter.java` (lines ≈ 178‑190) that `ParquetProperties.builder()` is not overriding dictionary settings. |
| **F** | **Lazy conversion of columnar rows to in‑memory row‑based format for hash‑join build side** – when a hash join needs the build table, read the columnar rows once, convert them to a simple `List<Object[]>` (or int/double arrays) and reuse that structure for probing. | Avoids repeated `groupToRow` map creation per probe row; hash‑join probing becomes faster. | In `SelectQuery.java:getIndexedRows` (or a new helper) when `table` is columnar and the caller is a hash join (detect via context), return a custom `List<Object[]>` instead of `List<Map>`. Adjust hash‑join code to read from arrays. |

*Items A‑C together address the repeated‑scan and per‑row overhead, which are the biggest contributors to the observed 3× slowdown. Implementing any one of them should give a noticeable boost; combining them yields the target ~80 % improvement.*

## 3. Suggested default configuration change

- **Set `storage.format` default to `CSV` (or `ROW_BASED`)** in `ErrorMessages.java` or `StorageFormat.java`.
- Keep the Parquet backend available; users who want OLAP can explicitly set `storage.format=PARQUET` in `config.properties` or via the `SET storage.format = PARQUET;` command.
- This ensures that existing OLTP‑style workloads (which make up the majority of traffic) continue to enjoy the fast CSV path, while analytical workloads can opt‑in to columnar storage when they need it.

**Implementation hint:** In `StorageFormat.java` the method `configuredFormat()` reads the property; change the fallback in `getStorageFormat(String val)` (lines ≈ 66‑72) to return `ErrorMessages.STORAGE_FORMAT_CSV` when the property is missing/invalid, instead of `PARQUET`.

## 4. Verification steps (read‑only)

1. **Check current default**:  
   ```bash
   grep -r "STORAGE_FORMAT_PARQUET\|STORAGE_FORMAT_CSV" diesel/ --include="*.java"
   ```
   Look at the fallback in `StorageFormat.getStorageFormat`.

2. **Confirm columnar storage has no indexes**:  
   ```bash
   grep -r "getIndex\|createBTreeIndex" diesel/ColumnarTableStorage.java
   ```
   Expect no index‑related methods.

3. **See where `getLiveRows` is used for joins**:  
   ```bash
   grep -r "getLiveRows\|getRows(null,null)" diesel/SelectQuery.java
   ```
   Should show the fallback branches in hash‑join and nested‑loop code.

4. **Look for caching opportunities**:  
   Search for `getLiveRows` call sites and see if any store the result.

## 5. Summary of the plan

- **Root cause:** Missing indexes on Parquet storage cause `getIndexedRows` → `null` → full `ParquetReader.readAll` on every scan; repeated scans turn linear work into quadratic‑like cost.
- **Immediate fix:** Disable columnar storage by default (use CSV/row‑based) and enable it only when explicitly configured or when the query displays OLAP characteristics.
- **High-impact, low-effort optimizations:** Cache columnar rows per query, push column projections down earlier, and avoid re‑reading the same Parquet file for aggregates.
- **Further tuning:** Optimize hash‑join build side to work with cached columnar data, and ensure Parquet reader/writer use efficient defaults.

Follow the steps above, re‑run the quick‑test (`make quick-test`) to validate correctness, then run the full acceptance gate (`make timing`) to ensure no regressions. Once satisfied, create a changelog entry, commit, and push as prescribed in `AGENTS.md`.