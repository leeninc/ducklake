# PR2–PR4: Server-side Postgres metadata reads for DuckLake

**Status:** IMPLEMENTED 2026-07-10 — branch `pg-server-side-metadata` (commit `1f68e67c`), all three PRs in one commit.
Implementation notes vs. this plan:
- PR4 option **3b is not possible**: zone-map predicates use `TRY_CAST`, which Postgres lacks. Implemented as hybrid (3a) via protected virtual *source hooks* (`GetDataFileSource`, `GetDeleteFileSource`, `GetFileColumnStatsJoinSource`, `GetFilePartitionValueSource`) overridden in `PostgresMetadataManager` with selective `postgres_query` subqueries; zone-map evaluation stays in DuckDB. No parser duplication needed.
- PR3 plumbed via commit-context fn `read_file_column_stats_for_table`; server-side commit keeps attached SQL (per §11 audit-separately).
- No feature flag added (§4.3) — overrides are small and the base path is one `git revert` away.
- Build: `PostgreSQL_ROOT=/opt/homebrew/opt/libpq ENABLE_POSTGRES_SCANNER=1 GEN=ninja make release` (needs Homebrew `libpq` ≥18 for postgres_oauth).
- Verified: PG statement log shows zero `ctid BETWEEN` COPYs; selective `WHERE table_id=` reads for all three paths; post-rewrite global stats exact; 486/486 tests pass on DuckDB catalog.

**Original status:** Ready to implement  
**Repo:** `/Users/neel/workspaces/ducklake` (fork)  
**Date:** 2026-07-10  
**Related app repo:** `/Users/neel/workspaces/leen/leen-security` (consumes the extension; no app changes required for PR2–PR4)

---

## 1. Problem statement

Under load, Leen workers open many DuckDB connections that ATTACH a shared DuckLake catalog in Postgres (`ducklake_meta`). Postgres logs show bulk parallel scans:

```sql
COPY (
  SELECT "data_file_id", "column_id", "value_count", "null_count",
         "min_value", "max_value", "contains_nan", "extra_stats"
  FROM "ducklake_meta"."ducklake_file_column_stats"
  WHERE ctid BETWEEN '(N,0)'::tid AND '(N+1000,0)'::tid
) TO STDOUT (FORMAT "binary");
```

Same pattern for `ducklake_table_column_stats` and `ducklake_data_file`.

Observed impact:
- hundreds of MB/s PG → worker transfer
- `file_column_stats` alone: tens of millions of COPY calls / hundreds of billions of rows returned in `pg_stat_statements`
- cleanup `DELETE FROM ducklake_file_column_stats WHERE data_file_id IN (...)` ~1.4s avg (separate index issue; not fixed by these PRs but related)

### Root cause (code-verified)

1. Most DuckLake metadata SQL runs via attached Postgres tables:
   - `DuckLakeMetadataManager::Query` → `transaction.ExecuteRaw(query)`
2. Metadata connection **forces**:
   ```cpp
   // src/storage/ducklake_transaction.cpp GetConnection()
   SET pg_experimental_filter_pushdown=false
   ```
   because postgres_scanner cannot handle all DuckDB filter types (e.g. `EXPRESSION_FILTER`) and would throw.
3. With pushdown off, `postgres_scanner` reads attached heaps with parallel **ctid page COPYs** (default 1000 pages/task) and filters in DuckDB.
4. Only a few paths already use selective server-side SQL via `postgres_query(...)`:
   - `GetLatestSnapshotQuery`
   - `PostgresMetadataManager::GenerateFileColumnStatsCTEBody` (zone-map CTE)

### Non-goal for PR2–PR4

- Do **not** re-enable `pg_experimental_filter_pushdown=true` globally.
- Do **not** change Leen writer/MERGE semantics.
- Do **not** implement worker connection pooling here (app follow-up).
- Do **not** add indexes here (ops parallel track; still recommended).

---

## 2. Solution strategy

Extend the existing **`postgres_query` pattern** to hot metadata **read** paths so Postgres executes selective SQL with predicates, instead of DuckDB full-scanning attached heaps.

```text
BEFORE:
  DuckDB plans SQL over attached ducklake_meta.* tables
  → ctid COPY full/near-full heaps
  → filter in DuckDB

AFTER (PR2–PR4 hot paths):
  DuckDB runs:
    SELECT * FROM postgres_query('meta_db',
      'SELECT ... FROM ducklake_meta.xxx WHERE table_id = N ...')
  → Postgres applies predicates
  → only matching rows transferred
  → no parallel full-table ctid fanout (postgres_query sets pages=0)
```

Keep writes/commits on existing `postgres_execute` / attached execute paths.

---

## 3. Existing patterns to copy

### 3.1 Good pattern (already in tree)

`src/metadata_manager/postgres_metadata_manager.cpp`:

```cpp
string PostgresMetadataManager::GenerateFileColumnStatsCTEBody(...) {
  return StringUtil::Format(
    "  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
    "    'SELECT %s\n"
    "     FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
    "     WHERE column_id = %d AND table_id = %d')\n",
    select_list, req.column_field_index, table_id.index);
}
```

`GetLatestSnapshotQuery()` uses the same wrapper.

### 3.2 Why `postgres_query` avoids ctid storms

In postgres_scanner `postgres_query.cpp` bind path:

```cpp
result->SetTablePages(0);  // disables ctid parallelization
result->sql = <user SQL with WHERE ... already present>;
```

### 3.3 Placeholder conventions

Attached-path SQL uses DuckDB placeholders substituted by `SubstituteCatalogPlaceholders` / `SubstituteSnapshotPlaceholders`:

| Placeholder | Meaning |
|---|---|
| `{METADATA_CATALOG}` | attached schema/catalog identifier for DuckDB |
| `{METADATA_CATALOG_NAME_LITERAL}` | metadata DB name as SQL string literal |
| `{METADATA_SCHEMA_ESCAPED}` | PG schema name escaped for embedding in a string |
| `{SNAPSHOT_ID}` | snapshot id integer |

For **inner** `postgres_query` SQL (executed by Postgres, not DuckDB):
- use **`{METADATA_SCHEMA_ESCAPED}.table`** (real PG schema), not DuckDB attach alias
- substitute snapshot values to concrete integers **before** wrapping
- escape single quotes inside the inner SQL string

### 3.4 Important current footgun

```cpp
unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
  return DuckLakeMetadataManager::Query(snapshot, query); // still attached ExecuteRaw
}
```

Do **not** change this to wrap *all* queries blindly. Only wrap known-safe hot read shapes via explicit overrides.

---

## 4. Shared infrastructure (do first, in PR2)

### 4.1 Add helper on `PostgresMetadataManager`

**Files:**
- `src/include/metadata_manager/postgres_metadata_manager.hpp`
- `src/metadata_manager/postgres_metadata_manager.cpp`

```cpp
// header (private/protected)
string WrapPostgresQuery(const string &pg_sql) const;
string QualifyPgTable(const string &table_name) const;
// e.g. ducklake_meta.ducklake_table_stats  (escaped schema + bare table)

// optional: snapshot-aware wrapper
unique_ptr<QueryResult> QueryPostgresSQL(DuckLakeSnapshot snapshot, string pg_sql);
```

**`WrapPostgresQuery` behavior:**
1. Input: pure Postgres SQL with schema-qualified tables, no DuckDB placeholders left (or only ones we substitute first).
2. Escape single quotes: `'` → `''`.
3. Return DuckDB SQL:
   ```sql
   SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, '<escaped pg_sql>')
   ```
4. Caller runs through normal `Query(...)` / connection query so outer placeholders (`{METADATA_CATALOG_NAME_LITERAL}`) still get substituted **or** substitute them inside the helper using catalog methods (preferred for less magic).

**Recommended helper implementation approach:**

```cpp
string PostgresMetadataManager::WrapPostgresQuery(const string &pg_sql) const {
  auto &cat = transaction.GetCatalog();
  auto catalog_literal = DuckLakeUtil::SQLLiteralToString(cat.MetadataDatabaseName());
  auto escaped = StringUtil::Replace(pg_sql, "'", "''");
  return StringUtil::Format(
      "SELECT * FROM postgres_query(%s, '%s')",
      catalog_literal, escaped);
}

string PostgresMetadataManager::QualifyPgTable(const string &table) const {
  // Match GenerateFileColumnStatsCTEBody style:
  // {METADATA_SCHEMA_ESCAPED}.table  then substitute, OR build directly:
  auto schema = DuckLakeUtil::SQLIdentifierToString(transaction.GetCatalog().MetadataSchemaName());
  // For embedding inside postgres_query string, prefer escaped schema without outer quotes issues.
  // Use same METADATA_SCHEMA_ESCAPED convention as existing code.
  ...
}
```

Mirror escaping exactly as `ExecuteQuery` does for `{METADATA_SCHEMA_ESCAPED}`.

### 4.2 Conversion helper: attached SQL → pure PG SQL

Many base methods build SQL with `{METADATA_CATALOG}.ducklake_xxx`.

For server-side execution, convert:

```text
{METADATA_CATALOG}.ducklake_foo  →  {METADATA_SCHEMA_ESCAPED}.ducklake_foo
```

or build PG SQL from scratch in the override (clearer, less brittle).

**Prefer rebuild in override** for P0 queries (small, known shapes).

### 4.3 Feature flag (recommended)

Env or DuckLake config option:

```text
ducklake_postgres_server_side_metadata = true  (default true once tested)
```

Or process env read once:

```text
DUCKLAKE_PG_SERVER_SIDE_METADATA=1
```

Allow emergency disable without rebuild if easy; otherwise default-on after staging soak is fine.

---

## 5. PR2 — `GetGlobalTableStats` via `postgres_query`

### Why first
Called on essentially every lake table scan/MERGE through:

- `DuckLakeCatalog::GetTableStats`
- `DuckLakeTableEntry::GetTableStats`
- `DuckLakeMultiFileList::GetCardinality`
- `ducklake_scan.cpp`

This explains `ducklake_table_column_stats` ctid COPYs during ordinary write activity.

### Current code

`src/storage/ducklake_metadata_manager.cpp`:

```cpp
vector<DuckLakeGlobalStatsInfo>
DuckLakeMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot, TableIndex table_id) {
  string query = StringUtil::Format(R"(
SELECT table_id, column_id, record_count, next_row_id, file_size_bytes,
       contains_null, contains_nan, min_value, max_value, extra_stats
FROM {METADATA_CATALOG}.ducklake_table_stats
LEFT JOIN {METADATA_CATALOG}.ducklake_table_column_stats USING (table_id)
WHERE table_id = %llu
  AND record_count IS NOT NULL
  AND file_size_bytes IS NOT NULL
ORDER BY table_id;
)", table_id.index);
  auto result = Query(snapshot, query);
  return TransformGlobalStats(*result);
}
```

Also note static all-tables template:

```cpp
static string GlobalTableStatsQuery(); // no table_id filter; used by server-side commit
```

Do **not** break server-side commit path. Only change the **instance method** `GetGlobalTableStats(snapshot, table_id)` for Postgres, or carefully wrap both if server-side commit executor also benefits (evaluate separately).

### Implementation

1. Declare override in `postgres_metadata_manager.hpp`:
   ```cpp
   vector<DuckLakeGlobalStatsInfo>
   GetGlobalTableStats(DuckLakeSnapshot snapshot, TableIndex table_id) override;
   ```

2. Implement in `postgres_metadata_manager.cpp`:
   ```cpp
   vector<DuckLakeGlobalStatsInfo>
   PostgresMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot, TableIndex table_id) {
     // Build pure PG SQL with schema-qualified tables and concrete table_id
     string pg_sql = StringUtil::Format(R"(
   SELECT table_id, column_id, record_count, next_row_id, file_size_bytes,
          contains_null, contains_nan, min_value, max_value, extra_stats
   FROM %s.ducklake_table_stats
   LEFT JOIN %s.ducklake_table_column_stats USING (table_id)
   WHERE table_id = %llu
     AND record_count IS NOT NULL
     AND file_size_bytes IS NOT NULL
   ORDER BY table_id
   )", schema_escaped, schema_escaped, table_id.index);

     string duck_sql = WrapPostgresQuery(pg_sql);
     auto result = Query(duck_sql); // or connection.Query after outer placeholder sub
     return TransformGlobalStats(*result);
   }
   ```

3. Reuse `TransformGlobalStats` / `ParseGlobalTableStats` unchanged.

### Correctness requirements
- Same column order/types as today
- Empty table / missing stats row → empty vector / same as before
- Snapshot parameter currently unused in the WHERE (stats tables are not snapshot-versioned the same way); preserve current behavior (don’t invent snapshot filters)

### Test plan (PR2)
1. Existing ducklake tests still pass.
2. New test with Postgres catalog (if harness exists) or manual staging:
   - create table, write data, call stats
   - with `log_min_duration_statement = 0` or statement logging, confirm:
     - **absent:** bare `FROM ducklake_table_column_stats WHERE ctid BETWEEN`
     - **present:** `WHERE table_id = ...` (or equivalent selective plan)
3. Functional: scan/MERGE cardinality and prune behavior unchanged.

### Success metrics (PR2 alone)
- Large drop in `table_column_stats` ctid COPY volume during write-only load
- Smaller drop overall network (file_column_stats still hot until PR3)

### Estimated scope
- ~50–100 LOC + tests
- Low risk

---

## 6. PR3 — `ReadFileColumnStatsForTable` via `postgres_query`

### Why
Matches the production log fingerprint of full `file_column_stats` COPYs during rewrite/compaction:

```sql
SELECT data.data_file_id, data.record_count, data.file_size_bytes,
       stats.column_id, stats.value_count, stats.null_count,
       stats.min_value, stats.max_value, stats.contains_nan, stats.extra_stats
FROM ducklake_data_file data
LEFT JOIN ducklake_file_column_stats stats ON stats.data_file_id = data.data_file_id
WHERE data.table_id = %d
  AND {SNAPSHOT_ID} >= data.begin_snapshot
  AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
ORDER BY data.data_file_id;
```

Used by:

```cpp
// ducklake_transaction_state.cpp
RecomputeGlobalStatsAfterRewrite(...)
  context.query_metadata_with_snapshot(
      snapshot, DuckLakeMetadataManager::ReadFileColumnStatsForTableSql(table_id));
```

Also relevant when Leen materialize runs `merge_adjacent_files` / `rewrite_data_files`.

### Current API shape problem

`ReadFileColumnStatsForTableSql` is a **static SQL builder**. Call sites execute via:
- `query_metadata_with_snapshot` (client commit)
- possibly server-side commit context

Static SQL + attached execution is what causes the bulk join scan.

### Implementation options

#### Option A (preferred): virtual method that returns results

```cpp
// base metadata manager
virtual unique_ptr<QueryResult>
ReadFileColumnStatsForTable(DuckLakeSnapshot snapshot, TableIndex table_id);

static string ReadFileColumnStatsForTableSql(TableIndex table_id); // keep for non-PG / server-side templates
```

Postgres override:
```cpp
unique_ptr<QueryResult>
PostgresMetadataManager::ReadFileColumnStatsForTable(DuckLakeSnapshot snapshot, TableIndex table_id) {
  string pg_sql = /* same as ReadFileColumnStatsForTableSql but:
                     - schema-qualified PG tables
                     - concrete snapshot_id
                     - no {METADATA_CATALOG}/{SNAPSHOT_ID} left */;
  return Query(WrapPostgresQuery(pg_sql));
}
```

Update call sites:
```cpp
// DuckLakeTransactionState::RecomputeGlobalStatsAfterRewrite
auto result = context.read_file_column_stats
  ? context.read_file_column_stats(snapshot, table_id)
  : context.query_metadata_with_snapshot(snapshot, ReadFileColumnStatsForTableSql(table_id));
```

Or cleaner: always plumb a function in `DuckLakeCommitContext`:

```cpp
std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, TableIndex)> read_file_column_stats_for_table;
```

Set it in `DuckLakeTransaction` when building commit context to call metadata manager virtual method.

#### Option B: wrap inside `query_metadata_with_snapshot` only for this SQL

Fragile (string matching). Avoid.

### SQL details for PG wrap

Inner SQL must:
1. Replace `{METADATA_CATALOG}.ducklake_data_file` → `ducklake_meta.ducklake_data_file` (escaped schema)
2. Replace `{SNAPSHOT_ID}` with `snapshot.snapshot_id`
3. Keep `table_id = %d` predicate
4. Prefer also constraining stats join if possible without changing semantics:
   ```sql
   LEFT JOIN ducklake_meta.ducklake_file_column_stats stats
     ON stats.data_file_id = data.data_file_id
    AND stats.table_id = data.table_id   -- if always true today, add for planner help
   ```
   Verify this is safe (stats rows always have matching table_id). If yes, add it.

### Indexes that make PR3 fast (ops, not blocking merge)
```sql
CREATE INDEX CONCURRENTLY ... ON ducklake_file_column_stats (data_file_id);
CREATE INDEX CONCURRENTLY ... ON ducklake_data_file (table_id, begin_snapshot, end_snapshot);
-- optional composite:
CREATE INDEX CONCURRENTLY ... ON ducklake_file_column_stats (table_id, data_file_id);
```

Without indexes, selective queries still beat full heap COPYs but may seq-scan within table_id.

### Correctness requirements
- `RecomputeGlobalStatsAfterRewrite` produces identical global min/max/null/nan results
- Compaction commit still succeeds under concurrency
- No type reinterpret issues on selected columns (all scalars/varchars already used elsewhere)

### Test plan (PR3)
1. Unit/functional: rewrite/merge_adjacent on a multi-file table; compare global stats before/after PR on same inputs
2. Log assertion: no full-table ctid COPY of `file_column_stats` during rewrite
3. Stress: concurrent MERGEs + one rewrite; no new conflict classes beyond existing snapshot retries

### Success metrics
- Collapse of `file_column_stats` ctid COPY rows during materialize/compaction windows
- Rewrite CPU/time may improve due to less transfer

### Estimated scope
- ~100–200 LOC including commit context plumbing
- Medium risk (commit path)

---

## 7. PR4 — `GetFilesForTable` / `GetExtendedFilesForTable` via `postgres_query`

### Why
Every table scan/MERGE lists live files:

```cpp
DuckLakeMultiFileList::GetFilesForTable()
  → metadata_manager.GetFilesForTable(table, snapshot, filter_info)
```

Base query (simplified):

```sql
SELECT data.data_file_id, <file cols>, data.row_id_start, ...,
       <delete file cols>
FROM {METADATA_CATALOG}.ducklake_data_file data
LEFT JOIN (
  SELECT * FROM {METADATA_CATALOG}.ducklake_delete_file
  WHERE table_id=%d AND snapshot bounds
) del ON del.data_file_id = data.data_file_id
WHERE data.table_id=%d AND snapshot bounds
[AND filter pushdown clauses using stats CTEs]
```

Even without stats, attached `data_file` scans show up as ctid COPYs.

### Complexity levels in this method

| Mode | Stats involvement | Difficulty |
|---|---|---|
| `filter_info == nullptr` | none | easiest wrap |
| `filter_info` with zone-map CTE | uses `GenerateFileColumnStatsCTEBody` (already `postgres_query` on PG) | medium — outer query still attached |
| dynamic Top-N filters | `LEFT JOIN file_column_stats` attached | hard — rewrite carefully |

### Implementation plan

#### Step 1: Override both methods on PostgresMetadataManager

```cpp
vector<DuckLakeFileListEntry>
GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                 const FilterPushdownInfo *filter_info) override;

vector<DuckLakeFileListExtendedEntry>
GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                         const FilterPushdownInfo *filter_info) override;
```

#### Step 2: Unfiltered path first (filter_info empty/null)

Build pure PG SQL equivalent of current base query with:
- schema-qualified tables
- concrete `table_id`, `snapshot_id`
- same column order as `ReadDataFile` / `ReadDeleteFile` expect

Wrap with `postgres_query`, then **reuse existing row parsing loop** from base method (factor parsing into shared helpers if needed to avoid duplication).

#### Step 3: Filtered path with zone-map CTE

Current filtered SQL roughly:

```sql
WITH col_X_stats AS MATERIALIZED (
  SELECT * FROM postgres_query(..., 'SELECT ... FROM file_column_stats WHERE column_id=.. AND table_id=..')
)
SELECT ...
FROM data_file data
LEFT JOIN delete_file ...
WHERE data.table_id=...
  AND (data_file_id NOT IN (SELECT ...) OR data_file_id IN (SELECT ... WHERE zone map pred))
```

Options:

**3a. Hybrid (good balance)**  
- Keep stats CTE as `postgres_query` (already)  
- Run **only the data_file/delete_file listing** via `postgres_query` without stats join  
- Apply zone-map filtering in DuckDB over the returned file list + stats CTE result  

But that may require restructuring. Simpler alternative:

**3b. Full server-side SQL (preferred if SQL stays PG-legal)**  
- Inline stats subquery as normal PG SQL (not nested postgres_query):
  ```sql
  WITH col_X_stats AS (
    SELECT data_file_id, min_value, max_value, value_count
    FROM ducklake_meta.ducklake_file_column_stats
    WHERE column_id = ? AND table_id = ?
  )
  SELECT ...
  FROM ducklake_meta.ducklake_data_file data
  ...
  ```
- Entire statement sent once through outer `postgres_query`
- Avoids double wrapper and attached scans entirely

Implement 3b for Postgres override by **rebuilding** filter SQL with PG schema names instead of calling base `GenerateFilterPushdownComponents` that embeds `{METADATA_CATALOG}` and DuckDB `postgres_query` CTEs.

Reuse filter predicate generation where possible:
- `ConvertFilterPushdownToSQL` produces predicates referencing `col_%d_stats` and stats column names — keep that
- Replace only CTE body generation to pure PG table reads (not nested postgres_query)

#### Step 4: Dynamic filter LEFT JOIN branch

Current:

```sql
LEFT JOIN {METADATA_CATALOG}.ducklake_file_column_stats stats_i
  ON stats_i.data_file_id = data.data_file_id
 AND stats_i.table_id = data.table_id
 AND stats_i.column_id = %d
```

In PG full-query wrap, this becomes a normal PG join with predicates — good, if indexes exist.

Ensure this path is included in the rebuilt SQL when dynamic filters present.

### Parsing / type pitfalls
- Keep column order identical to base implementation parsers
- Encryption key / path columns etc. must match `ReadDataFile`
- Test with tables that have delete files and without

### Test plan (PR4)
1. Unfiltered scan file list matches base implementation (same file ids/paths)
2. Equality filter on a column with zone maps returns same pruned file set
3. MERGE on `unique_hash` still correct (Leen path)
4. PG logs: `data_file` bare ctid full scans drop; selective `table_id` predicates appear
5. Extended file list path (deletes/compaction consumers) still works

### Success metrics
- Drop in `ducklake_data_file` ctid COPY volume
- Further drop in remaining `file_column_stats` traffic for filtered scans that previously fell into attached joins

### Estimated scope
- ~200–400 LOC (highest complexity of the three)
- Medium–high risk; ship after PR2/PR3 soak

---

## 8. Suggested PR sequence & branching

```text
main
  └─ pr2-pg-get-global-table-stats
       └─ pr3-pg-read-file-column-stats
            └─ pr4-pg-get-files-for-table
```

Each PR independently deployable to workers via extension bump.

### Deploy order to Leen
1. Build/publish ducklake extension artifact from PR2
2. Canary connector workers + one flight materialize path
3. Watch PG statements + network
4. PR3, then PR4 similarly

No leen-security code change required if extension is upgraded in place.

---

## 9. Files likely touched

### PR2
- `src/include/metadata_manager/postgres_metadata_manager.hpp`
- `src/metadata_manager/postgres_metadata_manager.cpp`
- tests under `test/` (add if harness supports PG)

### PR3
- same as PR2
- `src/include/storage/ducklake_metadata_manager.hpp` (virtual method)
- `src/storage/ducklake_metadata_manager.cpp` (default impl calling existing SQL)
- `src/include/storage/ducklake_transaction_state.hpp` (commit context fn if needed)
- `src/storage/ducklake_transaction_state.cpp` (`RecomputeGlobalStatsAfterRewrite`)
- `src/storage/ducklake_transaction.cpp` (wire commit context)
- `src/storage/ducklake_server_side_commit.cpp` (if it uses the same SQL)

### PR4
- `postgres_metadata_manager.hpp/cpp` overrides for GetFiles*
- possibly factor parsers from `ducklake_metadata_manager.cpp`
- filter SQL builder reuse / PG-specific CTE body path

---

## 10. Validation playbook (every PR)

### Local / CI
```bash
# from ducklake repo — use project’s normal test commands
make test
# or targeted:
./build/release/test/unittest "test/sql/*ducklake*"
```

### Staging experiment (required before prod)

**Before deploy window:** snapshot `pg_stat_statements` counters for ducklake relations.

**Workload:**
1. Write-only: 20–50 concurrent connector persists (no materialize)
2. Materialize/compaction-heavy: enable rewrite paths
3. Mixed

**For each workload, collect:**
- top statements by rows for `file_column_stats`, `table_column_stats`, `data_file`
- network TX on PG
- activity error rates / snapshot conflict retries
- p50/p95 flush times

**Pass criteria cumulative:**

| After | Expect |
|---|---|
| PR2 | `table_column_stats` ctid COPYs down sharply on write-only |
| PR3 | `file_column_stats` ctid COPYs down sharply on rewrite/materialize |
| PR4 | `data_file` ctid COPYs down; residual stats traffic mostly selective `WHERE table_id/column_id` |

### Log-level proof
With temporary statement logging, a single MERGE should show something like:

```sql
-- good
SELECT ... FROM ducklake_meta.ducklake_table_stats
 LEFT JOIN ducklake_meta.ducklake_table_column_stats USING (table_id)
 WHERE table_id = 123 ...;

SELECT ... FROM ducklake_meta.ducklake_data_file data
 WHERE data.table_id = 123 AND ... snapshot bounds ...;
```

and should **not** show:

```sql
COPY (SELECT ... FROM ducklake_meta.ducklake_file_column_stats
      WHERE ctid BETWEEN ...)  -- full table fanout
```

---

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Column order/type mismatch after wrap | Reuse parsers; golden tests compare file lists/stats rows |
| Nested `postgres_query` weirdness | Prefer pure PG SQL inside one outer wrap |
| Large result sets for huge tables still heavy | Still far better than global heap; follow with file compaction |
| Server-side commit path diverges | Keep static SQL builders; only override client metadata manager methods first; audit server-side separately |
| Quote/escaping bugs | Centralize WrapPostgresQuery; fuzz schema names with underscores |
| Performance regression if PG chooses bad plans | Add indexes (ops track); `EXPLAIN` selective queries |
| Accidentally wrapping writes | Only override read methods listed |

---

## 12. Out of scope follow-ups (do not block PR2–PR4)

1. **Ops indexes** on `file_column_stats(data_file_id)`, `(table_id, column_id)`, `data_file(table_id, begin_snapshot, end_snapshot)`, etc.
2. **leen-security worker DuckDB connection pooling** to cut ATTACH/schema reload tax
3. Safe postgres_scanner pushdown that ignores unsupported filters instead of throwing
4. Catalog sharding
5. DDL default indexes for new catalogs in `GetCreateTableStatements`

---

## 13. Implementation checklist for a new session

### Setup
- [ ] Open `/Users/neel/workspaces/ducklake`
- [ ] Read this doc fully
- [ ] Skim:
  - `src/metadata_manager/postgres_metadata_manager.cpp`
  - `src/storage/ducklake_metadata_manager.cpp` (`GetGlobalTableStats`, `GetFilesForTable`, `ReadFileColumnStatsForTableSql`)
  - `src/storage/ducklake_transaction.cpp` (`GetConnection` pushdown=false)
  - `src/storage/ducklake_transaction_state.cpp` (`RecomputeGlobalStatsAfterRewrite`)
- [ ] Confirm current branch; create `pr2-pg-get-global-table-stats` from latest main/master

### PR2 tasks
- [ ] Add `WrapPostgresQuery` (+ schema qualify helper)
- [ ] Override `GetGlobalTableStats`
- [ ] Build & run unit tests
- [ ] Manual/staging verification of statement shapes
- [ ] Open PR with metrics plan

### PR3 tasks (after PR2 merged or stacked)
- [ ] Add virtual `ReadFileColumnStatsForTable`
- [ ] Postgres override via wrap
- [ ] Wire commit context / call sites
- [ ] Verify rewrite/materialize path
- [ ] Open PR

### PR4 tasks
- [ ] Override `GetFilesForTable` + `GetExtendedFilesForTable`
- [ ] Unfiltered path first
- [ ] Filtered path with pure PG CTE rebuild
- [ ] Dynamic filter join path
- [ ] Compare file lists vs base implementation
- [ ] Open PR

### Done when
- [ ] Write-only and materialize staging loads show >90% drop in bare stats ctid COPY rows
- [ ] No correctness regressions on MERGE/scan/compaction
- [ ] Extension artifact consumable by leen workers

---

## 14. Quick reference: call sites → PR mapping

| Hot path | Symbol | PR |
|---|---|---|
| Table stats / cardinality | `GetGlobalTableStats` | **PR2** |
| Rewrite global stats recompute | `ReadFileColumnStatsForTableSql` + callers | **PR3** |
| File listing for scans/MERGE | `GetFilesForTable` | **PR4** |
| Extended file listing | `GetExtendedFilesForTable` | **PR4** |
| Zone-map stats CTE | `GenerateFileColumnStatsCTEBody` | Already good (leave) |
| Latest snapshot | `GetLatestSnapshotQuery` | Already good (leave) |
| Global pushdown flag | `GetConnection` sets false | Do not flip |

---

## 15. Context for humans / agents

This plan is the outcome of investigating production PG logs + DuckLake fork code. Key conclusions:

1. Transfer is dominated by **attached `postgres_scanner` ctid COPYs**, not by ATTACH alone.
2. DuckLake **intentionally disables** filter pushdown for metadata connections.
3. The fix is **selective `postgres_query`**, not “turn pushdown back on.”
4. Pooling connections in Leen helps open amplification but does **not** replace PR2–PR4.
5. Indexes help DELETE/cleanup and selective PG plans after PR2–PR4; they do not stop ctid COPYs while pushdown is false and queries remain attached scans.

When implementing, prefer small, reviewable PRs with staging proof after each step.

---

## 16. PR5 (added 2026-07-22): catalog load + commit conflict path server-side

Prod logs after PR2–PR4 rollout showed the remaining ctid COPY storms come from two paths this
plan never covered, visible only at prod heap sizes (small heaps don't fan out into ctid
ranges, which is why local verification missed them):

1. **Catalog schema load** — `BuildCatalogForSnapshot` (`ducklake_schema/table/column/tag/
   column_tag/view/macro*/partition*/sort*`), run on every fresh ATTACH and schema change.
   `ducklake_column` at ~6000 pages streamed whole per load.
2. **Commit retry path** — `CheckForConflicts` → `GetSnapshotAndStatsAndChanges` (all-tables
   `ducklake_table_stats ⋈ ducklake_table_column_stats` + `ducklake_snapshot_changes`) and
   `GetFilesDeletedOrDroppedAfterSnapshot` (`ducklake_data_file`/`ducklake_delete_file` full
   scans) on **every retry attempt**; plus `GetNetDataFileRowCount` (scan planning + commit).

Implementation (same patterns as PR2–PR4):
- `BuildCatalogForSnapshot` gained an optional `table_source(table_name, snapshot_filtered)`
  provider; base = attached tables; `PostgresMetadataManager::GetCatalogTableSource` wraps each
  in `postgres_query` with the snapshot bounds applied server-side. The DuckDB-only
  `LIST({...})` tag aggregation stays client-side (only the *sources* are wrapped). Macro
  correlated subqueries reference bare table names → wrapped sources are aliased to the
  original names. The DDL-commit schema rebuild (`transaction_state.cpp` `if (!new_tables.empty())`)
  keeps attached sources (rare path).
- New virtual `QueryConflictInfo(snapshot, sql)`; client `conflict_query_executor` routes
  through it; PG override rewrites `{METADATA_CATALOG}.` → `{METADATA_SCHEMA_ESCAPED}.` and
  wraps the whole query (both conflict queries are dialect-portable — no TRY_CAST here).
- PG overrides for `GetNetDataFileRowCount` / `GetNetInlinedRowCount` (also portable SQL).

Verified (podman PG 15.3, statement log): fresh-ATTACH catalog load = one selective
`COPY (SELECT * FROM ducklake_X WHERE snap >= begin_snapshot ...)` per table, correlated tag
subqueries decorrelated to single scans; forced commit collision (second process commits
mid-transaction) retries successfully with the conflict query running wholesale server-side;
**zero `ctid BETWEEN` statements across setup + attach + racing commits**.

Concurrency analysis & further options: `docs/plans/pg-commit-concurrency.md`.
