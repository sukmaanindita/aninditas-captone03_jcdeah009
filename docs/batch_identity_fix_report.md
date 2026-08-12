# Batch Identity Fix — Implementation Report

Scope: batch pipeline trip-identity correctness fix only (`event_id`, dedup, MERGE key).
No streaming, analytics, README, or architecture-diagram work included.
No BigQuery query was executed and no cloud resource was modified by the assistant.
All SQL below is prepared for **manual execution by the user**.

Real resource names used throughout (from `.env`, not invented):

| Config | Value |
|---|---|
| `GCP_PROJECT_ID` | `jcdeah-009` |
| `BQ_DATASET_STAGING` | `cp3_aninditas_staging` |
| `BQ_DATASET_CURATED` | `cp3_aninditas_curated` |
| `BQ_STAGING_TABLE_BATCH` | `green_tripdata_staging_batch` |
| `BQ_CURATED_TABLE` | `green_tripdata_curated` |

Full IDs used below:
- Staging (batch): `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`
- Curated: `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`

---

## 1. Files changed

| File | Type | Reason |
|---|---|---|
| `dags/scripts/load_staging.py` | modified | capture `source_row_number` / `source_file_identity`, fix dedup logic, enable schema evolution on load |
| `sql/transform_to_curated.sql` | modified | deterministic `event_id` for BATCH branch, simplified MERGE key |
| `tests/test_batch_identity.py` | new | identity/dedup unit tests (no cloud dependency) |

Not changed (considered, decided unnecessary):
- `sql/create_curated_tables.sql` — curated schema is unchanged; `source_row_number`/`source_file_identity` are **transient** (used only inside the hash expression), never persisted to the curated table.
- `sql/create_staging_stream.sql`, `sql/merge_batch.sql` — streaming/mart, out of approved scope. `merge_batch.sql` is not referenced by any Python script (`transform.py` only loads `create_curated_tables.sql` and `transform_to_curated.sql`); it appears to be dead/legacy code using yellow-taxi columns (`tpep_*`) and `GENERATE_UUID()`. Flagged in §12, not modified.
- `dags/*.py` (Airflow DAGs) — no task wiring changes were required for this fix.

---

## 2. Summary per change

**`dags/scripts/load_staging.py`**
- Added `assign_source_identity(df, source_file_identity)`: a pure function that stamps `source_row_number` (0-based positional index, `np.arange`) and `source_file_identity` (`config.gcs_blob_path`) onto the DataFrame **immediately after `read_parquet`**, before any lowercasing, type casting, period filtering, or dedup.
- Added `deduplicate_business_rows(df)`: replaces the old `df.drop_duplicates()` (full-row, all columns) with `drop_duplicates(subset=business_columns, keep='first')`, where `business_columns` = all columns **except** `source_row_number`, `source_file_identity`, `loaded_at`. Rows are sorted by `source_row_number` first so `keep='first'` deterministically keeps the lowest-row-number occurrence.
- Both functions were extracted specifically so they're unit-testable without GCS/BigQuery.
- `load_parquet_from_gcs()` now calls these two functions instead of the old inline logic. Behavior for a clean file with no true duplicates is unchanged.
- `LoadJobConfig` now sets `schema_update_options=[SchemaUpdateOption.ALLOW_FIELD_ADDITION]` so the next `WRITE_APPEND` load can add the two new columns to the existing staging table automatically (see §6 for the manual/explicit alternative).

**`sql/transform_to_curated.sql`**
- BATCH branch: `CAST(NULL AS STRING) AS event_id` replaced with a deterministic `TO_HEX(SHA256(...))` expression over `source_file_identity`, `source_row_number`, and core business fields (§3). Every component is wrapped in `IFNULL(..., '')` because BigQuery's `CONCAT()` returns `NULL` if *any* argument is `NULL` — without this, one missing business field (e.g. `passenger_count`) would silently null out the entire `event_id`.
- STREAMING branch: unchanged — still selects `event_id` directly from `{staging_stream_table}`.
- Internal staging dedup: `ROW_NUMBER() ... PARTITION BY COALESCE(event_id, <composite>)` simplified to `PARTITION BY event_id`, since `event_id` is now always populated on both branches.
- MERGE key: `ON COALESCE(T.event_id, <composite>) = COALESCE(S.event_id, <composite>)` simplified to `ON T.event_id = S.event_id`.

**`tests/test_batch_identity.py`** — see §9 / test list below.

---

## 3. Final `event_id` formula

```
event_id = TO_HEX(SHA256(CONCAT(
    IFNULL(source_file_identity, ''), '|',
    IFNULL(CAST(source_row_number AS STRING), ''), '|',
    IFNULL(CAST(lpep_pickup_datetime AS STRING), ''), '|',
    IFNULL(CAST(lpep_dropoff_datetime AS STRING), ''), '|',
    IFNULL(CAST(vendorid AS STRING), ''), '|',
    IFNULL(CAST(pulocationid AS STRING), ''), '|',
    IFNULL(CAST(dolocationid AS STRING), ''), '|',
    IFNULL(CAST(passenger_count AS STRING), ''), '|',
    IFNULL(CAST(trip_distance AS STRING), ''), '|',
    IFNULL(CAST(fare_amount AS STRING), ''), '|',
    IFNULL(CAST(total_amount AS STRING), ''), '|',
    IFNULL(CAST(payment_type AS STRING), '')
)))
```

- `source_file_identity` = `config.gcs_blob_path`, e.g. `data/raw/green_tripdata/2025/04/green_tripdata_2025-04.parquet`. Guarantees April and May can never collide, since the file path always encodes year/month.
- `source_row_number` = 0-based position of the row in the raw parquet file, assigned before any filter/dedup/sort. Guarantees two different physical rows never collide, even if every business field happens to match.
- Business fields are included as defense-in-depth / demo-readability (the hash visibly reflects trip content), not as the primary uniqueness driver.
- Rerunning the same file reproduces identical `(source_file_identity, source_row_number)` pairs for the same physical rows → identical `event_id` → idempotent MERGE.

---

## 4. Final dedup logic

**Before:**
```python
df = df.drop_duplicates()   # full row, including a per-run-constant loaded_at
```

**After:**
```python
df = assign_source_identity(df, source_file_identity=config.gcs_blob_path)
...
df = deduplicate_business_rows(df)
# subset = all columns except source_row_number, source_file_identity, loaded_at
# sorted by source_row_number first, keep='first'
```

Why this was necessary: once `source_row_number` is attached to every row *before* dedup, a naive full-row `drop_duplicates()` would never find a match again (every row is unique by construction), silently disabling duplicate detection. The fix explicitly excludes the identity/metadata columns from the dedup subset, so two source rows are only merged if **every business field matches** — a broad, strict match, which minimizes the risk of collapsing two legitimately distinct trips (safer than the old narrow 3-field key that used to live in the curated MERGE).

---

## 5. Before/after MERGE logic

**Before:**
```sql
ON COALESCE(
     T.event_id,
     CONCAT(CAST(T.pickup_datetime AS STRING), '_', CAST(T.pickup_location_id AS STRING), '_', CAST(T.fare_amount AS STRING))
   ) =
   COALESCE(
     S.event_id,
     CONCAT(CAST(S.pickup_datetime AS STRING), '_', CAST(S.pickup_location_id AS STRING), '_', CAST(S.fare_amount AS STRING))
   )
```
Risk: two distinct trips sharing the same `pickup_datetime|pickup_location_id|fare_amount` (realistic in busy zones) would match and silently overwrite each other.

**After:**
```sql
ON T.event_id = S.event_id
```
`event_id` is now always non-NULL for both BATCH (deterministic hash) and STREAMING (native `event_id`), so the fallback composite key is no longer needed and has been removed.

---

## 6. SQL DDL for manual execution

Run these **in order**, in BigQuery Console or `bq query`. Each is labeled read-only (safe) or mutating (review before running).

**DDL-1 — [READ-ONLY] View current staging schema**
```sql
SELECT column_name, data_type, is_nullable
FROM `jcdeah-009.cp3_aninditas_staging.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'green_tripdata_staging_batch'
ORDER BY ordinal_position;
```
Expected before rebuild: no `source_row_number` / `source_file_identity` columns present.

**DDL-2 — [ADDITIVE, non-destructive] Add identity columns to staging (explicit alternative to relying on `ALLOW_FIELD_ADDITION`)**
```sql
ALTER TABLE `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`
  ADD COLUMN IF NOT EXISTS source_row_number INT64,
  ADD COLUMN IF NOT EXISTS source_file_identity STRING;
```
Note: this step is optional. The updated `load_staging.py` (`schema_update_options=[ALLOW_FIELD_ADDITION]`) will add these columns automatically the next time April/May are (re)loaded. Run this manually only if you want the columns to exist before that first rerun, or prefer explicit control over implicit schema evolution.

**DDL-3 — [READ-ONLY] View current curated schema**
```sql
SELECT column_name, data_type, is_nullable
FROM `jcdeah-009.cp3_aninditas_curated.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'green_tripdata_curated'
ORDER BY ordinal_position;
```
Confirms the curated schema is unchanged (`source_row_number` / `source_file_identity` should **not** appear here — they are transient, computed only inside the MERGE, never persisted).

**DDL-4 — [READ-ONLY] Count existing BATCH curated rows with the old NULL `event_id`**
```sql
SELECT data_source, COUNT(*) AS row_count, COUNTIF(event_id IS NULL) AS null_event_id_count
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
GROUP BY data_source;
```
Under the *old* logic, every existing BATCH row has `event_id IS NULL` (the BATCH branch always selected `CAST(NULL AS STRING) AS event_id`). This is expected — it is not the bug itself, it's the reason the migration in DDL-5 is required.

**DDL-5 — [DESTRUCTIVE, scoped] Remove stale BATCH rows before reprocessing**

Why this step is required: with the new MERGE key `ON T.event_id = S.event_id`, a `NULL` target `event_id` will never match a freshly-computed hash `event_id` (`NULL = <hash>` is never true in SQL). If left in place, the old NULL-`event_id` BATCH rows would sit alongside newly-inserted, correctly-keyed rows — i.e. duplicates. Since the curated table is fully derived/rebuildable from staging, deleting old BATCH rows and reprocessing is the simplest safe fix — it does not touch STREAMING rows (none exist yet) or the underlying staging/raw data in GCS.

```sql
DELETE FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE data_source = 'BATCH';
```
Preferred over `TRUNCATE TABLE` because it is explicitly scoped to `data_source = 'BATCH'` and will not silently wipe future STREAMING rows if this is ever run again after streaming is live.

**DDL-6 — [READ-ONLY] Confirm cleanup**
```sql
SELECT data_source, COUNT(*) AS row_count
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
GROUP BY data_source;
```
Expected: no `BATCH` row present (0 rows, or the row entirely absent from the group-by result).

---

## 7. Verification queries (manual rebuild procedure, STEP 1–8)

Run in this exact order. Each step is marked **[SAFE]** (read-only) or **[DESTRUCTIVE]** (mutates data).

1. **[SAFE]** DDL-1 — inspect current staging schema.
2. **[SAFE]** DDL-4 — record current curated row counts (baseline, "before").
3. **[DESTRUCTIVE]** DDL-5 — `DELETE FROM curated WHERE data_source = 'BATCH'`.
4. **[SAFE]** DDL-6 — confirm 0 BATCH rows remain in curated.
5. **[MUTATING, application-level]** Rerun `load_staging.py` for April, then May:
   ```bash
   python dags/scripts/load_staging.py --year 2025 --month 04 --validate
   python dags/scripts/load_staging.py --year 2025 --month 05 --validate
   ```
   This DELETE+INSERTs the staging rows for each period and, via `ALLOW_FIELD_ADDITION`, adds `source_row_number` / `source_file_identity` to the staging table if DDL-2 wasn't run manually.
6. **[SAFE]** DDL-1 again — confirm `source_row_number` (INT64) and `source_file_identity` (STRING) now exist in the staging schema.
7. **[MUTATING, application-level]** Rerun curation for April, then May (via `transform.py` / the `cp3_aninditas_curation` DAG):
   ```bash
   python -c "from dags.scripts.transform import task_transform_curated, task_validate_curated; task_transform_curated(year='2025', month='04'); task_validate_curated(year='2025', month='04')"
   python -c "from dags.scripts.transform import task_transform_curated, task_validate_curated; task_transform_curated(year='2025', month='05'); task_validate_curated(year='2025', month='05')"
   ```
8. **[SAFE]** Run all validation queries in §8 below.

---

## 8. Validation queries

**(A) Schema check — `source_row_number` / `event_id` types**
```sql
SELECT column_name, data_type
FROM `jcdeah-009.cp3_aninditas_staging.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'green_tripdata_staging_batch'
  AND column_name IN ('source_row_number', 'source_file_identity');
```
Expected: `source_row_number` = `INT64`, `source_file_identity` = `STRING`.

**(B) April row count — staging vs curated**
```sql
SELECT
  (SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`
    WHERE EXTRACT(YEAR FROM lpep_pickup_datetime)=2025 AND EXTRACT(MONTH FROM lpep_pickup_datetime)=04) AS staging_april,
  (SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
    WHERE data_source='BATCH' AND EXTRACT(YEAR FROM pickup_date)=2025 AND EXTRACT(MONTH FROM pickup_date)=04) AS curated_april;
```
Expected: `curated_april <= staging_april` (curated can be lower due to the `WHERE` filters in `transform_to_curated.sql` — e.g. `dropoff_datetime > pickup_datetime`, `trip_distance >= 0`, `fare_amount >= 0` — never higher).

**(C) May row count — staging vs curated**
```sql
SELECT
  (SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`
    WHERE EXTRACT(YEAR FROM lpep_pickup_datetime)=2025 AND EXTRACT(MONTH FROM lpep_pickup_datetime)=05) AS staging_may,
  (SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
    WHERE data_source='BATCH' AND EXTRACT(YEAR FROM pickup_date)=2025 AND EXTRACT(MONTH FROM pickup_date)=05) AS curated_may;
```
Expected: same relationship as (B).

**(D) Duplicate `event_id` check**
```sql
SELECT event_id, COUNT(*) AS cnt
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
GROUP BY event_id
HAVING COUNT(*) > 1;
```
Expected: **0 rows returned.**

**(E) Null `event_id` check**
```sql
SELECT COUNT(*) AS null_event_id_count
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
WHERE event_id IS NULL;
```
Expected: `0`.

**(F) `event_id` collision check across different source rows**
```sql
SELECT
  b.event_id,
  COUNT(*) AS distinct_rows_matching_event_id
FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch` b
GROUP BY b.event_id
HAVING COUNT(*) > 1;
```
Note: `event_id` is not a physical column in the staging table (it's computed in `transform_to_curated.sql`); to check for hash collisions directly against staging rows, run:
```sql
WITH hashed AS (
  SELECT
    source_row_number,
    TO_HEX(SHA256(CONCAT(
      IFNULL(source_file_identity, ''), '|',
      IFNULL(CAST(source_row_number AS STRING), ''), '|',
      IFNULL(CAST(lpep_pickup_datetime AS STRING), ''), '|',
      IFNULL(CAST(lpep_dropoff_datetime AS STRING), ''), '|',
      IFNULL(CAST(vendorid AS STRING), ''), '|',
      IFNULL(CAST(pulocationid AS STRING), ''), '|',
      IFNULL(CAST(dolocationid AS STRING), ''), '|',
      IFNULL(CAST(passenger_count AS STRING), ''), '|',
      IFNULL(CAST(trip_distance AS STRING), ''), '|',
      IFNULL(CAST(fare_amount AS STRING), ''), '|',
      IFNULL(CAST(total_amount AS STRING), ''), '|',
      IFNULL(CAST(payment_type AS STRING), '')
    ))) AS event_id
  FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`
  WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = 2025
    AND EXTRACT(MONTH FROM lpep_pickup_datetime) IN (4, 5)
)
SELECT event_id, COUNT(*) AS cnt
FROM hashed
GROUP BY event_id
HAVING COUNT(*) > 1;
```
Expected: **0 rows returned** (mathematically guaranteed by `source_row_number` uniqueness within a file plus `source_file_identity` uniqueness across files, independent of SHA256 collision probability).

**(G) Idempotency test procedure**
1. Record baseline counts: run query (B) and (C), save the `curated_april` / `curated_may` values.
2. Rerun the batch pipeline for April only (staging load → curation), per §7 steps 5 and 7, April leg only.
3. Re-run query (B). Expected: `curated_april` count is **unchanged** from step 1.
4. Re-run query (D) (duplicate check). Expected: still **0 rows**.
5. Repeat steps 2–4 for May.

**(H) Partitioning / clustering verification**
```sql
SELECT table_name, ddl
FROM `jcdeah-009.cp3_aninditas_curated.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'green_tripdata_curated';
```
Expected: DDL output shows `PARTITION BY pickup_date` and `CLUSTER BY data_source, pickup_location_id, dropoff_location_id` (unchanged — this fix does not alter partitioning/clustering).

---

## 9. Idempotency test procedure

See §8(G) above for the exact steps. Summary of what "pass" means: after rerunning the same period twice, (1) row counts are identical between runs, (2) query (D) returns 0 duplicate `event_id` rows, (3) query (E) returns 0 null `event_id` rows.

Automated (local, no cloud) coverage in `tests/test_batch_identity.py` — 11 tests, all passing:
- `TestSourceIdentityAssignment` (3 tests): `source_row_number` is positional/deterministic; `source_file_identity` is constant and consistent; the assignment function doesn't mutate its input.
- `TestBusinessDeduplication` (3 tests): true duplicate business rows collapse to 1; distinct trips sharing the old narrow composite key are **not** dropped; `source_row_number` is confirmed excluded from the dedup subset (i.e. it cannot defeat dedup).
- `TestEventIdFormula` (5 tests, mirrors the SQL hash formula in Python via `hashlib.sha256` for local testability): same source row → same `event_id`; different `source_row_number` → different `event_id`; different `source_file_identity` (April vs May) → different `event_id`; a rerun computing the same inputs independently → identical `event_id`; a `NULL` business field does not collapse the whole hash to `NULL`.

Run locally:
```bash
python -m unittest tests.test_batch_identity -v
```
Verified in this session: **11/11 passed.**

---

## 10. Expected result per query

| Query | Expected result |
|---|---|
| DDL-1 (before rebuild) | no `source_row_number`/`source_file_identity` columns |
| DDL-1 (after §7 step 6) | both columns present, types INT64/STRING |
| DDL-4 (before DDL-5) | `BATCH` rows all have `event_id IS NULL` |
| DDL-6 (after DDL-5) | 0 `BATCH` rows in curated |
| (A) | `source_row_number`=INT64, `source_file_identity`=STRING |
| (B), (C) | `curated_<month> <= staging_<month>` |
| (D) | 0 rows (no duplicate `event_id`) |
| (E) | 0 (no null `event_id`) |
| (F) | 0 rows (no hash collisions) |
| (G) | counts unchanged after rerun; (D)/(E) still 0 |
| (H) | `PARTITION BY pickup_date`, `CLUSTER BY data_source, pickup_location_id, dropoff_location_id` |

---

## 11. Airflow execution order

Current DAGs are **not chained automatically** (pre-existing, unchanged by this fix):
- `cp3_aninditas_batch_pipeline` (`dags/gcs_to_bq_staging_dag.py`) — GCS → staging.
- `cp3_aninditas_curation` (`dags/curation_dag.py`) — staging → curated (runs `transform_to_curated.sql`).
- `cp3_aninditas_mart_dag` (`dags/mart_dag.py`) — curated → mart.

For this rebuild, trigger manually in order, once per period (April, then May):
```
cp3_aninditas_batch_pipeline (year=2025, month=04)
  → cp3_aninditas_curation (year=2025, month=04)
    → cp3_aninditas_mart_dag (year=2025, month=04)   [if mart layer needs refreshing too]

cp3_aninditas_batch_pipeline (year=2025, month=05)
  → cp3_aninditas_curation (year=2025, month=05)
    → cp3_aninditas_mart_dag (year=2025, month=05)
```
DAG-to-DAG auto-chaining (e.g. via `TriggerDagRunOperator` or datasets) was flagged in the earlier batch audit as a P1 hardening item; it is out of scope for this identity fix and unchanged.

---

## 12. Remaining issues

1. **One-time manual rebuild required.** Existing curated BATCH rows all carry `event_id IS NULL` under the old logic. They must be deleted and reprocessed (§6 DDL-5, §7) before the new MERGE key is safe to rely on — otherwise the next curation run will insert duplicate rows alongside the stale NULL-`event_id` ones. This is a one-time migration, not a recurring concern.
2. **`sql/merge_batch.sql` is dead/legacy code**, not invoked by any Python script. It targets yellow-taxi columns (`tpep_pickup_datetime`) and a `{mart_dataset}.ride_fact` table using `GENERATE_UUID()` as a non-deterministic `ride_id` — the same class of correctness risk this fix just addressed for the curated MERGE, but in an unused file. Not modified here per the approved scope (batch identity fix only); recommend addressing or removing in a future mart-layer hardening pass.
3. **`validate_staging_table()` and `validate_curated_table()` were not extended** to assert on `source_row_number`/`source_file_identity`/`event_id` non-null or uniqueness — the manual validation queries in §8 cover this instead, per the "manual execution only" constraint for this stage. Consider folding queries (D)/(E)/(F) into `validate_curated_table()` as an automated quality gate in a future iteration.
4. **Streaming `event_id` alignment is assumed, not verified in this session.** The STREAMING branch of `transform_to_curated.sql` was left untouched and still reads `event_id` directly from `{staging_stream_table}`, consistent with the earlier-approved streaming schema contract (hash of business fields + row index). Since streaming implementation is explicitly out of scope for this stage, this alignment should be re-confirmed once the streaming publisher/pipeline is actually built.
5. **`.env.example` is missing `BQ_DATASET_MART`**, which does exist in the real `.env`. Pre-existing documentation gap, unrelated to this fix; noted for the README pass.
