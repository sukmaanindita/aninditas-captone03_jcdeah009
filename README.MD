# Capstone 03 — NYC Green Taxi Data Pipeline

## 1. Project Overview

Pipeline data NYC Green Taxi yang menggabungkan dua jalur ingestion — **batch** (file Parquet historis dari NYC TLC) dan **streaming** (event trip sintetis real-time via Pub/Sub) — menjadi satu **curated layer** yang konsisten, lalu menyajikannya sebagai **data mart** untuk kebutuhan analytical/reporting (tren revenue, performa rute/zona, pola permintaan per jam).

Project identity: `cp3_aninditas` (student capstone), GCP project `jcdeah-009`.

## 2. Architecture

**Batch:**
```
Source (NYC TLC Parquet, GCS)
  → Airflow: aninditas_batch_pipeline_dag
  → Batch Staging (green_tripdata_staging_batch)
  → Curated (green_tripdata_curated)
  → Data Mart (aninditas_mart_dag)
```

**Streaming:**
```
Publisher (publisher.py, manual)
  → Google Cloud Pub/Sub
  → Apache Beam DirectRunner (streaming_pipeline.py, manual)
  → Streaming Staging (green_tripdata_staging_stream)
  → Curated (green_tripdata_curated)
  → Data Mart (aninditas_mart_dag)
```

Batch dan streaming **bertemu di curated layer**: `sql/transform_to_curated.sql` menggabungkan kedua source lewat `UNION ALL` ke dalam satu MERGE target (`green_tripdata_curated`), dikunci oleh `event_id` yang deterministic di kedua sisi (BATCH: hash SHA256 dari `source_file_identity` + `source_row_number` + field bisnis; STREAMING: UUID5 dari `sequence_key` + field bisnis, dihitung saat publish). Data mart selalu membaca dari curated ini (bukan langsung dari staging), kecuali dimensi referensi zona taxi yang memang sengaja dibaca langsung dari staging (lihat §4).

## 3. Tech Stack

- Python (pandas, pyarrow, jsonschema, python-dotenv, google-cloud-storage, google-cloud-bigquery, google-cloud-pubsub, requests)
- Apache Airflow 2.7.1 (via Docker Compose)
- Apache Beam 2.71.0 `[gcp]` — DirectRunner
- Google Cloud Pub/Sub
- Google Cloud Storage
- BigQuery (SQL)
- Docker / Docker Compose

## 4. Data Layers

### Staging

- **Batch** — `green_tripdata_staging_batch`: dimuat dari file Parquet GCS oleh `dags/scripts/load_staging.py`. `WRITE_APPEND` setelah `DELETE` periode yang sama (idempotent per periode). Partitioned by `DATE(lpep_pickup_datetime)`. Dedup berbasis kolom bisnis (bukan full-row) sebelum load.
- **Streaming** — `green_tripdata_staging_stream`: ditulis oleh Beam DoFn (`STREAMING_INSERTS`) dari event valid yang lolos schema + domain validation. Partitioned by `DATE(ingestion_time)`, clustered by `pickup_location_id, payment_type`.
- **Rejected/dead-letter** — `green_tripdata_staging_stream_rejected`: hanya untuk jalur streaming. Event yang gagal parse JSON atau gagal validasi (schema/domain) ditulis ke sini, bukan didrop diam-diam. Partitioned by `DATE(rejected_at)`. Jalur batch **tidak** punya tabel dead-letter setara — batch memakai gerbang kualitas di level tabel (`validate_staging_table()`, gagal total jika periode kosong / ada nilai invalid), bukan routing per baris.

### Curated

`green_tripdata_curated` — satu tabel MERGE target untuk kedua source, dengan kolom `data_source` (`'BATCH'` / `'STREAMING'`) untuk membedakan asal baris. Deduplikasi: `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_time DESC)`, ambil baris pertama. `event_id` selalu non-NULL di kedua source, sehingga `MERGE ... ON T.event_id = S.event_id` aman tanpa fallback composite key. Partitioned by `pickup_date`, clustered by `data_source, pickup_location_id, dropoff_location_id`.

### Data Mart

Dataset `cp3_aninditas_mart`, 4 tabel, semua `CREATE OR REPLACE TABLE` (full rebuild tiap run). Lihat detail grain di §7.

## 5. Streaming Pipeline

- **Publisher** (`dags/scripts/publisher.py`): membangkitkan event trip sintetis untuk 1 Juni – 31 Juli 2026 (`max_per_day=10`/hari → 610 event total), mem-publish ke Pub/Sub via REST API. `event_id` dihitung deterministic (UUID5 dari `sequence_key` + field bisnis inti) — rerun/redelivery tidak menghasilkan `event_id` baru.
- **Event schema** (`dags/scripts/event_schema.json`): JSON Schema draft-07, snake_case, `required[]` mencakup `event_id, event_time, ingestion_time, vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, passenger_count, trip_distance, fare_amount, total_amount, payment_type`.
- **Pub/Sub**: publisher memakai REST API `topics:publish`; Beam membaca lewat `ReadFromPubSub(subscription=...)`.
- **Beam** (`dags/scripts/streaming_pipeline.py`): `parse → validate (schema + domain rules) → transform (type coercion) → WriteToBigQuery`. Valid → `staging_stream`; invalid → `staging_stream_rejected` (`TaggedOutput`).
- **Validation**: 2 lapis — JSON Schema, lalu domain rules tambahan (`dropoff > pickup`, nilai numerik non-negatif, lokasi wajib ada).
- **Transformation**: coercion tipe numerik (int/float) agar sesuai skema BigQuery; nama field tidak diubah.
- **BigQuery staging**: `WriteToBigQuery` method `STREAMING_INSERTS`, `WRITE_APPEND`, `CREATE_NEVER` (tabel harus sudah dibuat lewat `sql/create_staging_stream*.sql`).
- **EOS (end-of-stream) mechanism**: publisher mengirim **satu** control message (`{"control": "END_OF_STREAM", "total_events": N}`) setelah seluruh trip event terkirim. Beam mengenali sinyal ini *sebelum* `validate_event()` dipanggil, sehingga tidak pernah masuk ke `staging_stream`/`staging_stream_rejected`. `StreamCompletionTracker` (in-process, thread-safe, `threading.Event`) menghitung message trip **distinct** (dedup via hash payload, mengantisipasi redelivery Pub/Sub at-least-once) yang sudah diproses; begitu jumlahnya ≥ `total_events` dari sinyal EOS, pipeline memanggil `result.cancel()` — bukan polling/sleep loop, tapi blocking `wait()` pada `threading.Event`.

> **Penting**: mekanisme auto-termination ini eksplisit **DirectRunner-only** (didokumentasikan langsung di kode) — bergantung pada DirectRunner yang tidak pernah keluar dari proses Python yang sama. Ini **tidak** berlaku untuk DataflowRunner atau runner terdistribusi lainnya.

## 6. Parameterized Curation

`aninditas_curation_dag` punya Airflow `Param year` dan `Param month`. `sql/transform_to_curated.sql` **selalu mengevaluasi kedua branch** (BATCH dan STREAMING) lewat `UNION ALL` pada setiap run — tidak ada percabangan if/else eksplisit yang memilih source berdasarkan periode. Sumber yang efektif berkontribusi ditentukan oleh isi data di masing-masing staging table untuk periode yang diminta:

- Staging batch (`green_tripdata_staging_batch`) saat ini hanya berisi data April–Mei 2026 → periode itu yang menghasilkan baris dari branch BATCH.
- Staging streaming (`green_tripdata_staging_stream`) saat ini hanya berisi data Juni–Juli 2026 (sesuai rentang tanggal di `publisher.py`) → periode itu yang menghasilkan baris dari branch STREAMING.

Filter periode BATCH memakai `lpep_pickup_datetime`; filter periode STREAMING memakai `pickup_datetime` — keduanya kolom waktu bisnis trip, bukan `ingestion_time`.

## 7. Data Mart

| Tabel | Grain | Source | Partition / Cluster |
|---|---|---|---|
| `dim_locations` | 1 baris / `location_id` | `taxi_zone_mapping_staging` (reference/master data — dibaca langsung dari staging, bukan curated, karena bukan trip/event yang perlu event-level curation) | – |
| `mart_daily_revenue` | `trip_date` × `pickup_borough` × `pickup_zone` | `green_tripdata_curated` + `dim_locations` | `PARTITION BY trip_date`, `CLUSTER BY pickup_borough, pickup_zone` |
| `mart_zone_performance` | `pickup_zone` × `pickup_borough` × `dropoff_zone` × `dropoff_borough` | `green_tripdata_curated` + `dim_locations` (join 2×, role-playing dimension) | `CLUSTER BY pickup_borough, dropoff_borough` |
| `mart_hourly_demand` | `day_of_week_num` × `hour_of_day` | `green_tripdata_curated` | `CLUSTER BY day_of_week_num` |

Semua tabel trip/event fact (`mart_daily_revenue`, `mart_zone_performance`, `mart_hourly_demand`) sumbernya **wajib** `green_tripdata_curated` — tidak ada filter `data_source`, sehingga baris BATCH dan STREAMING otomatis tercakup begitu keduanya ada di curated. `mart_daily_revenue` menambahkan kolom `batch_trips_count` / `streaming_trips_count` untuk visibilitas breakdown. Tidak ada logic dedup tambahan di mart — curated sudah menjamin satu baris per `event_id`.

## 8. Data Quality

Yang **benar-benar ada** di kode:

- Schema validation (streaming): JSON Schema draft-07 via `jsonschema`.
- Mandatory fields (streaming): `required[]` di schema + pengecekan eksplisit lokasi pickup/dropoff di `validate_event()`.
- Mandatory fields (batch): cek `NULL` pada `lpep_pickup_datetime` di `validate_staging_table()`.
- Invalid value handling (streaming): `trip_distance/fare_amount/total_amount < 0`, `passenger_count` invalid, `dropoff <= pickup` → ditolak ke rejected table.
- Invalid value handling (batch): `passenger_count < 0` atau `trip_distance < 0` → `validate_staging_table()` raise error (gagal total, bukan per baris).
- Duplicate detection: `event_id` deterministic + `ROW_NUMBER()` dedup di curated MERGE; `build_duplicate_key_check_sql()` di mart layer (dijalankan oleh `task_validate_mart`).
- Null checks: `build_null_key_check_sql()` di mart layer untuk kolom key/grain tiap tabel.
- Row count / freshness: `validate_staging_table()` (batch, gagal jika 0 baris untuk periode), `task_validate_curated()` (gagal jika 0 baris untuk periode), `task_validate_mart()` (gagal jika tabel mart kosong).

Yang **belum ada** (jangan diasumsikan implemented):

- Tidak ada quality-check/task otomatis untuk `green_tripdata_staging_stream` / `_rejected` (count valid vs rejected, breakdown alasan) — harus dicek manual via query (lihat §11).
- Tidak ada dead-letter table untuk batch.
- Tidak ada monitoring freshness/SLA di luar cek row count > 0.

## 9. Testing

**100/100 test offline passing** (diverifikasi ulang saat audit ini, 2026-08-12, `python -m unittest discover -s tests -v`):

| File | Jumlah test | Cakupan |
|---|---|---|
| `tests/test_batch_identity.py` | 11 | `assign_source_identity`, `deduplicate_business_rows`, mirror Python dari formula SHA256 `event_id` batch |
| `tests/test_streaming_pipeline.py` | 36 | `parse_event`, `validate_event`, `transform_event`, `process_message`, `is_end_of_stream_signal`, `StreamCompletionTracker` (termasuk kasus redelivery duplikat & sinyal EOS malformed) |
| `tests/test_curation_transform.py` | 19 | Rendering `.format()` `sql/transform_to_curated.sql` — mapping kolom, filter periode, no-hardcoding |
| `tests/test_data_mart_transform.py` | 34 | Rendering `.format()` keempat file SQL mart — aturan sourcing curated-vs-staging, `pickup_datetime` usage, partition/cluster, rerun-safety |

**Semua test di atas adalah offline** (logic Python murni + assertion string SQL hasil render) — **tidak ada satupun yang mengeksekusi query ke BigQuery**, dan tidak ada panggilan GCP/Pub/Sub apa pun.

**Validasi end-to-end** dilakukan manual oleh project owner via Airflow task logs (`logs/`), bukan lewat automated test suite:
- Batch staging + curation: sukses konsisten, terakhir 2026-08-11 (April 44.044 baris, Mei 44.764 baris).
- Streaming curation (Jun/Jul): sukses live 2026-08-12 13:17 UTC (Juni 300 baris, Juli 310 baris — total 610, cocok dengan jumlah event yang di-generate `publisher.py`).
- Data mart build + validate: sukses live 2026-08-12 13:49 UTC (`dim_locations` 265, `mart_daily_revenue` 7.943, `mart_zone_performance` 9.688, `mart_hourly_demand` 168 baris — seluruh cek NULL-key dan duplicate-key lulus).

Tidak ada automated end-to-end test (mis. test yang benar-benar menjalankan DAG atau query BigQuery); bukti "berhasil" di atas berasal dari log historis, bukan pipeline CI yang bisa diulang otomatis.

## 10. How to Run

Environment: `.venv` (Python, untuk batch/curation/mart/Airflow) dan `.venv-beam` (Python 3.12, khusus untuk menjalankan Beam) — **dua virtual environment terpisah**, karena `apache-beam[gcp]` punya dependency yang berbeda dari sisa project.

```bash
pip install -r requirements.txt          # .venv utama (batch, curation, mart, test)
python3.12 -m venv .venv-beam
source .venv-beam/bin/activate
pip install -r requirements-beam.txt     # khusus untuk streaming_pipeline.py runner
```

### Batch

```bash
# Salin .env.example -> .env dan isi GCP_PROJECT_ID, GCS_BUCKET_NAME, dst.
python dags/scripts/load_staging.py --year 2026 --month 4 --validate
```
Atau via Airflow:
```bash
docker compose up -d
# buka http://localhost:8080, trigger manual berurutan:
# aninditas_batch_pipeline_dag (params: year, month)
#   -> aninditas_curation_dag (params: year, month)
#     -> aninditas_mart_dag (tanpa params, full rebuild)
```
DAG-DAG ini **tidak** ter-chain otomatis — setiap DAG harus di-trigger manual satu per satu, dalam urutan di atas.

### Streaming

Butuh **dua terminal terpisah**, urutan disarankan: jalankan Beam pipeline lebih dulu supaya siap menerima pesan sebelum publisher mulai kirim (dan supaya sinyal EOS bisa langsung ditangkap):

```bash
# Terminal 1
source .venv-beam/bin/activate
python dags/scripts/streaming_pipeline.py --runner DirectRunner

# Terminal 2 (venv utama)
python dags/scripts/publisher.py
```
Pipeline berhenti otomatis setelah menerima sinyal `END_OF_STREAM` dan seluruh event sebelumnya selesai diproses (lihat §5). Jika publisher berhenti sebelum sempat mengirim sinyal EOS, hentikan pipeline manual dengan `Ctrl+C`.

Setelah streaming staging terisi, jalankan `aninditas_curation_dag` dengan `year=2026, month=6` lalu `year=2026, month=7`, kemudian `aninditas_mart_dag`.

## 11. Validation / Monitoring

Contoh query manual terhadap BigQuery (ganti sesuai kebutuhan):

```sql
-- Row count per staging table
SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_batch`;
SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_stream`;

-- Row count curated per periode & source
SELECT data_source, EXTRACT(YEAR FROM pickup_date) AS yr, EXTRACT(MONTH FROM pickup_date) AS mo, COUNT(*)
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
GROUP BY 1, 2, 3
ORDER BY 2, 3, 1;

-- Row count mart
SELECT COUNT(*) FROM `jcdeah-009.cp3_aninditas_mart.mart_daily_revenue`;

-- Duplicate event_id check (curated)
SELECT event_id, COUNT(*) AS cnt
FROM `jcdeah-009.cp3_aninditas_curated.green_tripdata_curated`
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Rejected streaming records: jumlah & alasan
SELECT reason, COUNT(*) AS cnt
FROM `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_stream_rejected`
GROUP BY reason
ORDER BY cnt DESC;
```

## 12. Known Limitations

- **EOS/auto-termination hanya berlaku untuk DirectRunner** — tidak akan berfungsi jika pipeline dijalankan dengan DataflowRunner atau runner terdistribusi lain (lihat §5).
- **Streaming pipeline harus dijalankan manual** (dua proses terpisah, dua virtual environment) — tidak diorkestrasi Airflow sama sekali. Tidak ada log persisten untuk run publisher/Beam di repo ini.
- **3 DAG Airflow (batch, curation, mart) tidak ter-chain otomatis** — user harus trigger manual berurutan; tidak ada `TriggerDagRunOperator` atau dataset-based scheduling.
- **Tidak ada quality-check otomatis untuk streaming staging** (valid vs rejected count) — harus dicek manual (§11).
- **`delete_existing_month_data()` di `load_staging.py` menangkap exception secara luas** — jika DELETE gagal karena sebab selain "tabel belum ada" (permission, quota, dll), proses tetap lanjut ke `WRITE_APPEND`, berisiko menumpuk data lama+baru untuk periode yang sama tanpa error yang terlihat jelas.
- **Ditemukan selisih kecil (3 baris) pada row count curated batch April** antar dua run live berbeda hari (44.041 vs 44.044) padahal staging source-nya identik (44.235 baris di kedua run) — belum bisa dijelaskan pasti dari kode, direkomendasikan verifikasi manual (duplicate `event_id` check).
- **Kemungkinan tabel curated lama/orphan** (`green_tripdata_curated_batch`) masih tersisa di BigQuery dari sebelum `.env` diperbaiki — tidak dipakai kode manapun sekarang, sebaiknya dibersihkan manual jika sudah dikonfirmasi tidak diperlukan.
- **`sql/merge_batch.sql` dan `analytics/batch_vs_stream.sql`** adalah dead code (tidak dipanggil dari Python manapun) — dibiarkan sebagaimana adanya, belum dibersihkan.
- **Tidak ada automated end-to-end test** — validasi live sejauh ini berbasis log manual, bukan test suite yang bisa dijalankan ulang otomatis.

## 13. Project Structure

```
Capstone03/
├── dags/
│   ├── gcs_to_bq_staging_dag.py      # aninditas_batch_pipeline_dag
│   ├── curation_dag.py               # aninditas_curation_dag
│   ├── mart_dag.py                   # aninditas_mart_dag
│   └── scripts/
│       ├── download_to_gcs.py
│       ├── load_lookup_zone.py
│       ├── load_staging.py
│       ├── transform.py
│       ├── data_mart.py
│       ├── publisher.py
│       ├── streaming_pipeline.py
│       └── event_schema.json
├── sql/
│   ├── create_staging_stream.sql
│   ├── create_staging_stream_rejected.sql
│   ├── create_curated_tables.sql
│   ├── transform_to_curated.sql
│   ├── validate_batch.sql
│   ├── create_dim_locations.sql
│   ├── create_mart_daily_revenue.sql
│   ├── create_mart_zone_performance.sql
│   ├── create_mart_hourly_demand.sql
│   └── merge_batch.sql               # dead code, tidak dipakai
├── analytics/
│   ├── batch_analysis.sql            # query analisis ad-hoc
│   └── batch_vs_stream.sql           # kosong
├── tests/
│   ├── test_batch_identity.py
│   ├── test_streaming_pipeline.py
│   ├── test_curation_transform.py
│   └── test_data_mart_transform.py
├── docs/
│   └── batch_identity_fix_report.md
├── docker-compose.yaml
├── requirements.txt
├── requirements-beam.txt
├── .env.example
└── README.MD
```

## 14. Final Requirement Checklist

| Requirement | Implementation | Status |
|---|---|---|
| Ingestion (GCS raw) | `download_to_gcs.py`, idempotent | PASS |
| Streaming publisher | `publisher.py`, event_id deterministic, EOS terkirim | PARTIAL — tidak ada log live untuk verifikasi langsung |
| Pub/Sub | REST publish + `ReadFromPubSub` | PARTIAL — wiring benar, volume tidak terverifikasi langsung |
| Beam/transform | `streaming_pipeline.py`, pure functions teruji 36 test | PARTIAL — wiring DoFn/pipeline tidak diuji otomatis |
| Schema validation | JSON Schema draft-07 | PASS |
| Mandatory fields | `required[]` + domain checks | PASS |
| Invalid value handling | reject negative/invalid values | PASS |
| Duplicate handling | deterministic `event_id` + `ROW_NUMBER` dedup | PARTIAL — desain solid, selisih 3 baris live belum terjelaskan |
| Rejected/dead-letter handling | `staging_stream_rejected` table | PARTIAL — routing benar, count live belum terverifikasi |
| Metadata event_id/event_time/ingestion_time | ada di schema, staging, curated | PASS |
| Staging (batch + streaming) | dua tabel terpisah, terisi | PASS |
| Curation | MERGE UNION ALL, live sukses | PASS |
| Batch + streaming integrasi di curated | `data_source` column, UNION ALL | PASS |
| Parameter year/month | Airflow `Param` di curation_dag | PASS |
| Data mart | 4 tabel, live-confirmed | PASS |
| Data quality checks | staging/curated/mart ada; streaming staging tidak ada | PARTIAL |
| Partitioning | curated, staging_stream, mart_daily_revenue | PASS |
| Clustering | curated + 3 tabel mart | PASS |
| Orchestration | 3 DAG batch/curation/mart, tidak auto-chained; streaming di luar Airflow | PARTIAL |
| Testing | 100/100 offline pass; tidak ada automated E2E test | PARTIAL |
