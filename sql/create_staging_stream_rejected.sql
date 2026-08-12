-- Dead-letter table for the streaming pipeline (dags/scripts/streaming_pipeline.py).
--
-- Setiap Pub/Sub message yang gagal di tahap parse (JSON tidak valid) atau
-- validate (schema/domain rule, lihat validate_event() di streaming_pipeline.py)
-- ditulis ke sini alih-alih di-drop diam-diam, supaya event invalid tetap bisa
-- diaudit (bukan silently disappear -- lihat requirement Section 11 project
-- instructions).
--
-- raw_payload disimpan sebagai STRING (bukan JSON) karena payload yang gagal
-- parse belum tentu berupa JSON valid sama sekali.
--
-- Sama seperti create_staging_stream.sql: `CREATE TABLE IF NOT EXISTS` tidak
-- akan mengubah schema tabel live yang sudah ada. Jika tabel ini pernah
-- dibuat dengan schema lain (mis. sebelum desain dead-letter ini ada), sync
-- ulang secara manual:
--   bq rm -f -t jcdeah-009:cp3_aninditas_staging.green_tripdata_staging_stream_rejected
--   bq query --use_legacy_sql=false < sql/create_staging_stream_rejected.sql
CREATE TABLE IF NOT EXISTS `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_stream_rejected` (
  rejected_at TIMESTAMP,
  reason STRING,
  raw_payload STRING
)
PARTITION BY DATE(rejected_at);
