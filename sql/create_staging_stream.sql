-- Streaming staging table for the Beam pipeline (dags/scripts/streaming_pipeline.py).
--
-- Column names are snake_case and match EXACTLY:
--   - the event payload published by dags/scripts/publisher.py
--   - dags/scripts/event_schema.json (JSON Schema used by validate_event())
--   - the dict produced by transform_event() / written by WriteToBigQuery()
-- Do NOT reintroduce old batch-style names here (VendorID, PULocationID,
-- DOLocationID, lpep_pickup_datetime, lpep_dropoff_datetime, RatecodeID) --
-- those belong ONLY to the raw NYC TLC / batch staging table
-- (BQ_STAGING_TABLE_BATCH, see dags/scripts/load_staging.py), a separate
-- table from this one.
--
-- IMPORTANT -- syncing an already-existing live table:
-- `CREATE TABLE IF NOT EXISTS` is a no-op if the table already exists, even
-- if its live schema is stale/different from what's below. If this table was
-- created earlier under the old batch-style column names, running this file
-- again will NOT fix it -- BigQuery will keep inserting against the old
-- schema and Beam writes will keep failing with "no such field: ...".
-- To resync a stale live table with this DDL (safe: this is a re-buildable
-- staging table, not the batch table, and not curated/analytical data):
--   1. Confirm you're targeting the STREAM table, not the batch table:
--        bq show jcdeah-009:cp3_aninditas_staging.green_tripdata_staging_stream
--   2. Drop only the stale streaming table (does NOT touch
--      green_tripdata_staging_batch or any curated table):
--        bq rm -f -t jcdeah-009:cp3_aninditas_staging.green_tripdata_staging_stream
--   3. Recreate it with the current schema below:
--        bq query --use_legacy_sql=false < sql/create_staging_stream.sql
-- Run the same rebuild for sql/create_staging_stream_rejected.sql if that
-- table also predates the current dead-letter design.
CREATE TABLE IF NOT EXISTS `jcdeah-009.cp3_aninditas_staging.green_tripdata_staging_stream` (
  event_id STRING,
  event_time STRING,
  ingestion_time TIMESTAMP,
  vendor_id STRING,
  pickup_datetime STRING,
  dropoff_datetime STRING,
  store_and_fwd_flag STRING,
  ratecode_id INT64,
  pickup_location_id INT64,
  dropoff_location_id INT64,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type INT64,
  trip_type INT64,
  congestion_surcharge FLOAT64
)
PARTITION BY DATE(ingestion_time)
CLUSTER BY pickup_location_id, payment_type;