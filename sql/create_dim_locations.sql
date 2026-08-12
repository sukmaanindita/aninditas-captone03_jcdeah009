-- Dimension: taxi zone lookup (location_id -> borough / zone / service_zone).
--
-- SOURCE EXCEPTION (deliberate, approved -- do not "fix" to read from a
-- curated table): this is REFERENCE/MASTER data, not trip/event data. It is
-- sourced directly from the staging zone lookup table
-- ({staging_zone_table} = BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP), the same
-- pattern already used for downstream layers in Capstone 2. Taxi zone
-- mapping never carries a business event -- there is no event_id dedup,
-- batch/stream reconciliation, or trip-level business rule for the curation
-- layer to apply to it, so routing it through `green_tripdata_curated`-style
-- curation would add a layer with no transformation to perform.
--
-- This exception applies ONLY to this reference dimension. All trip/event
-- fact data for the Data Mart (see create_mart_*.sql) is REQUIRED to come
-- from the curated layer (`green_tripdata_curated`), never from staging.
--
-- Dedup/NULL-key defensive guards (QUALIFY + WHERE) are added here even
-- though the source is a clean, official TLC lookup, so this table always
-- satisfies "no NULL key / no duplicate key" regardless of upstream staging
-- content.
--
-- CREATE OR REPLACE TABLE = full atomic replace every run, so reruns can
-- never accumulate duplicate rows no matter how many times this DAG task is
-- retried or re-triggered.
CREATE OR REPLACE TABLE `{mart_table}` AS
SELECT
  location_id,
  borough,
  zone,
  service_zone
FROM (
  SELECT
    CAST(location_id AS INT64) AS location_id,
    COALESCE(borough, 'Unknown') AS borough,
    COALESCE(zone, 'Unknown') AS zone,
    COALESCE(service_zone, 'Unknown') AS service_zone,
    ROW_NUMBER() OVER (PARTITION BY CAST(location_id AS INT64)) AS row_num
  FROM `{staging_zone_table}`
  WHERE location_id IS NOT NULL
)
WHERE row_num = 1;
