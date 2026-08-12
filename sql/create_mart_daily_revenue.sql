-- Fact: daily revenue & trip volume by pickup zone.
-- Grain: 1 row per (trip_date, pickup_borough, pickup_zone).
--
-- Source: `{curated_table}` (green_tripdata_curated) -- trip/event data,
-- REQUIRED to come from the curated layer, never staging -- joined to the
-- taxi zone dimension `{dim_locations_table}` (reference/master data, see
-- create_dim_locations.sql for its documented staging-source exception).
--
-- No `data_source` filter is applied, so BATCH and STREAMING trips are both
-- included automatically as soon as they land in curated; the
-- batch_trips_count / streaming_trips_count columns make the split visible
-- for QA/reporting without needing a separate table per source.
--
-- All date/time aggregation uses `pickup_datetime` (business trip time),
-- never `ingestion_time` -- consistent with the curation layer's own
-- period-filtering convention.
--
-- No extra dedup logic is needed here: `green_tripdata_curated` already
-- guarantees one row per `event_id` (enforced by the curation MERGE), so
-- this aggregation can never double-count a trip.
--
-- CREATE OR REPLACE TABLE = full atomic replace every run -> rerun-safe by
-- construction; no duplicate-accumulation risk no matter how many times
-- this DAG task is retried or re-triggered.
CREATE OR REPLACE TABLE `{mart_table}`
PARTITION BY trip_date
CLUSTER BY pickup_borough, pickup_zone
AS
SELECT
  DATE(c.pickup_datetime) AS trip_date,
  COALESCE(loc_pickup.borough, 'Unknown') AS pickup_borough,
  COALESCE(loc_pickup.zone, 'Unknown') AS pickup_zone,
  COUNT(c.event_id) AS total_trips,
  SUM(c.passenger_count) AS total_passengers,
  ROUND(SUM(c.trip_distance), 2) AS total_distance_miles,
  ROUND(SUM(c.fare_amount), 2) AS total_fare_amount,
  ROUND(SUM(c.tip_amount), 2) AS total_tip_amount,
  ROUND(SUM(c.total_amount), 2) AS total_revenue,
  ROUND(SAFE_DIVIDE(SUM(c.tip_amount), SUM(c.fare_amount)) * 100, 2) AS avg_tip_percentage,
  COUNTIF(c.data_source = 'STREAMING') AS streaming_trips_count,
  COUNTIF(c.data_source = 'BATCH') AS batch_trips_count,
  CURRENT_TIMESTAMP() AS updated_at
FROM `{curated_table}` c
LEFT JOIN `{dim_locations_table}` loc_pickup
  ON c.pickup_location_id = loc_pickup.location_id
WHERE c.pickup_datetime IS NOT NULL
GROUP BY 1, 2, 3;
