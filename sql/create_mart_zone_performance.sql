-- Fact: route (pickup -> dropoff zone pair) performance.
-- Grain: 1 row per (pickup_zone, pickup_borough, dropoff_zone, dropoff_borough).
--
-- Source: `{curated_table}` (green_tripdata_curated) -- trip/event data,
-- REQUIRED to come from the curated layer -- joined TWICE to the taxi zone
-- dimension `{dim_locations_table}` (role-playing dimension: pickup side and
-- dropoff side; reference/master data, see create_dim_locations.sql).
--
-- No `data_source` filter -> both BATCH and STREAMING trips are included
-- automatically.
--
-- No extra dedup logic needed: `green_tripdata_curated` already guarantees
-- one row per `event_id` (curation MERGE), so this aggregation can never
-- double-count a trip.
--
-- CREATE OR REPLACE TABLE = full atomic replace every run -> rerun-safe by
-- construction.
CREATE OR REPLACE TABLE `{mart_table}`
CLUSTER BY pickup_borough, dropoff_borough
AS
SELECT
  COALESCE(loc_pickup.zone, 'Unknown') AS pickup_zone,
  COALESCE(loc_pickup.borough, 'Unknown') AS pickup_borough,
  COALESCE(loc_dropoff.zone, 'Unknown') AS dropoff_zone,
  COALESCE(loc_dropoff.borough, 'Unknown') AS dropoff_borough,
  COUNT(c.event_id) AS total_completed_trips,
  ROUND(AVG(c.trip_distance), 2) AS avg_trip_distance_miles,
  ROUND(AVG(TIMESTAMP_DIFF(c.dropoff_datetime, c.pickup_datetime, MINUTE)), 2) AS avg_trip_duration_minutes,
  ROUND(AVG(c.total_amount), 2) AS avg_total_amount,
  CURRENT_TIMESTAMP() AS updated_at
FROM `{curated_table}` c
LEFT JOIN `{dim_locations_table}` loc_pickup
  ON c.pickup_location_id = loc_pickup.location_id
LEFT JOIN `{dim_locations_table}` loc_dropoff
  ON c.dropoff_location_id = loc_dropoff.location_id
WHERE c.pickup_datetime IS NOT NULL AND c.dropoff_datetime IS NOT NULL
GROUP BY 1, 2, 3, 4;
